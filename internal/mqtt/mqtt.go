// Copyright 2020-2026 The NATS Authors
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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats-server/v2/server/stree"
	"github.com/nats-io/nuid"
)

// References to "spec" here is from https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf

const (
	mqttPacketConnect    = byte(0x10)
	mqttPacketConnectAck = byte(0x20)
	mqttPacketPub        = byte(0x30)
	mqttPacketPubAck     = byte(0x40)
	mqttPacketPubRec     = byte(0x50)
	mqttPacketPubRel     = byte(0x60)
	mqttPacketPubComp    = byte(0x70)
	mqttPacketSub        = byte(0x80)
	mqttPacketSubAck     = byte(0x90)
	mqttPacketUnsub      = byte(0xa0)
	mqttPacketUnsubAck   = byte(0xb0)
	mqttPacketPing       = byte(0xc0)
	mqttPacketPingResp   = byte(0xd0)
	mqttPacketDisconnect = byte(0xe0)
	mqttPacketAuth       = byte(0xf0) // MQTT 5.0 only
	mqttPacketMask       = byte(0xf0)
	mqttPacketFlagMask   = byte(0x0f)

	// Protocol levels. 0x4 is MQTT 3.1.1, 0x5 is MQTT 5.0.
	mqttProtoLevel  = byte(0x4)
	mqttProtoLevel5 = byte(0x5)

	// mqttSessionNeverExpire is the MQTT 5.0 Session Expiry Interval value that
	// means the session never expires. It is also the effective value we map a
	// persistent (non-clean) 3.1.1 session to. Spec5 [3.1.2.11.2].
	mqttSessionNeverExpire = uint32(0xFFFFFFFF)

	// Connect flags
	mqttConnFlagReserved     = byte(0x1)
	mqttConnFlagCleanSession = byte(0x2)
	mqttConnFlagWillFlag     = byte(0x04)
	mqttConnFlagWillQoS      = byte(0x18)
	mqttConnFlagWillRetain   = byte(0x20)
	mqttConnFlagPasswordFlag = byte(0x40)
	mqttConnFlagUsernameFlag = byte(0x80)

	// Publish flags
	mqttPubFlagRetain = byte(0x01)
	mqttPubFlagQoS    = byte(0x06)
	mqttPubFlagDup    = byte(0x08)
	mqttPubQos1       = byte(0x1 << 1)
	mqttPubQoS2       = byte(0x2 << 1)

	// Subscribe flags
	mqttSubscribeFlags = byte(0x2)
	mqttSubAckFailure  = byte(0x80)

	// Unsubscribe flags
	mqttUnsubscribeFlags = byte(0x2)

	// ConnAck returned codes (MQTT 3.1.1)
	mqttConnAckRCConnectionAccepted          = byte(0x0)
	mqttConnAckRCUnacceptableProtocolVersion = byte(0x1)
	mqttConnAckRCIdentifierRejected          = byte(0x2)
	mqttConnAckRCServerUnavailable           = byte(0x3)
	mqttConnAckRCBadUserOrPassword           = byte(0x4)
	mqttConnAckRCNotAuthorized               = byte(0x5)
	mqttConnAckRCQoS2WillRejected            = byte(0x10)

	// MQTT 5.0 reason codes. The success range (< 0x80) is context dependent
	// (Success, Normal disconnection, Granted QoS 0, ...). Codes >= 0x80 are
	// failures. Only the subset used by this implementation is defined here.
	// Spec [MQTT-2.4].
	mqttReasonSuccess                     = byte(0x00) // also Normal disconnection / Granted QoS 0
	mqttReasonGrantedQoS1                 = byte(0x01)
	mqttReasonGrantedQoS2                 = byte(0x02)
	mqttReasonDisconnectWithWill          = byte(0x04)
	mqttReasonNoMatchingSubscribers       = byte(0x10)
	mqttReasonNoSubscriptionExisted       = byte(0x11)
	mqttReasonUnspecifiedError            = byte(0x80)
	mqttReasonMalformedPacket             = byte(0x81)
	mqttReasonProtocolError               = byte(0x82)
	mqttReasonImplementationSpecificError = byte(0x83)
	mqttReasonUnsupportedProtocolVersion  = byte(0x84)
	mqttReasonClientIDNotValid            = byte(0x85)
	mqttReasonBadUserOrPassword           = byte(0x86)
	mqttReasonNotAuthorized               = byte(0x87)
	mqttReasonServerUnavailable           = byte(0x88)
	mqttReasonServerBusy                  = byte(0x89)
	mqttReasonServerShuttingDown          = byte(0x8b)
	mqttReasonBadAuthMethod               = byte(0x8c)
	mqttReasonSessionTakenOver            = byte(0x8e)
	mqttReasonTopicNameInvalid            = byte(0x90)
	mqttReasonTopicFilterInvalid          = byte(0x8f)
	mqttReasonPacketIDInUse               = byte(0x91)
	mqttReasonPacketIDNotFound            = byte(0x92)
	mqttReasonReceiveMaxExceeded          = byte(0x93)
	mqttReasonTopicAliasInvalid           = byte(0x94)
	mqttReasonPacketTooLarge              = byte(0x95)
	mqttReasonQuotaExceeded               = byte(0x97)
	mqttReasonPayloadFormatInvalid        = byte(0x99)
	mqttReasonQoSNotSupported             = byte(0x9b)
	mqttReasonSharedSubNotSupported       = byte(0x9e)
	mqttReasonSubIDNotSupported           = byte(0xa1)
	mqttReasonWildcardSubNotSupported     = byte(0xa2)

	// MQTT 5.0 property identifiers. Spec [MQTT-2.2.2.2].
	mqttPropPayloadFormat        = byte(0x01) // byte
	mqttPropMessageExpiry        = byte(0x02) // 4-byte int
	mqttPropContentType          = byte(0x03) // UTF-8 string
	mqttPropResponseTopic        = byte(0x08) // UTF-8 string
	mqttPropCorrelationData      = byte(0x09) // binary
	mqttPropSubscriptionID       = byte(0x0b) // variable int (repeatable)
	mqttPropSessionExpiry        = byte(0x11) // 4-byte int
	mqttPropAssignedClientID     = byte(0x12) // UTF-8 string
	mqttPropServerKeepAlive      = byte(0x13) // 2-byte int
	mqttPropAuthMethod           = byte(0x15) // UTF-8 string
	mqttPropAuthData             = byte(0x16) // binary
	mqttPropRequestProblemInfo   = byte(0x17) // byte
	mqttPropWillDelay            = byte(0x18) // 4-byte int
	mqttPropRequestResponseInfo  = byte(0x19) // byte
	mqttPropResponseInfo         = byte(0x1a) // UTF-8 string
	mqttPropServerReference      = byte(0x1c) // UTF-8 string
	mqttPropReasonString         = byte(0x1f) // UTF-8 string
	mqttPropReceiveMaximum       = byte(0x21) // 2-byte int
	mqttPropTopicAliasMax        = byte(0x22) // 2-byte int
	mqttPropTopicAlias           = byte(0x23) // 2-byte int
	mqttPropMaxQoS               = byte(0x24) // byte
	mqttPropRetainAvailable      = byte(0x25) // byte
	mqttPropUserProperty         = byte(0x26) // UTF-8 string pair (repeatable)
	mqttPropMaxPacketSize        = byte(0x27) // 4-byte int
	mqttPropWildcardSubAvailable = byte(0x28) // byte
	mqttPropSubIDAvailable       = byte(0x29) // byte
	mqttPropSharedSubAvailable   = byte(0x2a) // byte

	// Maximum payload size of a control packet
	mqttMaxPayloadSize = 0xFFFFFFF

	// Largest legal full control packet: the Remaining Length ceiling plus the
	// fixed header (type/flags byte + up-to-4-byte Remaining Length varint).
	// Maximum Packet Size counts the full packet, not just Remaining Length.
	// Spec5 [2.1.4], [3.1.2.11.4].
	mqttMaxControlPacketSize = mqttMaxPayloadSize + 5

	// Largest value representable as an MQTT variable byte integer (4 bytes),
	// also the maximum valid Subscription Identifier. Spec5 [1.5.5], [3.8.2.1.2].
	mqttMaxVarInt = 0xFFFFFFF

	// Topic/Filter characters
	mqttTopicLevelSep = '/'
	mqttSingleLevelWC = '+'
	mqttMultiLevelWC  = '#'
	mqttReservedPre   = '$'

	// This is appended to the sid of a subscription that is
	// created on the upper level subject because of the MQTT
	// wildcard '#' semantic.
	mqttMultiLevelSidSuffix = " fwc"

	// This is the prefix used for all subjects used by MQTT code.
	mqttPrefix = "$MQTT."

	// This is the prefix for NATS subscriptions subjects associated as delivery
	// subject of JS consumer. We want to make them unique so will prevent users
	// MQTT subscriptions to start with this.
	mqttSubPrefix = mqttPrefix + "sub."

	// Stream name for MQTT messages on a given account
	mqttStreamName          = "$MQTT_msgs"
	mqttStreamSubjectPrefix = mqttPrefix + "msgs."

	// Stream name for MQTT retained messages on a given account
	mqttRetainedMsgsStreamName    = "$MQTT_rmsgs"
	mqttRetainedMsgsStreamSubject = mqttPrefix + "rmsgs."

	// Stream name for MQTT sessions on a given account
	mqttSessStreamName          = "$MQTT_sess"
	mqttSessStreamSubjectPrefix = mqttPrefix + "sess."

	// Stream name and subject prefix for the delayed-Will durable outbox
	// (Layer 2). One subject per Will event: <prefix><domainTk><cidHash>.<willID>,
	// MaxMsgsPer:1, so each generation is an independent CAS target and an
	// in-flight publication survives a fresh reconnect+disconnect. See
	// will_outbox.go.
	mqttWillQStreamName    = "$MQTT_willq"
	mqttWillQSubjectPrefix = mqttPrefix + "willq."

	// Stream name and subject prefix for the durable per-client delayed-Will
	// cancellation watermark (one subject per client, MaxMsgsPer:1, permanent
	// retention). A CONNECT-accept advances the client's watermark to its session
	// save sequence; the takeover fence suppresses any older-generation Will whose
	// origin sequence is below it, even after the session record is gone. Kept on a
	// dedicated stream so the willq claim/cancel scans are not slowed by permanent
	// per-client markers. See will_outbox.go.
	mqttWillCancelStreamName    = "$MQTT_willcancel"
	mqttWillCancelSubjectPrefix = mqttPrefix + "willcancel."

	// Reserved $MQTT_sess metadata key holding the generated stream-incarnation
	// epoch, and the only valid epoch form: "nuid:<22-character NUID>". The epoch
	// is an identity token (equality only), so a session sequence is comparable to
	// a watermark or Will origin sequence only when their epochs match. See
	// validateSessStream.
	mqttSessEpochMetaKey    = "mqtt_sess_epoch"
	mqttSessEpochNUIDPrefix = "nuid:"

	// Stream name and subject for incoming MQTT QoS2 messages
	mqttQoS2IncomingMsgsStreamName          = "$MQTT_qos2in"
	mqttQoS2IncomingMsgsStreamSubjectPrefix = mqttPrefix + "qos2.in."

	// Stream name and subjects for outgoing MQTT QoS (PUBREL) messages
	mqttOutStreamName               = "$MQTT_out"
	mqttOutSubjectPrefix            = mqttPrefix + "out."
	mqttPubRelSubjectPrefix         = mqttPrefix + "out.pubrel."
	mqttPubRelDeliverySubjectPrefix = mqttPrefix + "deliver.pubrel."
	mqttPubRelConsumerDurablePrefix = "$MQTT_PUBREL_"

	// Stream name and subject prefix for shared-subscription QoS 2 assignment
	// claims: one durable record per (group durable, source sequence) naming the
	// session generation that owns a STARTED delivery. Permanent retention,
	// MaxMsgsPer:1, read by direct subject lookup and never consumed. This is the
	// arbitration record that keeps a started shared QoS 2 delivery with its
	// chosen client for as long as it exists. See qos2_shared_affine.go.
	mqttQoS2AssignStreamName          = "$MQTT_qos2assign"
	mqttQoS2AssignStreamSubjectPrefix = mqttPrefix + "qos2.assign."

	// Stream name and subject prefix for OUTBOUND in-flight registers: one tiny
	// record per started QoS 1/2 PUBLISH, naming the packet identifier it was sent
	// under together with the source durable and stream sequence it came from
	// (MaxMsgsPer:1, keyed by the identifier). This is what makes an outbound
	// identifier survive a broker restart or a failover instead of only a
	// same-process reconnect. Kept on its own stream: $MQTT_out is interest-retained
	// and would keep nothing, and a per-message write to the session record would
	// serialize every delivery on the record CAS. See inflight_register.go.
	mqttInflightStreamName          = "$MQTT_inflight"
	mqttInflightStreamSubjectPrefix = mqttPrefix + "inflight."

	// Generation-scoped names for a session's affine QoS 2 delivery path on
	// $MQTT_out: the per-claim copy subject prefix, the internal deliver subject
	// prefix, and the consumer durable prefix. All three embed the session's
	// durable generation so a leaked consumer from an earlier generation of the
	// same client ID can neither read a new generation's copies nor inject into
	// its delivery subscription. See qos2_shared_affine.go.
	mqttQ2AffineSubjectPrefix         = mqttPrefix + "out.q2."
	mqttQ2AffineDeliverPrefix         = mqttPrefix + "deliver.q2."
	mqttQ2AffineConsumerDurablePrefix = "$MQTT_Q2AFF_"

	// Internal names for MQTT 5.0 Shared Subscriptions ("$share/{ShareName}/
	// {filter}"). All three are derived deterministically from a hash of the
	// ShareName and the underlying NATS subject (see mqttSharedNames), so every
	// group member across the cluster converges on a single JetStream consumer.
	// The deliver subject lives under the $MQTT. prefix so it is treated as an
	// internal subject users cannot subscribe to. Spec5 [4.8.2].
	mqttSharedSubDeliverPrefix      = mqttPrefix + "qsub." // "$MQTT.qsub."
	mqttSharedConsumerDurablePrefix = "$MQTT_SHARED_"
	// mqttSharedQueuePrefix marks the generated NATS queue-group name as
	// server-internal so an MQTT client cannot allow/deny it and cannot join a
	// plain NATS queue group by naming it. Used as the DeliverGroup and as the
	// queue name on both the raw QoS0 sub and the shared deliver subscription.
	mqttSharedQueuePrefix = "$mqttshare."

	// As per spec, MQTT server may not redeliver QoS 1 and 2 messages to
	// clients, except after client reconnects. However, NATS Server will
	// redeliver unacknowledged messages after this default interval. This can
	// be changed with the server.Options.MQTT.AckWait option.
	mqttDefaultAckWait = 30 * time.Second

	// This is the default for the outstanding number of pending QoS 1
	// messages sent to a session with QoS 1 subscriptions.
	mqttDefaultMaxAckPending = 1024

	// How long a closing connection's teardown waits out contention on the
	// session gate before concluding it will not get it. Mirrors the CONNECT
	// side's own spin, so neither starves the other. See
	// lockSessionForTeardown.
	mqttTeardownLockAttempts = 10
	mqttTeardownLockItvl     = 100 * time.Millisecond

	// Default MQTT 5.0 Receive Maximum advertised to v5 clients when the
	// receive_maximum option is unset: the in-flight QoS 1/2 window a compliant
	// client self-limits to, of which the server enforces the observable part
	// (QoS 2 held awaiting PUBREL). Symmetric with the outbound
	// mqttDefaultMaxAckPending. Spec5 [3.2.2.3.3].
	mqttDefaultReceiveMax = 1024

	// Default MQTT 5.0 Topic Alias Maximum advertised to (and accepted from)
	// v5 clients when the topic_alias_maximum option is unset. Bounds the
	// per-connection alias map. For reference, mosquitto defaults to 10.
	// Spec5 [3.2.2.3.2].
	mqttDefaultTopicAliasMax = 64

	// A session's list of subscriptions cannot have a cumulative MaxAckPending
	// of more than this limit.
	mqttMaxAckTotalLimit = 0xFFFF

	// Prefix of the reply subject for JS API requests.
	mqttJSARepliesPrefix = mqttPrefix + "JSA."

	// Those are tokens that are used for the reply subject of JS API requests.
	// For instance "$MQTT.JSA.<node id>.SC.<number>" is the reply subject
	// for a request to create a stream (where <node id> is the server name hash),
	// while "$MQTT.JSA.<node id>.SL.<number>" is for a stream lookup, etc...
	mqttJSAIdTokenPos     = 3
	mqttJSATokenPos       = 4
	mqttJSAClientIDPos    = 5
	mqttJSAStreamCreate   = "SC"
	mqttJSAStreamUpdate   = "SU"
	mqttJSAStreamLookup   = "SL"
	mqttJSAStreamPurge    = "SPU"
	mqttJSAConsumerCreate = "CC"
	mqttJSAConsumerLookup = "CL"
	mqttJSAConsumerDel    = "CD"
	mqttJSAMsgStore       = "MS"
	mqttJSAMsgLoad        = "ML"
	mqttJSAMsgDelete      = "MD"
	mqttJSASessPersist    = "SP"
	mqttJSAConsumerList   = "CLS"
	// mqttJSAAckConfirm is the JetStream "double ack": the acknowledgement of a
	// delivery, sent WITH a reply subject so JetStream answers once it has been
	// applied. The ordinary sendAck path is fire-and-forget, which is fine when a
	// lost ack only costs a redelivery — but not when a durable record is deleted
	// on the strength of it. See qos2_shared_affine.go.
	mqttJSAAckConfirm = "AC"

	// This is how long to keep a client in the flappers map before closing the
	// connection. This prevent quick reconnect from those clients that keep
	// wanting to connect with a client ID already in use.
	mqttSessFlappingJailDur = time.Second

	// This is how frequently the timer to cleanup the sessions flappers map is firing.
	mqttSessFlappingCleanupInterval = 5 * time.Second

	// For Websocket URLs
	mqttWSPath = "/mqtt"

	// Default bound (in seconds) on the server-side TLS handshake for an MQTT
	// connection when MQTTOpts.TLSTimeout is unset. Matches nats-server's default
	// so a client that opens a socket and then stalls cannot pin a connection
	// slot indefinitely.
	mqttDefaultTLSTimeout = 2.0

	mqttInitialPubHeader        = 16 // An overkill, should need 7 bytes max
	mqttProcessSubTooLong       = 100 * time.Millisecond
	mqttDefaultRetainedCacheTTL = 2 * time.Minute
	mqttDefaultJSAPITimeout     = 5 * time.Second
)

const (
	sparkbNBIRTH = "NBIRTH"
	sparkbDBIRTH = "DBIRTH"
	sparkbNDEATH = "NDEATH"
	sparkbDDEATH = "DDEATH"
)

var (
	sparkbNamespaceTopicPrefix    = []byte("spBv1.0/")
	sparkbCertificatesTopicPrefix = []byte("$sparkplug/certificates/")
)

var (
	mqttPingResponse     = []byte{mqttPacketPingResp, 0x0}
	mqttProtoName        = []byte("MQTT")
	mqttOldProtoName     = []byte("MQIsdp")
	mqttSessJailDur      = mqttSessFlappingJailDur
	mqttFlapCleanItvl    = mqttSessFlappingCleanupInterval
	mqttRetainedCacheTTL = mqttDefaultRetainedCacheTTL
	// How old a consumer must be before the orphan sweep may delete it: a plain
	// session durable is created before the record save that references it, so a
	// young unreferenced consumer may just be mid-SUBSCRIBE. A var so tests can
	// shorten it; the orphan QoS 2 sweep additionally supports an instance-scoped
	// override, because it is read by a goroutine a test cannot synchronize with.
	mqttOrphanSweepGrace = 5 * time.Minute
	// Ceiling on how long a QoS 1/2 delivery waits for the resume reconciliation
	// phase to release its subscription. The effective bound is the consumer's
	// AckWait (mqttPhaseGateWaitFor): waiting past it buys nothing, because
	// JetStream re-pushes the message at exactly that point anyway. The bound
	// exists so a CONNECT that dies without releasing — every ordinary exit
	// releases, so that means a panic — cannot park a delivery goroutine
	// indefinitely. A var so tests can shorten it. See mqttSub.phaseGate.
	mqttPhaseGateWaitMax = 30 * time.Second
)

// mqttPhaseGateWaitFor is how long a delivery waits on a reconciliation phase
// gate: the connection's effective AckWait, capped at mqttPhaseGateWaitMax.
// Tying it to AckWait is what keeps the wait from changing the outcome — at
// that deadline JetStream redelivers regardless, and the skipped attempt is
// recorded so the redelivery is still the client's first transmission.
func mqttPhaseGateWaitFor(c *client) time.Duration {
	wait := mqttDefaultAckWait
	if c != nil && c.srv != nil {
		if aw := c.srv.getOpts().MQTT.AckWait; aw > 0 {
			wait = aw
		}
	}
	return min(wait, mqttPhaseGateWaitMax)
}

// How often to re-run the session-expiry sweep for an account.
//
// Atomic rather than a plain var: every account manager's sweep goroutine reads
// this, and those goroutines outlive the test that created them, so a test
// shortening the interval would otherwise race every sweeper still draining from an
// earlier test. Only the ticker construction reads it, so a change takes effect for
// account managers created afterwards.
var mqttSessionExpirySweepIntervalNS = func() *atomic.Int64 {
	v := &atomic.Int64{}
	v.Store(int64(15 * time.Minute))
	return v
}()

func mqttSessionExpirySweepInterval() time.Duration {
	return time.Duration(mqttSessionExpirySweepIntervalNS.Load())
}

// setMQTTSessionExpirySweepInterval installs a new interval and returns the restore
// func. Test-only, but it lives here beside the value it guards.
func setMQTTSessionExpirySweepInterval(d time.Duration) func() {
	old := mqttSessionExpirySweepIntervalNS.Swap(int64(d))
	return func() { mqttSessionExpirySweepIntervalNS.Store(old) }
}

// Test hook: called once sweepSessionExpiries has finished a full pass over the
// records. A test seeding an already-overdue record needs to know that the
// STARTUP pass is behind it — that pass is launched asynchronously from the
// account manager's own goroutine with no completion signal, so a test that
// merely waited would race it arming an immediate expiry timer and deleting the
// fixture out from under the assertion.
//
// Atomic for the same reason as the save hook: the sweep goroutine outlives the
// test that created its account manager.
var mqttTestSessExpirySweepPassHookV atomic.Pointer[func(as *mqttAccountSessionManager)]

// setMQTTTestSessExpirySweepPassHook installs (or, with nil, clears) the sweep-pass
// hook and returns a function that restores the previous value.
func setMQTTTestSessExpirySweepPassHook(h func(as *mqttAccountSessionManager)) func() {
	prev := mqttTestSessExpirySweepPassHookV.Load()
	if h == nil {
		mqttTestSessExpirySweepPassHookV.Store(nil)
	} else {
		mqttTestSessExpirySweepPassHookV.Store(&h)
	}
	return func() { mqttTestSessExpirySweepPassHookV.Store(prev) }
}

// Test hook: when set, sess.save() consults it and fails with the returned error
// instead of persisting. Used to exercise persistence-failure rollback.
//
// Held in an atomic because saves run on server goroutines that outlive the
// client connection a test just closed: clearing a plain global in a deferred
// cleanup races a disconnect-path save still in flight.
var mqttTestSessSaveHookV atomic.Pointer[func(sess *mqttSession) error]

// setMQTTTestSessSaveHook installs (or, with nil, clears) the save hook and
// returns a function that restores the previous value.
func setMQTTTestSessSaveHook(h func(sess *mqttSession) error) func() {
	prev := mqttTestSessSaveHookV.Load()
	if h == nil {
		mqttTestSessSaveHookV.Store(nil)
	} else {
		mqttTestSessSaveHookV.Store(&h)
	}
	return func() { mqttTestSessSaveHookV.Store(prev) }
}

// Test hook: when set, it is invoked after a teardown claim is CONFIRMED and
// before the first deletion — the exact window a nonce re-check cannot close,
// where a paused broker can let a peer finish the whole teardown and create a
// fresh session before its own deletes land. Nothing else can reproduce that
// interleaving from outside, so the seam is deliberate.
//
// Atomic for the same reason as the save hook above.
var mqttTestTeardownClaimedHookV atomic.Pointer[func(sess *mqttSession, nonce string, markerSeq uint64)]

// setMQTTTestTeardownClaimedHook installs (or, with nil, clears) the post-claim
// hook and returns a function that restores the previous value.
func setMQTTTestTeardownClaimedHook(h func(sess *mqttSession, nonce string, markerSeq uint64)) func() {
	prev := mqttTestTeardownClaimedHookV.Load()
	if h == nil {
		mqttTestTeardownClaimedHookV.Store(nil)
	} else {
		mqttTestTeardownClaimedHookV.Store(&h)
	}
	return func() { mqttTestTeardownClaimedHookV.Store(prev) }
}

// Test hooks: when set and returning a non-nil error, loadLastMsgFor and
// deleteMsg fail with it WITHOUT issuing the JetStream request, so the stream
// is left exactly as it was and a retry can still succeed. Used to exercise the
// QoS2 PUBREL failure classification.
//
// Deliberately per-operation and predicated on (stream, subject) / (stream,
// sequence) rather than one hook on jsa.newRequest: the broker issues unrelated
// JetStream requests concurrently from other goroutines, and a blanket hook
// would fail those too and turn these tests into misleading noise. Atomics for
// the same reason as the save hook above.
// mqttTestStoreMsgHookV is the same idea for storeMsg, predicated on the publish
// subject (so a test can target only "$MQTT.msgs.>" or only "$MQTT.qos2.in.>" and
// leave session persistence and JSA traffic alone). It also receives the encoded
// message, which lets a test OBSERVE what was written — the Nats-Msg-Id stability
// assertions need that and cannot get it any other way. Returning a non-nil error
// fails the store without issuing the request; returning nil lets it proceed.
var (
	mqttTestLoadLastMsgHookV atomic.Pointer[func(stream, subject string) error]
	// mqttTestLoadNextMsgHookV fails a forward scan, so a test can make the
	// past-PUBREC barrier's walk unreadable and assert it fails CLOSED rather than
	// reading an unreadable scan as "no PUBREL owed".
	mqttTestLoadNextMsgHookV atomic.Pointer[func(stream, subject string, seq uint64) error]
	mqttTestDeleteMsgHookV   atomic.Pointer[func(stream string, seq uint64) error]
	mqttTestStoreMsgHookV    atomic.Pointer[func(subject string, hdrLen int, msg []byte) error]
)

// setMQTTTestLoadLastMsgHook installs (or, with nil, clears) the loadLastMsgFor
// hook and returns a function that restores the previous value.
func setMQTTTestLoadLastMsgHook(h func(stream, subject string) error) func() {
	prev := mqttTestLoadLastMsgHookV.Load()
	if h == nil {
		mqttTestLoadLastMsgHookV.Store(nil)
	} else {
		mqttTestLoadLastMsgHookV.Store(&h)
	}
	return func() { mqttTestLoadLastMsgHookV.Store(prev) }
}

// setMQTTTestDeleteMsgHook installs (or, with nil, clears) the deleteMsg hook
// and returns a function that restores the previous value.
// mqttTestSuppressMsgTTLV drops the JetStream per-message TTL derived from a v5
// Message Expiry Interval, leaving the MQTT property itself intact. See
// mqttStoredMsgTTL.
var mqttTestSuppressMsgTTLV atomic.Bool

// mqttTestSessStoreResultHookV rewrites the outcome of a SESSION RECORD write
// after it has been issued, so a test can produce the one case the pre-write hook
// cannot: a write that LANDED and whose response was lost. That is what makes
// sess.seq stale and drives the three-way readback.
var mqttTestSessStoreResultHookV atomic.Pointer[func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)]

// mqttTestSessStoreSuppressV drops a session-record store before it is issued and
// reports an unclassifiable error, producing an ambiguous failure whose write
// genuinely did NOT land. See storeSessionMsg.
var mqttTestSessStoreSuppressV atomic.Bool

func setMQTTTestSessStoreResultHook(h func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)) func() {
	prev := mqttTestSessStoreResultHookV.Load()
	if h == nil {
		mqttTestSessStoreResultHookV.Store(nil)
	} else {
		mqttTestSessStoreResultHookV.Store(&h)
	}
	return func() { mqttTestSessStoreResultHookV.Store(prev) }
}

// mqttTestPubRelPreForwardHookV fires AFTER a drained exchange's claim removal has
// been persisted and BEFORE the PUBREL is forwarded — the exact window a
// forward-first implementation would leave open. A test can inspect the persisted
// record there and prove the ordering, rather than inferring it from a state that
// both orderings eventually reach.
var mqttTestPubRelPreForwardHookV atomic.Pointer[func()]

func setMQTTTestPubRelPreForwardHook(h func()) func() {
	prev := mqttTestPubRelPreForwardHookV.Load()
	if h == nil {
		mqttTestPubRelPreForwardHookV.Store(nil)
	} else {
		mqttTestPubRelPreForwardHookV.Store(&h)
	}
	return func() { mqttTestPubRelPreForwardHookV.Store(prev) }
}

// mqttTestPubRelDeliveryDelayV stalls PUBREL delivery, so a test can let the
// SOURCE redelivery win the race the past-PUBREC barrier exists to settle. Without
// it the callback always arrives first and the barrier looks redundant.
var mqttTestPubRelDeliveryDelayV atomic.Pointer[func()]

func setMQTTTestPubRelDeliveryDelay(h func()) func() {
	prev := mqttTestPubRelDeliveryDelayV.Load()
	if h == nil {
		mqttTestPubRelDeliveryDelayV.Store(nil)
	} else {
		mqttTestPubRelDeliveryDelayV.Store(&h)
	}
	return func() { mqttTestPubRelDeliveryDelayV.Store(prev) }
}

func setMQTTTestLoadNextMsgHook(h func(stream, subject string, seq uint64) error) func() {
	prev := mqttTestLoadNextMsgHookV.Load()
	if h == nil {
		mqttTestLoadNextMsgHookV.Store(nil)
	} else {
		mqttTestLoadNextMsgHookV.Store(&h)
	}
	return func() { mqttTestLoadNextMsgHookV.Store(prev) }
}

func setMQTTTestDeleteMsgHook(h func(stream string, seq uint64) error) func() {
	prev := mqttTestDeleteMsgHookV.Load()
	if h == nil {
		mqttTestDeleteMsgHookV.Store(nil)
	} else {
		mqttTestDeleteMsgHookV.Store(&h)
	}
	return func() { mqttTestDeleteMsgHookV.Store(prev) }
}

// mqttTestStoreMsgResultHookV is the POST-store sibling of mqttTestStoreMsgHookV:
// it runs after the store has actually been issued, and rewrites what the caller
// sees. That is the half that matters for ambiguity — a store that landed, and
// may already have been delivered, whose response was lost — which the pre-store
// hook cannot model at all, since it prevents the store from happening.
var mqttTestStoreMsgResultHookV atomic.Pointer[func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)]

// setMQTTTestStoreMsgResultHook installs (or, with nil, clears) the post-store
// hook and returns a function that restores the previous value.
func setMQTTTestStoreMsgResultHook(h func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)) func() {
	prev := mqttTestStoreMsgResultHookV.Load()
	if h == nil {
		mqttTestStoreMsgResultHookV.Store(nil)
	} else {
		mqttTestStoreMsgResultHookV.Store(&h)
	}
	return func() { mqttTestStoreMsgResultHookV.Store(prev) }
}

// setMQTTTestStoreMsgHook installs (or, with nil, clears) the storeMsg hook and
// returns a function that restores the previous value.
func setMQTTTestStoreMsgHook(h func(subject string, hdrLen int, msg []byte) error) func() {
	prev := mqttTestStoreMsgHookV.Load()
	if h == nil {
		mqttTestStoreMsgHookV.Store(nil)
	} else {
		mqttTestStoreMsgHookV.Store(&h)
	}
	return func() { mqttTestStoreMsgHookV.Store(prev) }
}

var (
	errMQTTNotWebsocketPort           = errors.New("MQTT clients over websocket must connect to the Websocket port, not the MQTT port")
	errMQTTTopicFilterCannotBeEmpty   = errors.New("topic filter cannot be empty")
	errMQTTMalformedVarInt            = errors.New("malformed variable int")
	errMQTTSecondConnectPacket        = errors.New("received a second CONNECT packet")
	errMQTTServerNameMustBeSet        = errors.New("mqtt requires server name to be explicitly set")
	errMQTTUserMixWithUsersNKeys      = errors.New("mqtt authentication username not compatible with presence of users/nkeys")
	errMQTTTokenMixWIthUsersNKeys     = errors.New("mqtt authentication token not compatible with presence of users/nkeys")
	errMQTTAckWaitMustBePositive      = errors.New("ack wait must be a positive value")
	errMQTTJSAPITimeoutMustBePositive = errors.New("JS API timeout must be a positive value")
	errMQTTStandaloneNeedsJetStream   = errors.New("mqtt requires JetStream to be enabled if running in standalone mode")
	errMQTTConnFlagReserved           = errors.New("connect flags reserved bit not set to 0")
	errMQTTWillAndRetainFlag          = errors.New("if Will flag is set to 0, Will Retain flag must be 0 too")
	errMQTTPasswordFlagAndNoUser      = errors.New("password flag set but username flag is not")
	errMQTTCIDEmptyNeedsCleanFlag     = errors.New("when client ID is empty, clean session flag must be set to 1")
	errMQTTEmptyWillTopic             = errors.New("empty Will topic not allowed")
	errMQTTEmptyUsername              = errors.New("empty user name not allowed")
	errMQTTTopicIsEmpty               = errors.New("topic cannot be empty")
	errMQTTPacketIdentifierIsZero     = errors.New("packet identifier cannot be 0")
	errMQTTUnsupportedCharacters      = errors.New("character not supported for MQTT topics")
	errMQTTInvalidSession             = errors.New("invalid MQTT session")
	errMQTTInvalidRetainFlags         = errors.New("invalid retained message flags")
	// A stored retained record without the Nmqtt-RFlags header is not in the
	// format this broker writes. It is rejected as an unsupported format rather
	// than guessed at — a distinct fact from "the flags value is out of range".
	errMQTTRetainedMsgUnsupportedFormat = errors.New("retained message without Nmqtt-RFlags header: unsupported pre-release format")
	errMQTTSessionCollision             = errors.New("stored session does not match client ID")
	errMQTTInvalidPublishLength         = errors.New("invalid publish message, variable header exceeds remaining length")
	errMQTTMalformedProperties          = errors.New("malformed properties")
	errMQTTProtocolError                = errors.New("protocol error")
	errMQTTDuplicateProperty            = errors.New("duplicate property")
	errMQTTUnknownProperty              = errors.New("unknown property identifier")
	errMQTTPropertyNotAllowed           = errors.New("property not allowed for this packet type")
	errMQTTAuthMethodNotSupported       = errors.New("enhanced authentication (auth method) is not supported")
	errMQTTUnsupportedSubOption         = errors.New("unsupported MQTT 5.0 subscription option")
	errMQTTMalformedSubOption           = errors.New("malformed MQTT 5.0 subscription option")
	errMQTTMalformedSharedSub           = errors.New("malformed shared subscription")
	errMQTTTopicAliasInvalid            = errors.New("topic alias invalid")
	errMQTTReceiveMaxExceeded           = errors.New("receive maximum exceeded")
	errMQTTPayloadFormatInvalid         = errors.New("payload format invalid")
	errMQTTNotAuthorizedToSubscribe     = errors.New("not authorized to subscribe")
	errMQTTMalformedPacket              = errors.New("malformed packet")
)

type srvMQTT struct {
	listener     net.Listener
	listenerErr  error
	authOverride bool
	sessmgr      mqttSessionManager
	// v5Enabled caches whether this server accepts MQTT 5.0 (max_protocol_version
	// == 5), set once at startup and read lock-free thereafter. It lets v5-only
	// machinery be skipped entirely on a 3.1.1-only server.
	v5Enabled bool
	// originKey is a per-server-run random secret keying the HMAC that
	// authenticates the No Local origin marker stamped on stored QoS 1/2 (and
	// retained) messages. It is never sent to clients, so a client that can write
	// the MQTT stream subjects still cannot forge a marker for a victim's session;
	// an unauthenticated or unverifiable origin is simply not suppressed (No Local
	// fails open, never dropping a legitimate message). Because the key is not
	// persisted or shared across the cluster, a self-published message that
	// outlives its stamping server-run — a retained/QoS message replayed after a
	// restart, or delivered after the persistent session reconnects to another
	// node — no longer verifies and is delivered rather than suppressed. That is
	// an accepted, safe degradation of No Local for persisted messages across a
	// restart/failover, not a security gap. Spec5 [3.8.3.1].
	originKey []byte
}

type mqttSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*mqttAccountSessionManager // key is account name

	// testSessCreateGate / testSessCreateObserve are instance-scoped test seams,
	// nil in production. The gate is invoked after the $MQTT_sess lookup reports the
	// stream absent and BEFORE createStream, so a test can barrier two racing
	// managers at the exact lookup->create boundary; the observer receives the
	// createStream outcome (created, err) so a test can prove exactly one manager
	// won the create and the other saw JSStreamNameExistErr. See
	// mqttCreateAccountSessionManager.
	testSessCreateGate    func()
	testSessCreateObserve func(created bool, err error)

	// testRetainedApplyGate is an instance-scoped test seam, nil in production,
	// invoked by processRetainedMsg with the decoded record's logical subject and
	// whether it is a tombstone, immediately BEFORE the record is applied to the
	// index. Holding one application there lets a test run the other index writer
	// — mqttHandlePubRetain's synchronous local seed — ahead of it, which is the
	// reordering F-059 describes and the window that made the removed compaction
	// delete destructive. See processRetainedMsg.
	testRetainedApplyGate func(subject string, tombstone bool)
}

type mqttAccountSessionManager struct {
	mu         sync.RWMutex
	sessions   map[string]*mqttSession // key is MQTT client ID
	sessByHash map[string]*mqttSession // key is MQTT client ID hash
	sessLocked map[string]struct{}     // key is MQTT client ID and indicate that a session can not be taken by a new client at this time
	flappers   map[string]time.Time    // When connection connects with client ID already in use
	flapTimer  *time.Timer             // Timer to perform some cleanup of the flappers map
	// Delayed Wills are no longer tracked in memory here: they live on the durable
	// $MQTT_willq outbox and are driven by the claim loop. See will_outbox.go.
	// Pending session-expiry timers, keyed by MQTT client ID hash, that clean up a
	// session once its MQTT 5.0 Session Expiry Interval elapses after the
	// connection closes. A reconnect (local or remote takeover) cancels them.
	// Spec5 [3.1.2.11.2]. Guarded by mu.
	pendingExpiries map[string]*time.Timer
	// Per-client reconciliation ordering state, keyed by MQTT client ID hash and
	// guarded by mu. A remote session-record write observed by the session-record watch
	// hands the expiry reconciliation to a spawned worker; because each worker
	// loads the record before taking mu, worker completion order can reverse the
	// (ordered) delivery order. latest is the highest observed PubAck sequence for
	// the client, inFlight counts admitted-but-not-yet-completed workers. A worker
	// applies its decision only if it is still the latest observation, and the
	// entry is pruned only once inFlight drops to zero so a worker parked before mu
	// cannot resume into a pruned-away watermark. See reconcileRemoteSessionExpiry.
	reconcileWM map[string]*sessionReconcileState
	// refusedWillRecords is the set of $MQTT_willq subjects whose record has
	// already been reported unsupported, so the 5s claim scan warns once per
	// record rather than every pass. Guarded by mu; bounded by the number of
	// distinct refused subjects, and never cleared because a refused record is
	// permanent until an operator removes it. See refuseWillOutboxRecord.
	refusedWillRecords map[string]struct{}
	retmsgs            *stree.SubjectTree[mqttRetainedMsgRef] // retained message metadata
	rmsCache           *sync.Map                              // map[subject]mqttRetainedMsg
	jsa                mqttJSA
	domainTk           string // Domain (with trailing "."), or possibly empty. This is added to session subject.

	// sessStreamEpoch is the $MQTT_sess incarnation identity resolved once at
	// account-manager init from the stream's mqtt_sess_epoch metadata (form
	// "nuid:<NUID>"). It stamps every watermark (SessEpoch) and native Will record
	// (OriginSessEpoch); a sequence is only comparable across the two when the
	// epochs are equal. Immutable after init (read without lock). If a valid epoch
	// cannot be resolved, the account manager is not published. See
	// validateSessStream, willTakeoverFence.
	sessStreamEpoch string

	// testPublishWill, when non-nil, replaces the delayed-Will publication for
	// THIS account manager only (instance-scoped, so parallel tests stay
	// isolated). Set/read under mu. Used to inject publish failures and verify
	// cross-replica recovery. Nil in production. See publishWillRecord.
	testPublishWill func(rec *mqttWillOutbox) error

	// testEnqueueStore, when non-nil, replaces the durable enqueue store for THIS
	// account manager only (instance-scoped). Set/read under mu. Used to force the
	// enqueue CAS to fail so the immediate-publish fail-safe is exercised. Nil in
	// production. See storeWillEnqueueCAS / enqueueDelayedWill.
	testEnqueueStore func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error)

	// testFinalizeStore, when non-nil, replaces the terminal publishing->done CAS
	// store for THIS account manager only (instance-scoped). Set/read under mu.
	// Used to fail or ambiguate the terminal commit and verify the finalizer's
	// re-read/reconcile path. Nil in production. See finalizeWillPublish.
	testFinalizeStore func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error)

	// testSessionLoad, when non-nil, replaces the session-record load used by the
	// delayed-Will takeover fence for THIS account manager only (instance-scoped).
	// Set/read under mu. Used to drive the fence decision (a connected/disconnected
	// newer generation) and to inject a load error so the fence defers on an
	// indeterminate read. Nil in production. See loadFenceSession.
	testSessionLoad func(cidHash string) (*mqttPersistedSession, uint64, error)

	// testCancelWillDone, when non-nil, is invoked after each cancelWillOutbox pass
	// for THIS account manager only (instance-scoped). Set/read under mu. Lets the
	// takeover late-enqueue test wait until the displaced broker's asynchronous
	// cancel scan (the session-record watch's observation) has completed on the still-empty outbox
	// before releasing the blocked enqueue, so a stray old cancel cannot be mistaken
	// for the new enqueue-side fence. Nil in production.
	testCancelWillDone func()

	// testBeforeTeardownLock, when non-nil, is invoked by mqttHandleClosedClient for
	// THIS account manager only (instance-scoped) after the closing connection has
	// recorded what its Will is owed, but BEFORE it takes the session gate. Set/read
	// under mu. Lets a test park the teardown there and drive a same-client-ID
	// CONNECT to completion, so "the close was observed first, then the session was
	// rebound" is an ordering rather than a probability. Nil in production.
	testBeforeTeardownLock func(clientID string)

	// testBeforeTakeoverDiscard, when non-nil, is invoked by mqttProcessConnect for
	// THIS account manager only (instance-scoped) after a takeover has rebound the
	// session to the new connection and before it decides the displaced
	// connection's Will. Set/read under mu. That gap is the window in which the
	// displaced connection can classify its own Will, so a test needs to hold it
	// open to pin which interval that classification uses. Nil in production.
	testBeforeTakeoverDiscard func(clientID string)

	// testBeforeResumeNaks, when non-nil, is invoked on the resume path for THIS
	// account manager only (instance-scoped) after subscriptions have been restored
	// and before resendPendingRedelivery runs. Set/read under mu. Lets a test hold
	// the resume open until a fresh delivery has certainly landed in the pending
	// map, which is the window in which a NAK would duplicate it. Nil in production.
	testBeforeResumeNaks func(clientID string)

	// testResumeReconcileLookup, when non-nil, replaces the resume reconciliation
	// phase's consumer lookup for THIS account manager only (instance-scoped).
	// Set/read under mu. It is what lets a test fail (or delay) a chosen batch of
	// a REAL CONNECT deterministically — the batching helper's function parameter
	// alone would leave the CONNECT path hardwired. Nil in production. See
	// reconcileResumedConsumers.
	testResumeReconcileLookup func(durables []string) ([]*ConsumerInfo, []bool, error)

	// testResumeReconcilePreCreate, when non-nil, is invoked by the resume
	// reconciliation phase for THIS account manager only (instance-scoped)
	// immediately before it creates a replacement durable, with the deliver
	// subject the flushed subscription was installed on. Returning a non-nil
	// error rejects the create WITHOUT issuing it — the same failure surface as
	// a JetStream rejection. Set/read under mu. It is the seam that pins
	// subscribe-BEFORE-create as an ordering assertion rather than a race
	// against the reaper, and the lever for a forced repair failure. Nil in
	// production.
	testResumeReconcilePreCreate func(sess *mqttSession, deliver string) error

	// testReconcilePause, when non-nil, is invoked by a remote-expiry reconciliation
	// worker for THIS account manager only (instance-scoped) AFTER it has loaded the
	// record but just BEFORE it takes mu to apply its decision. Set/read under mu.
	// Lets the worker-ordering tests park a stale worker while a newer worker (or a
	// local reconnect/re-disconnect) races ahead, so the ordering/ownership guards
	// are exercised deterministically. Nil in production. See
	// reconcileRemoteSessionExpiry.
	testReconcilePause func(idHash string)

	// testEvictPauseBeforeLoad / testEvictPauseAfterLoad, when non-nil, are invoked
	// by an eviction worker for THIS account manager only (instance-scoped) around
	// its per-iteration record load: BEFORE the load (after the trigger read) and
	// AFTER the load/decode, both outside every lock, so a handler may block. They
	// are the levers for the replacement-object, stale-save, pair-freshness and
	// coalescing races, and pauseBeforeLoad is where test 14(a)'s Goexit exercises
	// the pre-fence abnormal cleanup. Set/read under mu. Nil in production. See
	// evictDisplacedOnRemotePersist.
	testEvictPauseBeforeLoad func(idHash string)
	testEvictPauseAfterLoad  func(idHash string)

	// testEvictPostFence, when non-nil, is invoked by an eviction worker's decision
	// section for THIS account manager only, with as.mu HELD and the record fence
	// OPEN — the window an abnormal exit must clean up from. A handler may panic or
	// Goexit (the deferred guard releases the fence handoff-free and the deferred
	// unlock releases as.mu, in that LIFO order) but must NOT block: it pins as.mu.
	// Read under the held mu. Nil in production. See evictDecideOnce.
	testEvictPostFence func(idHash string)

	// testEvictCleanupEntry, when non-nil, is invoked by an eviction worker at the
	// ENTRY of its terminal cleanup, outside every lock — before the as.mu section
	// that re-checks the trigger watermark and clears the coalescing flag. It is
	// what makes the cleanup's atomic re-check causally testable: a handler can
	// release a gate entry and deliver a newer observation here, and the same
	// worker must loop instead of stranding the trigger. Set/read under mu. Nil in
	// production. See evictDisplacedOnRemotePersist.
	testEvictCleanupEntry func(idHash string)

	// testEvictComplete, when non-nil, is invoked by an eviction worker AFTER its
	// cleanup on both the normal and abnormal exit paths, outside every lock,
	// carrying whether an eviction was applied. Tests awaiting "no eviction"
	// outcomes assert after this seam, so the negative is the worker's DECISION
	// rather than its absence. Set/read under mu. Nil in production. See
	// evictDisplacedOnRemotePersist.
	testEvictComplete func(idHash string, applied bool)

	// testSessPersistDrop, when non-nil, drops the remote persist observation for the
	// client ID hashes it returns true for, on THIS account manager only. Set/read
	// under mu. That is the state a lost core-NATS notification leaves — the remote
	// store happened and this broker never learned of it — which is the condition the
	// record-rejection settlement exists for and which nothing else can produce
	// deterministically. Hash-keyed so a parallel test and every unrelated session on
	// this manager are unaffected. Nil in production. See processSessionRecordDelivery.
	testSessPersistDrop func(idHash string) bool
	// testSessRecordBackstopOff disables the periodic ownership backstop
	// (reconcileResidentOwnership) for this manager. It is the counterpart of
	// testSessPersistDrop, and the two exist so a test can isolate ONE convergence
	// channel: with both live, a test that means to prove the watch converged a
	// session would also pass if only the backstop did, and vice versa. Read under
	// mu; false in production.
	testSessRecordBackstopOff bool

	// testCancelWatermarkLoad / testCancelWatermarkStore, when non-nil, replace the
	// per-client cancel-watermark load / CAS store for THIS account manager only
	// (instance-scoped). Set/read under mu. Used to drive the takeover fence off a
	// planted/stale/corrupt watermark and to fail the CONNECT-side advance so the
	// gating and failure transaction are exercised. Nil in production. See
	// loadWillCancelWatermark / storeWillCancelWatermarkCAS.
	testCancelWatermarkLoad  func(cidHash string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus)
	testCancelWatermarkStore func(subject string, expectSeq uint64, wm *mqttWillCancelWatermark) (uint64, error)

	// testQoS2HoldStore / testQoS2FinalizeStore / testQoS2Load, when non-nil,
	// replace the inbound QoS 2 move protocol's create-only hold CAS, its terminal
	// held->delivered CAS, and its classified record load, for THIS account manager
	// only (instance-scoped). Set/read under mu. Each is routed separately so a test
	// can fail exactly one step of the move — the reconcile loops only make sense
	// when the OTHER steps still reach real JetStream. Nil in production. See
	// qos2_move.go.
	testQoS2HoldStore     func(subject string, expectSeq uint64, hdrLen int, msg []byte) (uint64, error)
	testQoS2FinalizeStore func(subject, gen string, expectSeq uint64) (uint64, error)
	testQoS2Load          func(subject string) (*mqttQoS2Record, uint64, error)

	// testQoS2FinalizeDone, when non-nil, is invoked with the RESOLVED status of every
	// terminal commit on THIS account manager (instance-scoped). Set/read under mu.
	// It is the only way to assert what a finalize decided when its client cannot be
	// asked: a cross-broker takeover closes the connection whatever the finalize
	// concluded, so the wire carries no evidence, and a stalled finalize is
	// indistinguishable from a displaced one to a reader. Nil in production. See
	// finalizeQoS2Move.
	testQoS2FinalizeDone func(subject, gen string, status mqttQoS2FinalizeStatus)

	// testQoS2PreFinalize, when non-nil, is invoked immediately BEFORE the terminal
	// held->delivered CAS, with the generation and sequence the PUBREL loaded. This
	// is the only seam that can reach into the load..commit window, which is where
	// every interesting race lives: a test uses it to purge the held record (driving
	// Gone) or to replace it with a NEW generation (driving Superseded). The
	// pre-existing deleteMsg hook cannot do either — after the reorder deleteMsg is
	// terminal cleanup that runs AFTER the commit. A returned error aborts the
	// finalize as indeterminate. Nil in production. See finalizeQoS2Move.
	testQoS2PreFinalize func(subject, gen string, seq uint64) error

	// Seams for the inbound QoS 2 RELEASE CLAIM (qos2_release_claim.go),
	// instance-scoped and set/read under mu. Routed separately from the move
	// protocol's seams because the claim is a different register on a different
	// subject: a test must be able to fail or observe the claim while the hold
	// store, the terminal commit and the record loads all still reach real
	// JetStream. Nil in production.
	testQoS2ClaimStore func(subject string, expectSeq uint64, c *mqttQoS2RelClaim) (uint64, error)
	testQoS2ClaimLoad  func(subject string) (*mqttQoS2RelClaim, *StoredMsg, error)

	// testQoS2ClaimDone, when non-nil, is invoked with the RESOLVED status of every
	// release claim on THIS account manager. Same reason as testQoS2FinalizeDone: a
	// releaser that correctly declines to deliver sends nothing at all, and on a
	// takeover its connection is already closed, so the wire carries no evidence of
	// what it decided. Nil in production. See claimQoS2Release.
	testQoS2ClaimDone func(holdSubject, gen string, status mqttQoS2ClaimStatus)

	// testQoS2PreClaim, when non-nil, is invoked immediately BEFORE the first
	// create-only claim CAS, with the generation and hold sequence the PUBREL loaded.
	// It is the only seam inside the load..claim window, which is where the race this
	// protocol arbitrates lives: a test uses it to discard the exchange from another
	// broker (driving Discarded), or to let a peer complete and reap the whole
	// exchange (driving the stalled-loader case that the post-claim re-read catches).
	// A returned error abandons the claim as indeterminate. Nil in production.
	testQoS2PreClaim func(holdSubject, gen string, holdSeq uint64) error

	// Seams for the shared-subscription QoS 2 session-affine move
	// (qos2_shared_affine.go), instance-scoped and set/read under mu. Each step is
	// routed separately because every interesting failure here is "this ONE
	// operation did not commit while the others did": a claim CAS that is rejected
	// or lost, a copy store that succeeds while its response is lost, or a load
	// that is indeterminate rather than absent. Nil in production.
	testQ2AffClaimStore func(subject string, expectSeq uint64, rec *mqttQ2AffClaim) (uint64, error)
	testQ2AffClaimLoad  func(subject string) (*mqttQ2AffClaim, uint64, error)
	testQ2AffCopyStore  func(subject string, hdrLen int, msg []byte) (uint64, error)
	testQ2AffCopyLoad   func(subject string) (*StoredMsg, error)

	// testQ2AffMoveDone, when non-nil, reports the resolved disposition of every
	// shared QoS 2 move attempt on this account manager. Like testQoS2FinalizeDone,
	// it exists because the decision is often unobservable on the wire: a contender
	// that correctly declines to deliver sends nothing at all, and "declined" and
	// "still running" look identical to a reader. Nil in production.
	testQ2AffMoveDone func(claimSubject string, res mqttQ2AffMoveResult)

	// testQ2AffPreMaterialize, when non-nil, runs immediately BEFORE the
	// claimed->materialized CAS, after the copy has been stored. It is the only
	// seam inside the window where the copy is visible to a live affine consumer
	// but the claim does not yet authorize delivery — the window the delivery gate
	// exists for. A returned error abandons the transition. Nil in production.
	testQ2AffPreMaterialize func(claimSubject string, seq uint64) error

	// testRollbackSessionStore / testRollbackSessionLoad, when non-nil, replace the
	// session-record CAS store / load used ONLY by the CONNECT-accept watermark
	// failure transaction (instance-scoped). Set/read under mu. Routed only through
	// the rollback path so a test can fail the rollback (and its confirming re-read)
	// without perturbing the initial es.save() or unrelated fence loads, exercising
	// the rollbackIndeterminate classification. Nil in production. See
	// rollbackAcceptedSession.
	testRollbackSessionStore func(ps *mqttPersistedSession, idHash string, seq uint64) error
	testRollbackSessionLoad  func(cidHash string) (*mqttPersistedSession, uint64, error)

	// testRollbackDone, when non-nil, is invoked at the end of the CONNECT-accept
	// watermark-failure transaction with the detached session and its classified
	// result, so a test can assert the unconditional guarantees (es.c == nil, the
	// result classification) precisely. Instance-scoped, set/read under mu, nil in
	// production. See rollbackAcceptedSession.
	testRollbackDone func(es *mqttSession, result rollbackResult)

	// Seams for the outbound in-flight register (inflight_register.go),
	// instance-scoped and set/read under mu. Routed only through the register's own
	// store and load so a test can fail, stall or observe the write that must
	// PRECEDE a send while the source delivery, the PUBREL store and every unrelated
	// load still reach real JetStream. A store hook that reports success models
	// "landed, response lost", which is the ambiguous outcome the nonce re-read
	// resolves. Nil in production.
	testInflightStore func(subject string, expectSeq uint64, r *mqttInflightRecord) (uint64, error)
	testInflightLoad  func(subject string) (*mqttInflightRecord, *StoredMsg, error)

	// testInflightEnumerate, when non-nil, replaces the resume barrier's subject
	// enumeration. Routed separately from the loads because "the identifier space could
	// not be read at all" and "one register could not be loaded" are different failures
	// with the same remedy, and a test needs to reach each on its own. Nil in production.
	testInflightEnumerate func(filter string) (map[string]uint64, error)

	// testInflightSourceAck, when non-nil, replaces the CONFIRMED source acknowledgement
	// on the QoS 2 PUBREC path — the one that licenses deleting the in-flight register.
	// It exists because that failure is unobservable from outside: a confirmation that
	// does not land looks exactly like one that does, and what the broker must then do
	// (keep the register, and keep the source correlated so its redelivery is
	// suppressed) is visible only in what does NOT appear on the wire. Nil in
	// production.
	testInflightSourceAck func(ackSubject string) error

	// testInflightDelete, when non-nil, replaces the exact-sequence register delete.
	// It exists because R5's failure mode is unobservable on the wire: a delete that
	// never lands leaves a stale record that only a later scan reveals, so a test
	// that wants to reach the "completed exchange, register survived" state has to
	// suppress the delete rather than race it. Nil in production.
	testInflightDelete func(seq uint64) error

	// qos2SweepCursor is the orphan QoS 2 sweep's position in $MQTT_qos2in, and
	// qos2SweepSeeded whether it has been seeded yet. Scheduling state only: every
	// candidate it discovers is authoritatively revalidated before any mutation, so
	// losing, wrapping or reseeding the cursor changes only which records a given
	// pass looks at. Guarded by mu. See orphan_qos2_sweep.go.
	qos2SweepCursor uint64
	qos2SweepSeeded bool

	// inflightSweepCursor / inflightSweepSeeded are the same scheduling state for the
	// $MQTT_inflight walk. Separate from the inbound pair because the two streams have
	// independent sequence spaces and independent budgets; sharing one cursor would
	// make each pass's coverage of one stream depend on the other's size. Guarded by
	// mu. See orphan_qos2_sweep.go.
	inflightSweepCursor uint64
	inflightSweepSeeded bool

	// testQoS2SweepSeed, when non-nil, chooses the sweep cursor's initial position
	// instead of the pseudo-random seed, so a test can make one pass deterministic.
	// Instance-scoped, set/read under mu, nil in production.
	testQoS2SweepSeed func(firstSeq, lastSeq uint64) uint64

	// Instance-scoped overrides for the orphan sweep's age policies and discovery
	// budget. Pointers, because zero is a value a test legitimately wants to set.
	// Set/read under mu, nil in production — package-level knobs cannot be used here
	// because the account manager's sweep goroutine reads them concurrently with any
	// test that would swap them.
	testOrphanGrace          *time.Duration
	testQoS2ReleaseGrace     *time.Duration
	testQoS2SweepRecordLimit *int
	testQoS2SweepByteLimit   *int64

	// sweepQuarantine records how long each orphan-sweep candidate has been
	// continuously observed as an orphan, on this process's monotonic clock. Created
	// on first use; see orphan_qos2_sweep.go for why no JetStream timestamp can serve
	// that purpose.
	sweepQuarantine *mqttSweepQuarantine
}

// sessionReconcileState orders remote-persist workers for a single client ID
// hash: the no-local expiry reconciliation worker and the resident-session
// eviction worker share the watermark, so a newer observation supersedes an
// older in-flight worker of either kind. Guarded by mqttAccountSessionManager.mu.
type sessionReconcileState struct {
	latest   uint64 // highest observed PubAck sequence for this client
	inFlight uint64 // admitted-but-not-yet-completed workers (both kinds)
	// evictLatest is the eviction worker's TRIGGER: the highest observation
	// delivered while a session was RESIDENT. It is deliberately not `latest` —
	// an observation that arrives with no resident session belongs to the
	// no-local reconcile branch exclusively (which in the remove→re-add window
	// deliberately does nothing), and a lingering eviction worker replaying it
	// against a subsequently re-added object would evict on evidence that was
	// never an eviction trigger (holistic round 3). Guarded by as.mu.
	evictLatest uint64
	// evictRunning coalesces resident-branch observations onto ONE eviction worker
	// per client: while set, an observation only advances evictLatest, and the
	// running worker's terminal cleanup re-checks it under as.mu before clearing
	// the flag, looping instead of exiting when a newer trigger arrived. Checked
	// on the resident branch only, so an observation with no resident session
	// always reaches the no-local reconcile regardless of the flag.
	evictRunning bool
	// evictDecided is the highest trigger a decision actually CONSUMED, and it is
	// what makes a refused eviction recoverable instead of lost. A refusal — a held
	// ownership gate, a busy record fence, a digest deferral, an exhausted retry
	// budget — ends the worker without deciding anything, and clearing the trigger
	// there is what used to discard the observation entirely (docs/CONFORMANCE.md
	// stated the eviction as losable with zero notifications). Only a real decision
	// advances this, so the record-state backstop can re-fire any entry left with
	// evictLatest > evictDecided. Guarded by as.mu.
	evictDecided uint64
}

// pruneReconcileEntryLocked removes a client's reconciliation entry, and it is the
// ONLY thing that may: the entry now outlives its workers whenever a refusal left a
// trigger undecided, so "no worker is running" stopped being a licence to delete it.
//
// Three conjuncts, each answering a way the delete goes wrong:
//
//   - the map must still hold THIS st. A cleanup arriving late holds a pointer that a
//     replacement may already have superseded, and deleting by hash alone would take
//     the replacement's trigger with it — the same identity argument
//     removeSessionIfCurrent exists for.
//   - no session may be resident for the hash. A resident session is exactly who an
//     undecided trigger is about, so pruning under one discards the eviction the
//     backstop was going to re-fire.
//   - inFlight must be zero. A live worker of either kind holds this pointer and
//     will write through it.
//
// as.mu must be held. Shutdown is the one exception and does not come through here:
// it nils the whole map, because nothing may install new state once the process is
// stopping.
func (as *mqttAccountSessionManager) pruneReconcileEntryLocked(cIDHash string, st *sessionReconcileState) {
	if as.reconcileWM == nil || st == nil {
		return
	}
	if cur := as.reconcileWM[cIDHash]; cur != st {
		return
	}
	if _, resident := as.sessByHash[cIDHash]; resident {
		return
	}
	if st.inFlight != 0 {
		return
	}
	delete(as.reconcileWM, cIDHash)
}

type mqttJSAResponse struct {
	reply string // will be used to map to the original request in jsa.NewRequestExMulti
	value any
}

type mqttJSA struct {
	mu      sync.Mutex
	id      string
	c       *client
	sendq   *ipQueue[*mqttJSPubMsg]
	rplyr   string
	replies sync.Map // [string]chan *mqttJSAResponse
	nuid    *nuid.NUID
	quitCh  chan struct{}
	// domain is the JetStream domain from -js-domain, or empty for the domain of
	// the NATS server we are connected to. When set it both prefixes the JS API
	// subjects (see prefixDomain) and is added to the session subject.
	domain  string
	timeout time.Duration

	// subsBarrier lets QoS 1/2 message stores wait out any SUBSCRIBE that is
	// still being processed in this account. See mqttSubsBarrier.
	subsBarrier mqttSubsBarrier
}

// mqttSubsBarrier prevents a PUBLISH from racing a concurrent SUBSCRIBE into
// the JetStream messages stream. $MQTT_msgs uses interest-based retention: a
// message stored while a subscription's JetStream consumer create is still in
// flight finds no interest and is discarded outright by the stream (the
// sequence is burned, nothing is stored), leaving it permanently invisible to
// the new subscription. SUBSCRIBE processing holds the barrier for its
// duration; a QoS 1/2 store calls wait() first, so any SUBSCRIBE the server
// began processing before the publish has its consumer — and therefore stream
// interest — registered by the time the message is stored. MQTT does not
// require delivery to a subscription whose SUBSCRIBE is still in flight, but
// brokers that register subscriptions synchronously do deliver in this
// window, and the Eclipse Paho interop suite relies on it
// (test_request_response, test_subscribe_options). The zero value is ready to
// use.
type mqttSubsBarrier struct {
	mu sync.Mutex
	ch chan struct{} // non-nil while n > 0; closed when n drops back to 0
	n  int
}

func (b *mqttSubsBarrier) enter() {
	b.mu.Lock()
	if b.n == 0 {
		b.ch = make(chan struct{})
	}
	b.n++
	b.mu.Unlock()
}

func (b *mqttSubsBarrier) leave() {
	b.mu.Lock()
	b.n--
	if b.n == 0 {
		close(b.ch)
		b.ch = nil
	}
	b.mu.Unlock()
}

// wait blocks until every SUBSCRIBE in flight when wait sampled the barrier
// has finished processing. SUBSCRIBEs entering afterwards are deliberately
// not waited for: the publish preceded them, so it owes them nothing.
//
// abort (when non-nil) unblocks the wait early on shutdown: a delayed-Will
// publisher worker passes willClaimStopCh (closed in shutdown Phase 2) so it
// cannot stay blocked here past Phase 3's wait. Skipping the barrier only risks a
// racing SUBSCRIBE missing this message, and that subscriber is being drained
// anyway. Normal operation passes an open channel, so behavior is unchanged.
func (b *mqttSubsBarrier) wait(abort <-chan struct{}) {
	b.mu.Lock()
	ch := b.ch
	b.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case <-ch:
	case <-abort:
	}
}

type mqttJSPubMsg struct {
	subj  string
	reply string
	hdr   int
	msg   []byte
}

type mqttSession struct {
	// subsMu is a "quick" version of the session lock, sufficient for the QoS0
	// callback. It only guarantees that a new subscription is initialized, and
	// its retained messages if any have been queued up for delivery. The QoS12
	// callback uses the session lock.
	mu     sync.Mutex
	subsMu sync.RWMutex

	id     string // client ID
	idHash string // client ID hash
	c      *client
	jsa    *mqttJSA
	// asm is the account session manager this session belongs to. Needed by the
	// shared QoS 2 affinity protocol, whose durable state (the assignment claims)
	// is account-scoped and reached through instance-scoped helpers, from paths
	// like clear() that are handed no other account context.
	asm         *mqttAccountSessionManager
	subs        map[string]byte     // Key is MQTT SUBSCRIBE filter, value is the subscription QoS
	noLocalSubs map[string]struct{} // Set of SUBSCRIBE filters that carry the v5 No Local option
	rapSubs     map[string]struct{} // Set of SUBSCRIBE filters that carry the v5 Retain As Published option
	subIDs      map[string]int      // SUBSCRIBE filter -> v5 Subscription Identifier (absent = none)
	// sharedSubs holds MQTT 5.0 Shared Subscriptions, keyed by session key
	// ("$share/{ShareName}/{NATS subject}") so distinct groups (and a plain
	// subscription) on the same subject do not collide. Kept separate from subs
	// so an older server reading the persisted record never subscribes to a
	// "$share/..." key as a literal topic. rapSubs/subIDs are shared with plain
	// subscriptions but keyed by session key (collision-free). Spec5 [4.8.2].
	sharedSubs map[string]byte

	// The record transaction fence. Every write to this session's record — save or
	// delete — happens inside an open transaction, so a writer cannot persist a
	// mutation another transaction may still roll back, and so a writer from a
	// terminated generation cannot recreate the record. recordEpoch is the record
	// generation, bumped by clear(); recordTxID identifies the transaction that is
	// currently open, and recordTxSeqno mints those identities. See
	// sess_recordtx.go.
	recordTxOpen  bool
	recordTxID    uint64
	recordTxSeqno uint64
	recordEpoch   uint64
	recordTxCond  *sync.Cond
	// recordTxWaiters counts the blocking writers that have ANNOUNCED themselves for
	// the fence. Atomic, and deliberately not under mu: a blocking writer announces
	// itself BEFORE contending on mu, which is what makes the announce the
	// linearization point of waiter priority. A non-blocking acquisition linearizes
	// at its successful predicate load under mu (fence free AND count zero), so while
	// an announce remains outstanding no non-blocking caller whose load follows it can
	// acquire — later loads, after the waiter completes and withdraws, succeed
	// normally. At most ONE transaction already past its successful free-fence
	// predicate at the announce — blocking, try, or the inline handoff, since mu
	// serializes them all — may still open in wall time after it; it counts as
	// already admitted at the announce.
	recordTxWaiters atomic.Int64

	// seqUncertain marks a record write whose outcome is UNKNOWN (it may have
	// landed with its response lost), with pendingWrite holding that write's
	// identity so a readback can classify it. While unresolved, save, clear and the
	// record delete refuse at entry, and deferredDisconnect carries the MQTT
	// obligations the close path could not discharge — with their ORIGINAL
	// deadlines, so an outage cannot silently extend the session or its Will Delay.
	// See sess_uncertain.go.
	seqUncertain       bool
	pendingWrite       *mqttPendingWrite
	deferredDisconnect *mqttDeferredDisconnect
	// recordRejectedSeq is the sequence a DEFINITELY REJECTED record CAS was based
	// on, held as OUTSTANDING WORK rather than as a settled fact. The write did not
	// land, so sess.seq is still right and the session stays usable — but the subject
	// MOVED, so this broker's belief that it owns the session may be obsolete, and on
	// an unbound never-expiring session nothing else ever asks again. Zero means none.
	//
	// Guarded by mu; validity is the predicate recordRejectionPendingLocked, not the
	// field alone. See sess_rejected_write.go.
	recordRejectedSeq uint64
	// gen is this session INCARNATION's durable generation. It namespaces every
	// resource whose name would otherwise derive from idHash alone — the PUBREL
	// durable, subject and delivery subject, and the inbound QoS 2 hold subjects —
	// because idHash is reused by a later session of the same client ID and a
	// deterministic name is therefore not an identity. Minted at creation,
	// re-minted by clear(), preserved by a reconnect or resume. Empty means a
	// legacy record written before generations existed: keep the old names. See
	// sess_incarnation.go.
	gen string
	// wid / pwid are the identity of the record write this broker was last
	// ACKNOWLEDGED for, and that record's own parent. Every save stamps a fresh wid
	// with pwid set to this one, so the durable chain is a parent pointer and two
	// writes off one base are recognisable as siblings. Adopted wherever seq is —
	// a confirmed save, an ambiguity resolved landed, a repair save proven durable —
	// because a lineage that lags the sequence would parent the next write to a
	// record that is no longer the base. See sess_fork_lineage.go.
	wid, pwid string
	// regClaim is this session OBJECT's ownership identity for shared-delivery
	// in-flight registers. Unlike gen it is deliberately not persisted: a resume on
	// another broker constructs a new object and therefore takes a new claim, while a
	// same-broker rebind keeps the object and its claim. See inflight_register.go.
	regClaim string

	cons map[string]*ConsumerConfig
	// drains holds consumers that survived an UNSUBSCRIBE with started deliveries
	// still pending, keyed by sid; drainByDur is the same set keyed by durable, for
	// the delivery callback. A drained consumer lives EXCLUSIVELY here — its
	// cons[sid] entry is gone — so nothing can reach it through cons and create a
	// second durable on the same filter (I7). See unsub_drain.go.
	drains     map[string]*mqttDrain
	drainByDur map[string]*mqttDrain
	// sharedCharged tracks the MaxAckPending this session has added to tmaxack
	// for each shared group durable it joined (durable -> amount), so a release
	// subtracts exactly what was charged and only once. In-memory only. Spec5 [4.8.2].
	sharedCharged map[string]int
	// consCharged is the same accounting for PLAIN durables. A consumer restored
	// from the session record is never charged (only the create path charges), so
	// refunding its config's MaxAckPending on teardown drove tmaxack negative after
	// a resume and inflated the account's remaining budget. In-memory only.
	consCharged    map[string]int
	pubRelConsumer *ConsumerConfig
	// pubRelVerified is the PUBREL path's equivalent of q2AffReady: this connection
	// has PROVEN the durable exists, rather than trusting a config restored from the
	// record. Every restore starts it false. See
	// ensurePubRelConsumerSubscription.
	pubRelVerified bool
	// uncertainLanded holds what callers deferred for want of proof that a record
	// write landed, for mutations retained because it MAY have. Run by the resolver
	// once it proves those exact bytes durable. See retainOnLanded.
	uncertainLanded []func(recordTx)

	pubRelSubscribed       bool
	pubRelDeliverySubject  string
	pubRelDeliverySubjectB []byte
	pubRelSubject          string
	seq                    uint64

	// Session-affine delivery of started shared-subscription QoS 2 messages.
	// q2AffGen is this session incarnation's durable generation: it namespaces the
	// copy subject, the consumer durable and the deliver subject, because idHash
	// alone is reused by a later clean session of the same client ID. Minted at
	// first provisioning, persisted, restored, and dropped by clear().
	//
	// q2AffReady is the ONLY thing a move may consult. It means "the consumer and
	// its delivery subscription exist AND the session record naming them is
	// saved" — a non-nil config proves neither, and $MQTT_out is interest-retained,
	// so a copy stored before the consumer exists is silently discarded by a store
	// that returns success. Every restore starts it false. See
	// ensureQ2AffineConsumerSubscription and qos2_shared_affine.go.
	q2AffGen              string
	q2AffConsumer         *ConsumerConfig
	q2AffSubscribed       bool
	q2AffDeliverySubject  string
	q2AffDeliverySubjectB []byte
	q2AffReady            bool
	// q2AffClosing / q2AffInFlight / q2AffCond are the move-operation lifecycle
	// fence. A move captures readiness under mu, releases it, then performs
	// JetStream operations, so a delayed claim CAS could otherwise land AFTER
	// clear() finished — creating a claim for a terminated generation that stalls
	// the group delivery forever, since contenders wait on a `claimed` record.
	// clear() sets q2AffClosing (no new move may start), then waits on q2AffCond
	// until q2AffInFlight drains, and only then scans and terminates the claims.
	q2AffClosing  bool
	q2AffInFlight int
	q2AffCond     *sync.Cond

	// qos2RelClosing / qos2RelInFlight / qos2RelCond are the same fence for the
	// INBOUND QoS 2 release (qos2_release_claim.go), and exist for the same reason:
	// a PUBREL claims the release and then performs JetStream round trips, so
	// without this a release admitted before clear() could still fan out after the
	// discard finished and its CONNACK reported Session Present false. The durable
	// claim arbitrates that across brokers; this closes it outright on THIS one,
	// where it can be closed. clear() sets qos2RelClosing (no new release may start)
	// and waits on qos2RelCond until qos2RelInFlight drains, before it scans.
	qos2RelClosing  bool
	qos2RelInFlight int
	qos2RelCond     *sync.Cond

	// pendingPublish maps packet identifiers (PI) to JetStream ACK subjects for
	// QoS1 and 2 PUBLISH messages pending delivery to the session's client.
	pendingPublish map[uint16]*mqttPending

	// pendingPubRel maps PIs to JetStream ACK subjects for QoS2 PUBREL
	// messages pending delivery to the session's client.
	pendingPubRel map[uint16]*mqttPending

	// cpending maps delivery attempts (that come with a JS ACK subject) to
	// existing PIs.
	cpending map[string]map[uint64]uint16 // composite key: jsDur, sseq

	// srcAckOwed names QoS 2 source deliveries that are past PUBREC but whose
	// acknowledgement could not be proven. Their payload must never be sent again — the
	// client already answered it — so the next delivery attempt of each is acknowledged
	// instead of delivered, and the entry is released once that acknowledgement is
	// confirmed. Keyed by the SOURCE rather than the packet identifier on purpose: the
	// guard in trackPublish tests pendingPubRel, which PUBCOMP removes, so an
	// identifier-keyed suppression stops working exactly when the exchange completes.
	// Created on first use; bounded by unproven acknowledgements, not by traffic. See
	// retireSourceLocked.
	srcAckOwed map[mqttDeferredKey]*mqttRetiredSource

	// cancelledRegNonces holds the nonces of registers this session has given up — the
	// VOID-register verdicts. A teardown that abandons a register voids its nonce, whether
	// the allocation was still storing or already transmitted; recovery reads a register
	// carrying one of these exactly as it reads a cancellation tombstone: never owed to the
	// client, so its identifier and Receive Maximum slot are released rather than held. A
	// nonce is minted with its allocation and never reused, which is what a subject, a
	// packet identifier or a (deterministically named) shared group durable could not
	// offer. Guarded by mu, and persisted as mqttPersistedSession.CancelledRegs. See
	// voidRegNonceLocked and voidRegistrationsLocked.
	// The VALUE is the voidedRegsRev at which the nonce was recorded, and it is what a
	// landed save may claim: a save whose captured revision is r provably carried every
	// nonce recorded at or before r, and none recorded after — so a release keyed on
	// membership alone would free a reservation whose verdict a CONCURRENT save never
	// serialized. See releaseSettledVoidReservationsLocked.
	cancelledRegNonces map[string]uint64

	// settledVoidReservations indexes, by nonce, the cancelled reservations whose store
	// has SETTLED but whose verdict is not yet proven — the only reservations a later
	// proof edge (a landed save, a stream-rung retirement) still has to release. An index
	// rather than a pendingPublish walk, because retirement runs once per voided register
	// in a teardown and pendingPublish can legitimately hold tens of thousands of
	// entries: walking it per nonce is quadratic teardown work. Entries are rare — only a
	// cancellation whose every durable rung failed at its settle point parks here.
	// Guarded by mu. See voidVerdictProvenLocked and releaseSettledVoidReservationsLocked.
	settledVoidReservations map[string]uint16

	// voidedRegsRev counts changes to cancelledRegNonces, and voidedRegsSavedRev is the
	// highest revision PROVEN to be in the session record. They are not a bool because the
	// writer cannot see the race it loses: an off-readLoop teardown appends while a
	// readLoop transaction that has already marshalled its record holds the fence, so that
	// transaction commits without the entry and the append was refused its own save.
	// Driving persistence off the gap between the two — rather than off "did I just change
	// something" — is what makes every convergence point retry until the record agrees.
	// Both guarded by mu. See persistVoidedRegs.
	voidedRegsRev      uint64
	voidedRegsSavedRev uint64

	// voidHandoffActive says a release-triggered persistence LOOP is running for this
	// session (attemptVoidVerdictHandoff). It is what keeps successive late stagings on one
	// frame: the loop's own fence and gate releases would otherwise re-enter the handoff
	// decision inside their own frames, recursing once per staged revision with a JetStream
	// round trip held in each. Cleared only in the same sess.mu critical section that finds
	// nothing left pending, so no staging can slip between the last check and the exit.
	// Guarded by mu. See voidVerdictHandoffDueLocked.
	voidHandoffActive bool

	// voidedRegsHandoffRev is the revision the last release-triggered persistence
	// ATTEMPT acquired the record fence at — the handoff watermark, and the reason the
	// handoff makes one attempt per newly staged revision instead of a retry storm or a
	// synchronous recursion without a bound. It advances only on acquisition, never on
	// the decision to try: a trigger that lost the fence stored nothing, and burning the
	// watermark on it would strand the nonce it was firing for. Generation-local, so it
	// is reset wherever voidedRegsRev is. Guarded by mu. See
	// voidVerdictHandoffDueLocked.
	voidedRegsHandoffRev uint64

	// piSpaceIncomplete is set when a resume could not fully reconstruct the durable
	// outbound identifier space, so some identifier the client holds may be unknown
	// here. While it is set bumpPI allocates NOTHING: a new identifier could collide
	// with a register or an unreplayed PUBREL that still owns it. Nothing else is
	// restricted, and a later resume that completes both scans clears it. See
	// inflight_register.go (R3).
	piSpaceIncomplete bool

	// deferred tracks QoS 1/2 deliveries withheld by trackPublish because the
	// session was at its effective in-flight cap (Receive Maximum /
	// MaxAckPending). Keyed by JS durable + stream sequence; the value is the
	// latest delivery attempt's JS ack subject, NAK'd to force prompt
	// redelivery when the client frees a slot (Spec5 [4.9]). In-memory only,
	// naturally bounded by the consumers' MaxAckPending.
	deferred map[mqttDeferredKey]string

	// "Last used" publish packet identifier (PI). starting point searching for the next available.
	last_pi uint16

	// Maximum number of pending acks for this session.
	maxp uint16
	// rmax is the current connection's MQTT 5.0 Receive Maximum: the max number
	// of unacknowledged QoS1/2 PUBLISH the server may have in flight to the
	// client. 0 means no client-imposed limit (3.1.1, or not sent). The
	// effective in-flight cap is min(maxp, rmax). Set on each CONNECT.
	rmax    uint16
	tmaxack int
	clean   bool
	// expiryInterval is the effective MQTT 5.0 Session Expiry Interval in seconds:
	// 0 means the session ends when the connection closes, mqttSessionNeverExpire
	// means it never expires, any other value keeps the session for that many
	// seconds after the connection closes. For 3.1.1 this is derived from the
	// Clean Session flag (clean => 0, persistent => mqttSessionNeverExpire). Set on
	// each CONNECT and possibly overridden by a v5 DISCONNECT. Spec5 [3.1.2.11.2].
	expiryInterval uint32
	// disconnectedAt is the Unix time the client disconnected (0 while connected).
	// Persisted so a finite deadline survives a restart — but it is stamped for a
	// NEVER-EXPIRING session too, because the delayed-Will record fence reads a newer
	// record with DisconnectedAt == 0 as a live newer connection and would cancel a Will
	// that session is owed. It is inert for every expiry reader: each pre-filters on
	// mqttSessionNeverExpire before looking at it. See sessionRecordFence.
	disconnectedAt int64
	domainTk       string

	// will is the session's pending delayed Will, persisted with the record so
	// it survives a server restart during the Will Delay Interval. nil when no
	// Will is pending. Spec5 [3.1.3.2.2].
	will *mqttPersistedWill
}

type mqttPersistedSession struct {
	Origin string          `json:"origin,omitempty"`
	ID     string          `json:"id,omitempty"`
	Clean  bool            `json:"clean,omitempty"`
	Subs   map[string]byte `json:"subs,omitempty"`
	// NoLocal and RetainAsPublished are the sets of Subs filters that carry the
	// v5 No Local / Retain As Published options; SubIDs maps a Subs filter to its
	// v5 Subscription Identifier. All kept separate from Subs (rather than packed
	// into its QoS byte) so an older server reading this record still sees a valid
	// 0..2 QoS and simply ignores these fields. Spec5 [3.8.3.1], [3.8.2.1.2].
	NoLocal           []string       `json:"no_local,omitempty"`
	RetainAsPublished []string       `json:"retain_as_published,omitempty"`
	SubIDs            map[string]int `json:"sub_ids,omitempty"`
	// SharedSubs is the sole persisted authority for MQTT 5.0 Shared
	// Subscriptions (session key -> QoS). The group consumer's durable, deliver
	// subject, and queue name are all derived deterministically from each key
	// (see mqttSharedNames), so no consumer config is persisted for them — and
	// they are deliberately excluded from Cons, since an older server would
	// otherwise delete the group durable in clear(). Spec5 [4.8.2].
	SharedSubs map[string]byte            `json:"shared_subs,omitempty"`
	Cons       map[string]*ConsumerConfig `json:"cons,omitempty"`
	PubRel     *ConsumerConfig            `json:"pubrel,omitempty"`
	// Q2Aff / Q2AffGen are the session-affine QoS 2 delivery path for shared
	// subscriptions: the consumer that delivers copies of started deliveries this
	// session owns, and the generation that namespaces all three of its names. An
	// older server ignores both, which leaks the consumer but can never expose one
	// session's in-flight message to another (the generation is in every name).
	// Spec5 [4.8.2]; see qos2_shared_affine.go.
	Q2Aff    *ConsumerConfig `json:"q2aff,omitempty"`
	Q2AffGen string          `json:"q2affgen,omitempty"`
	// Gen is this session incarnation's generation, which namespaces every
	// resource named after idHash alone (the PUBREL path, the inbound QoS 2 hold
	// subjects). Absent means a record written before generations existed: the
	// legacy hash-only names apply. An older broker ignores the field and keeps
	// using those names, so the two do not see each other's PUBRELs — the same
	// mixed-version rule as Q2AffGen. See sess_incarnation.go.
	Gen string `json:"gen,omitempty"`
	// WID is this record write's own identity, minted once per write; PWID is the
	// WID of the record the write was derived from (empty only for a first create).
	// The pair is a PARENT POINTER, and it exists because a per-subject CAS is a
	// linearization point only WITHIN a leader term: across a transition the
	// pre-proposal check runs against stale applied state with the in-process map
	// cleared, so two writes derived from ONE base can both apply and the later
	// discards the earlier under MaxMsgsPer:1. Two records carrying the same PWID
	// are therefore SIBLINGS of one base, and a survivor that is provably our
	// sibling is not evidence that anybody took the session over.
	//
	// RECOGNITION ONLY: nothing repairs the erased write, and an absent WID simply
	// concedes exactly as every record written before this field did. An older
	// broker ignores both fields and drops them on its next write, which limits
	// detection rather than disabling it. See sess_fork_lineage.go.
	WID  string `json:"wid,omitempty"`
	PWID string `json:"pwid,omitempty"`
	// Drains are the consumers that survived an UNSUBSCRIBE because deliveries they
	// had already STARTED must still be completed ([MQTT-3.10.4-3]). Keyed by the
	// same sid as Cons, so a "foo/#" filter's level-up companion drains
	// independently. Deliberately kept OUT of Cons: an older broker restoring one
	// there would find cons[sid] on a re-SUBSCRIBE, take the fromSubProto early
	// return and never create a delivery subscription. See unsub_drain.go.
	Drains map[string]*mqttPersistedDrain `json:"drains,omitempty"`

	// CancelledRegs are the NONCES of registers this session has given up — the
	// void-register verdicts. Written by the transaction that abandons them (a teardown's
	// membership-removal write, or a later convergence-point save for a verdict recorded
	// after that write), and read by recovery, which frees the identifier such a register
	// names instead of reserving it for the life of the session.
	//
	// Keyed by nonce, never by packet identifier, subject or durable: a teardown frees the
	// identifier for immediate reuse, subjects are reused within a generation, and shared
	// group durables are named deterministically so the same name returns on a rejoin — a
	// verdict scoped by any of those would eventually condemn a genuine later delivery. A
	// nonce is minted with the allocation and never reused. An entry is removed when its
	// register's deletion is proven, or by a complete resume scan that finds no register
	// carrying it. See inflight_register.go.
	CancelledRegs []string `json:"cancelled_regs,omitempty"`
	// Teardown, when present, means a broker has CLAIMED this record's teardown and
	// may be part way through deleting the incarnation's resources. Nobody may
	// resume that state: every reader that can INTERPRET the record finishes the
	// teardown (the deletions are idempotent and generation-scoped) and releases the
	// record before starting fresh. A reader that refuses the record's schema does not
	// resume it either, but it does not finish it: the expiry path and the orphan
	// sweep pick a reserved-namespace record up only while its GENERATION is valid,
	// and one without a valid generation is adopted by nothing until the streams are
	// recreated (docs/ARCHITECTURE.md, teardown section). The record keeps its full
	// content under the marker precisely so any broker can finish the job, and so the
	// orphan sweep still reads Cons / SharedSubs / Drains as referenced.
	// See sess_teardown.go.
	Teardown *mqttSessTeardown  `json:"teardown,omitempty"`
	Will     *mqttPersistedWill `json:"will,omitempty"`
	// Session Expiry Interval (seconds) and disconnect Unix time (0 while
	// connected): together the expiry deadline, restart-safe. Spec5 [3.1.2.11.2].
	ExpiryInterval uint32 `json:"expiry,omitempty"`
	DisconnectedAt int64  `json:"disconnected,omitempty"`
}

// mqttPersistedWill is a delayed Will stored in the session record so it still
// fires if the server restarts during its Will Delay Interval. Publisher
// permissions are not serializable, so the Will's publish permission is
// checked before persisting and a denied Will is never stored. Spec5
// [3.1.3.2.2].
type mqttPersistedWill struct {
	Topic  string `json:"topic"`
	Msg    []byte `json:"msg,omitempty"`
	QoS    byte   `json:"qos,omitempty"`
	Retain bool   `json:"retain,omitempty"`
	Props  []byte `json:"props,omitempty"` // encoded forwardable properties block
	User   string `json:"user,omitempty"`
	FireAt int64  `json:"fire,omitempty"` // Unix nanoseconds to publish at (truncation could fire early)
}

// mqttRetainedMsg is stored and read back through the NATS header format that
// mqttEncodeRetainedMessage writes, never as a marshalled struct — so the fields
// carry no serialization tags.
type mqttRetainedMsg struct {
	Origin  string
	Subject string
	Topic   string
	Msg     []byte
	Flags   byte
	Source  string
	// Props is the raw MQTT 5.0 properties block captured from the retained
	// PUBLISH, forwarded to v5 subscribers when the message is replayed. Empty
	// for messages published by 3.1.1 clients or without properties.
	Props []byte

	// SessionHash is the authenticated No Local origin marker (HMAC of the
	// publishing session hash, subject, and payload under a per-server secret)
	// for the session that published this retained message, so No Local can
	// suppress replaying it to that same session without trusting a forgeable
	// header. Empty when v5 is disabled or there is no publishing session. Spec5
	// [3.8.3.1].
	SessionHash string

	// expires is the absolute deadline after which this retained message must no
	// longer be delivered, when it was published with an MQTT 5.0 Message Expiry
	// Interval. Zero when the message has no expiry. Persisted with the stored
	// message via the mqttNatsRetainedMessageExpiry header (not JSON). Spec5
	// [3.3.2.3.3].
	expires time.Time

	expiresFromCache time.Time
}

type mqttRetainedMsgRef struct {
	sseq uint64
}

// mqttSub contains fields associated with a MQTT subscription, and is added to
// the main subscription struct for MQTT message delivery subscriptions. The
// delivery callbacks may get invoked before sub.mqtt is set up, so they should
// acquire either sess.mu or sess.subsMu before accessing it.
type mqttSub struct {
	// The sub's QOS and the JS durable name. They can change when
	// re-subscribing, and are used in the delivery callbacks. They can be
	// quickly accessed using sess.subsMu.RLock, or under the main session lock.
	qos   byte
	jsDur string

	// shared marks this subscription as belonging to an MQTT 5.0 Shared
	// Subscription group. Read in the delivery callbacks like qos. When set on a
	// QoS0 (raw-subject) sub, the callback must not deliver stored (QoS 1/2
	// published) messages — those arrive through the shared JS consumer instead.
	// Spec5 [4.8.2].
	shared bool

	// closed marks the subscription as torn down (QoS downgrade to 0, or
	// unsubscribe) so QoS 1/2 delivery callbacks stop tracking new messages for
	// it. Guarded like qos/jsDur (sess.mu or sess.subsMu). Unlike clearing
	// sub.mqtt, this keeps the struct valid for an in-flight enqueue.
	closed bool

	// phaseOwned marks a delivery subscription installed CLOSED by the resume
	// reconciliation phase and not yet adopted by subscription restoration.
	// Restoration adopts it IN PLACE (clearing closed and this marker together,
	// so interest never lapses); a marker still standing after processSubs means
	// restoration skipped the filter and the sweep must reclaim the
	// subscription, or it would hold consumer interest forever while delivering
	// nothing. Guarded like closed. See sess_resume_reconcile.go.
	phaseOwned bool

	// phaseGate is closed when the reconciliation phase releases this
	// subscription — adopted by restoration, or reclaimed by the sweep/undo. A
	// QoS 1/2 delivery that arrives while it is still open WAITS for it rather
	// than being skipped: the interest is installed before the CONNACK, so
	// JetStream pushes the session's whole offline backlog into that window, and
	// skipping it would hold every one of those messages until AckWait (30s by
	// default) — turning an immediate resume delivery into a half-minute stall —
	// and then re-send them with DUP set, on their FIRST transmission to the
	// client. Waiting keeps them first deliveries, in order, at adoption speed.
	// Bounded by mqttPhaseGateWait so a CONNECT that dies without releasing
	// cannot park a delivery goroutine; past it the delivery falls back to the
	// closed-subscription skip and AckWait. nil once released (and on every
	// subscription the phase did not install). Guarded like closed.
	phaseGate chan struct{}

	// Pending serialization of retained messages to be sent when subscription
	// is registered. The sub's delivery callbacks must wait until `prm` is
	// ready (can block on sess.mu for that, too).
	prm [][]byte

	// If this subscription needs to be checked for being reserved. E.g. '#' or
	// '*' or '*/'.  It is set up at the time of subscription and is immutable
	// after that.
	reserved bool

	// noLocal is the v5 No Local subscription option: when set, a message
	// published by this same session's connection must not be delivered to this
	// subscription. Read in the delivery callbacks under sess.subsMu/sess.mu,
	// like qos. Spec5 [3.8.3.1].
	noLocal bool

	// retainAsPublished is the v5 Retain As Published subscription option: when
	// set, a live-forwarded message keeps the RETAIN flag it was published with
	// instead of having it cleared. Read in the delivery callbacks like noLocal.
	// Spec5 [3.8.3.1].
	retainAsPublished bool

	// subID is the v5 Subscription Identifier for this subscription (0 = none).
	// The server injects it into every PUBLISH delivered because of this sub.
	// Read in the delivery callbacks like noLocal. Spec5 [3.8.2.1.2].
	subID int
}

// mqttWillDisposition is what is owed to one connection's Will Message. (Not to
// be confused with mqttWillState, the durable state of an outbox record.)
//
// A Will starts armed: the connection is up, nothing is owed yet, and a
// same-client-ID CONNECT taking the session over discards it (a takeover is not
// one of the events that publishes a Will, and it is the takeover itself that
// closes the old connection). Observing the connection close is what makes the
// Will owed, and an owed immediate Will can no longer be cancelled by a
// reconnect: the Will Delay Interval it would have to be cancelled within has
// already elapsed. Spec5 [3.1.2.5], [3.14.4], [MQTT-3.1.2-8].
type mqttWillDisposition uint8

const (
	// mqttWillArmed is a Will on a live connection: publishable if the connection
	// drops, discardable by a normal DISCONNECT or by a takeover.
	mqttWillArmed mqttWillDisposition = iota
	// mqttWillOwedNow is a Will whose connection has closed with no Will Delay
	// Interval left to run. It must be published; a reconnect cannot cancel it.
	mqttWillOwedNow
	// mqttWillOwedDelayed is a Will whose connection has closed but whose Will
	// Delay Interval is still running. It is enqueued on the durable outbox by the
	// normal teardown, and a reconnect within the interval still cancels it.
	mqttWillOwedDelayed
	// mqttWillDischarged is terminal: the Will has been published, enqueued or
	// discarded, and must not be acted on again.
	mqttWillDischarged
)

// mqttWillIsDelayed reports whether a Will's publication has to wait out a Will
// Delay Interval instead of happening when the connection closes. The interval
// only defers a Will while the session outlives the connection: a zero Session
// Expiry Interval ends the session at close, and the session ending is itself a
// publication trigger. Spec5 [3.1.3.2.2], [MQTT-3.1.2-8].
func mqttWillIsDelayed(proto byte, will *mqttWill, sessionExpiry uint32) bool {
	return proto == mqttProtoLevel5 && will != nil && will.props != nil &&
		will.props.willDelay > 0 && sessionExpiry > 0
}

// mqttSessionExpiry reads THIS connection's effective Session Expiry Interval —
// deliberately its own copy and not the session's, which a replacement
// connection may already have overwritten (see mqtt.sessionExpiry). 0 for a
// connection that never completed CONNECT, which also means "the session ends
// when this connection closes", so its Will is due immediately: the right
// default.
//
// No lock held on entry.
func (c *client) mqttSessionExpiry() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mqtt == nil {
		return 0
	}
	return c.mqtt.sessionExpiry
}

// mqttOweWill records that this connection's close has been observed, moving an
// armed Will to owed. sessionExpiry is the session's effective Session Expiry
// Interval, read by the caller while the session is still unambiguously this
// connection's; it decides whether the Will is due now or after its delay.
//
// A no-op unless the Will is still armed, which is what keeps a
// takeover-initiated close silent: the takeover discharged the Will before
// closing the connection it displaces.
//
// No lock held on entry.
func (c *client) mqttOweWill(sessionExpiry uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mqtt == nil || c.mqtt.cp == nil || c.mqtt.cp.will == nil ||
		c.mqtt.willDisposition != mqttWillArmed {
		return
	}
	if mqttWillIsDelayed(c.mqtt.proto, c.mqtt.cp.will, sessionExpiry) {
		c.mqtt.willDisposition = mqttWillOwedDelayed
	} else {
		c.mqtt.willDisposition = mqttWillOwedNow
	}
}

// mqttDiscardWill drops this connection's Will without publishing it. A normal
// DISCONNECT does this. Spec [MQTT-3.1.2-8].
//
// No lock held on entry.
func (c *client) mqttDiscardWill() {
	c.mu.Lock()
	c.mqttDiscardWillLocked()
	c.mu.Unlock()
}

// Lock held on entry.
func (c *client) mqttDiscardWillLocked() {
	if c.mqtt == nil || c.mqtt.cp == nil {
		return
	}
	c.mqtt.cp.will = nil
	c.mqtt.willDisposition = mqttWillDischarged
}

// mqttDiscardWillOnTakeover drops the Will of a connection being displaced by a
// new one with the same client ID. A takeover is not a Will publication trigger,
// and it is the takeover that closes this connection, so an armed Will goes away
// with it. A Will already owed immediately is not the takeover's to cancel: that
// connection closed first, and the Will Delay Interval a reconnect would have to
// cancel it within has already run out. A delayed one still is. Spec5 [3.1.2.5].
//
// No lock held on entry.
func (c *client) mqttDiscardWillOnTakeover() {
	c.mu.Lock()
	if c.mqtt != nil && c.mqtt.willDisposition != mqttWillOwedNow {
		c.mqttDiscardWillLocked()
	}
	c.mu.Unlock()
}

// mqttTakeWill consumes this connection's Will, returning it together with the
// disposition it was consumed in; a nil Will means there is nothing left to do.
//
// Every path that discharges a Will goes through here, because more than one
// runs: mqttHandleClosedClient is invoked twice for a graceful DISCONNECT (once
// from the packet, once when the socket closes), the deferred-disconnect capture
// takes over responsibility for an unresolved session, and a takeover races all
// of them. "Has this already been discharged?" can only be answered by the same
// transition that hands the Will over.
//
// No lock held on entry.
func (c *client) mqttTakeWill() (*mqttWill, mqttWillDisposition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mqttTakeWillLocked()
}

// Lock held on entry.
func (c *client) mqttTakeWillLocked() (*mqttWill, mqttWillDisposition) {
	if c.mqtt == nil || c.mqtt.cp == nil || c.mqtt.cp.will == nil {
		return nil, mqttWillDischarged
	}
	will, disposition := c.mqtt.cp.will, c.mqtt.willDisposition
	c.mqtt.cp.will = nil
	c.mqtt.willDisposition = mqttWillDischarged
	return will, disposition
}

type mqtt struct {
	r    *mqttReader
	cp   *mqttConnectProto
	pp   *mqttPublish
	asm  *mqttAccountSessionManager // quick reference to account session manager, immutable after processConnect()
	sess *mqttSession               // quick reference to session, immutable after processConnect()
	cid  string                     // client ID

	// proto is the negotiated protocol level: mqttProtoLevel (3.1.1) or
	// mqttProtoLevel5 (5.0). Set during CONNECT parsing and immutable after.
	// It outlives individual packets so that ACK/DISCONNECT handlers running
	// long after CONNECT can select the proper wire framing.
	proto byte

	// sessionExpiry is the effective Session Expiry Interval THIS connection
	// negotiated (a v5 DISCONNECT may update it). It duplicates the session's own
	// field on purpose: the session's belongs to whichever connection currently
	// holds it, and a same-client-ID CONNECT overwrites it at the moment it
	// rebinds — before the connection it displaces has finished closing. Deciding
	// a displaced connection's Will delay from the replacement's interval gets it
	// backwards in both directions (publishing a delayed Will the reconnect should
	// have cancelled, or discarding an immediate one). Set under c.mu at CONNECT,
	// read by mqttSessionExpiry. Spec5 [3.1.2.11.2].
	sessionExpiry uint32

	// willDisposition tracks what is owed to this connection's Will. It exists
	// because two goroutines decide the Will's fate concurrently — this
	// connection's teardown and a same-client-ID CONNECT taking the session over —
	// and the answer depends on which of them observed the close first, which the
	// Will pointer alone cannot express. Transitioned only under c.mu; consumed
	// exactly once by mqttTakeWill. See mqttHandleClosedClient, mqttHandleWill.
	willDisposition mqttWillDisposition

	// disconnectSent records that a server-initiated DISCONNECT has been
	// ATTEMPTED on this connection. After sending a DISCONNECT the sender must
	// not send any more MQTT Control Packets on that Network Connection
	// [MQTT-3.14.4-1], so the first attempt consumes the connection's one
	// notification and every later would-be sender stays silent — whichever of
	// the four displacement paths, the parse-error footer or the shutdown drain
	// it is. It exists because nothing else owns "displace this connection":
	// close is idempotent (c.closed) and the Will's fate is consumed exactly
	// once (willDisposition), but every notification site used to write
	// unconditionally, so two racing paths put two DISCONNECTs on one socket.
	// Consumed only through mqttClaimDisconnectLocked, under c.mu — for every
	// enqueue-path writer in the same critical section as the write it
	// authorizes; shutdownDisconnect is the exception, reserving the claim under
	// the lock and attempting its writeDirect after releasing it. The guarantee
	// is at-most-one ATTEMPT: the winner's write stays best-effort (a concurrent
	// close can fail it, and the shutdown drain can spend the claim and then
	// write nothing at all), never exactly-once delivery.
	disconnectSent bool

	// pubOrigin is the authenticated No Local origin marker (or empty) resolved
	// for the message currently being delivered. Set by
	// mqttInitiateMsgDeliveryJSA before onward processing so mqttHandlePubRetain
	// stamps a retained copy with the same value the live delivery used — in
	// particular empty for a server-originated Will, whatever path delivers it.
	// Valid only for the duration of that call. Spec5 [3.8.3.1].
	pubOrigin string

	// maxPacketSize is the current connection's MQTT 5.0 Maximum Packet Size:
	// the largest PUBLISH (whole control packet) the client is willing to
	// accept. 0 means no client-imposed limit (3.1.1, or not sent). Set at
	// CONNECT parse time (so even a rejection CONNACK honors it) and read
	// lock-free on the delivery and ack paths like proto. Unlike the session's
	// Receive Maximum (rmax), this lives on the per-connection client: it must
	// gate the QoS0 direct-delivery path, which runs without the session lock.
	// Spec5 [3.1.2.11.4].
	maxPacketSize uint32

	// maxInPacketSize is the server's MQTT 5.0 Maximum Packet Size advertised in
	// this connection's success CONNACK and enforced on inbound packets in the
	// read loop; a larger packet gets a DISCONNECT 0x95. 0 means nothing is
	// advertised or enforced (3.1.1, or no configured limit). Captured once in
	// mqttProcessConnect so the encoded value and the enforced value are always
	// identical even if a config reload changes c.mpay. Read lock-free on the
	// read-loop goroutine like proto. Spec5 [3.2.2.3.6], [MQTT-3.2.2-15].
	maxInPacketSize uint32

	// noProblemInfo is true when the v5 client sent Request Problem Information 0:
	// the server must then not attach a Reason String (or User Property) to any
	// packet other than CONNACK/DISCONNECT. Absent means 1 (allowed). Set at
	// CONNECT parse time, read lock-free on the ack paths like proto. Spec5
	// [3.1.2.11.7].
	noProblemInfo bool

	// cidGenerated is true when the client sent an empty client ID and the
	// server assigned one. For v5 it must be echoed back in the CONNACK as the
	// Assigned Client Identifier property. Spec5 [3.2.2.3.7].
	cidGenerated bool

	// topicAliasMax is the inbound MQTT 5.0 Topic Alias Maximum this server
	// accepts from this connection, resolved from Options at CONNECT and
	// advertised in the CONNACK. 0 means aliases are not accepted (property
	// omitted, absent => 0). Set during CONNECT, read lock-free on the parse path
	// like proto. The outbound direction (aliases the server sends) is
	// outTopicAliasMax. Spec5 [3.2.2.3.2], [3.3.2.3.4].
	topicAliasMax uint16

	// topicAliases maps a client-assigned Topic Alias to the (copied) pre-mapping
	// wire topic it was last bound to. Lazily allocated on first bind, bounded by
	// topicAliasMax entries. Aliases are connection-scoped and never survive a
	// reconnect or session takeover (c.mqtt is rebuilt per connection). Only
	// accessed from the read loop. Spec5 [3.3.2.3.4].
	topicAliases map[uint16][]byte

	// outTopicAliasMax is how many MQTT 5.0 Topic Aliases the server may use on
	// PUBLISH packets it sends TO this connection: the client's CONNECT Topic
	// Alias Maximum bounded by the server-side cap (the min of the two, so a
	// client advertising 65535 cannot force the server to hold 65535 topics).
	// 0 => the server sends no aliases. Set once during CONNECT, read lock-free
	// on delivery goroutines like maxPacketSize. Spec5 [3.1.2.11.2], [3.3.2.3.4].
	outTopicAliasMax uint16

	// outTopicAliases maps a full MQTT topic to the outbound alias the server
	// bound it to on this connection. Guarded by the client lock (c.mu): deliveries
	// arrive on multiple publisher goroutines, and the alias assignment must be
	// atomic with queueOutbound so the binding PUBLISH (full topic + alias) is
	// always buffered before any alias-only PUBLISH referencing it. Lazily
	// allocated on first bind; first-come-first-served, values 1..outTopicAliasMax,
	// no eviction (a topic beyond the table is sent unaliased). Connection-scoped:
	// aliases never survive a reconnect or session takeover. Spec5 [3.3.2.3.4].
	outTopicAliases map[string]uint16

	// serverKeepAlive is the keep alive (seconds) this server enforces when it
	// overrides a v5 client's requested value; advertised in the CONNACK Server
	// Keep Alive property. 0 = no override (property omitted). Set during
	// CONNECT, read on the CONNACK path only. Spec5 [3.2.2.3.14].
	serverKeepAlive uint16

	// responseInfo is the CONNACK Response Information string to return to this
	// client. Set during CONNECT only when the client's Request Response
	// Information is 1 and the server is configured with a base response topic;
	// empty otherwise. Read on the CONNACK path only. Spec5 [3.2.2.3.15].
	responseInfo string

	// recvMaxIn is the server's Receive Maximum advertised in this connection's
	// success CONNACK and enforced on inbound QoS 1/2 PUBLISH. 0 => not advertised
	// or enforced (3.1.1, or disabled). Resolved once at CONNECT (like
	// topicAliasMax) so the advertised and enforced values match across a reload.
	// Read loop only. Spec5 [3.2.2.3.3], [4.9].
	recvMaxIn uint16

	// qos2InFlight holds the inbound QoS 2 packet identifiers between PUBREC and
	// PUBREL: the portion of the client's consumed send quota the server can
	// durably observe, checked against recvMaxIn [MQTT-4.9]. QoS 1 is not
	// tracked: its quota slot is returned when the client receives the PUBACK,
	// which the server cannot observe (no post-PUBACK signal flows back), so pure
	// QoS 1 overrun is left to the client's own compliance (and a flood to the
	// slow-consumer limits); a QoS 1 PUBLISH is still rejected when held QoS 2
	// messages already fill the quota. Lazily allocated, bounded by recvMaxIn,
	// connection-scoped (c.mqtt is rebuilt per connection => a fresh quota each
	// network connection). Read loop only.
	qos2InFlight map[uint16]struct{}

	// rejectQoS2Pub tells the MQTT client to not accept QoS2 PUBLISH, instead
	// error and terminate the connection.
	rejectQoS2Pub bool

	// downgradeQOS2Sub tells the MQTT client to downgrade QoS2 SUBSCRIBE
	// requests to QoS1.
	downgradeQoS2Sub bool
}

// mqttProperties holds the parsed MQTT 5.0 variable-header properties for a
// packet. Only the fields needed by the current implementation are tracked;
// recognized-but-unused properties are validated then discarded. Spec
// [MQTT-2.2.2].
type mqttProperties struct {
	sessionExpiry    uint32
	receiveMax       uint16
	maxPacketSize    uint32
	topicAliasMax    uint16
	topicAlias       uint16
	willDelay        uint32
	messageExpiry    uint32
	serverKeepAlive  uint16
	maxQoS           byte
	payloadFormat    byte
	requestProblem   byte
	requestResponse  byte
	retainAvailable  byte
	wildcardSubAvail byte
	subIDAvail       byte
	sharedSubAvail   byte
	subIDs           []int
	assignedClientID string
	authMethod       string
	contentType      string
	responseTopic    string
	responseInfo     string
	reasonString     string
	correlationData  []byte
	authData         []byte
	user             []mqttUserProperty
	// Tracks which scalar properties were present, so we can reject illegal
	// duplicates and tell "absent" apart from "explicit zero".
	present map[byte]bool
}

type mqttUserProperty struct {
	key   string
	value string
}

type mqttPending struct {
	sseq         uint64 // stream sequence
	jsAckSubject string // the ACK subject to send the ack to
	jsDur        string // JS durable name
	// q2AffClaim is the subject of the assignment claim that authorized this
	// delivery, set only for a session-affine shared QoS 2 copy. It travels with
	// the pending entry because the claim's affine-side completion can only be
	// recorded from the terminal paths (PUBREC, expiry, oversize discard), which
	// see the packet identifier and nothing else. See qos2_shared_affine.go.
	q2AffClaim string

	// regSeq is the stream sequence of this delivery's durable in-flight register on
	// $MQTT_inflight, and regNonce the register's immutable identity. Every delete
	// targets regSeq exactly, because the register's subject is reused by later
	// exchanges on the same identifier (R4). Zero/empty for a delivery with no
	// register: a retained send (no JetStream backing at all), or one recovered from
	// a PUBREL record, which is past the PUBLISH phase and whose identity lives in
	// that record instead (R2).
	//
	// regNonce is minted with the ALLOCATION, one step ahead of the store that writes
	// it, so an allocation cancelled while its store is in flight can be named durably
	// before any sequence for it exists. regSeq appears only on confirmation.
	regSeq   uint64
	regNonce string
	// regForeign marks a reservation left by a LOST shared-claim arbitration: the
	// register at regSeq is another session object's claimed identity. The identifier
	// stays reserved (the client may hold it, and the non-zero regSeq keeps
	// dropUnredeliverableRetained from reading it as a retained send), but no terminal
	// path may settle the record — deleting a register this object does not own would
	// destroy the rival's only durable identity. See inflight_register.go.
	regForeign bool

	// (see the session's piSpaceIncomplete for the other half of R3)
	//
	// regPending marks an identifier that has been ALLOCATED but whose register is
	// not yet confirmed, so the PUBLISH has not been transmitted and must not be
	// (R1). Nothing else may act on the entry while it is set: a concurrent
	// redelivery declines it, resendPendingRedelivery does not NAK it, and
	// stageDrain does not enrol it. Cleared by the register store's owner on a
	// confirmed write; on a definite failure the entry is removed outright.
	regPending bool

	// regCancelled marks an entry a consumer teardown has WITHDRAWN. It stays in
	// pendingPublish, inert, until BOTH release conditions hold: the register store has
	// SETTLED (freeing the identifier earlier would let a new delivery take the subject
	// while the old CAS was still in flight, and the cancellation tombstone would then be
	// indistinguishable from that new delivery's record), and some durable rung of the
	// cleanup ladder is PROVEN (released with none, a crash could resurrect the register
	// as an ordinary delivery nothing anywhere calls void). While regPending is still set
	// the store is unsettled; the settle points drop it, and
	// releaseSettledVoidReservationsLocked frees the identifier once the verdict's save
	// lands. Holding it costs one in-flight store's round trip in the common case.
	regCancelled bool
}

// mqttPendingRef identifies one in-flight QoS 1/2 delivery. A packet identifier
// alone does not: identifiers are released and reused, so the same number can
// name a different delivery a moment later. The (durable, stream sequence) it
// was delivered from is what makes it the same delivery.
type mqttPendingRef struct {
	pi    uint16
	sseq  uint64
	jsDur string
}

// mqttPendingRefs snapshots the identities currently tracked in a pending map.
// Returns nil for an empty map, which reads the same as "nothing was pending".
//
// Session lock held on entry.
func mqttPendingRefs(pending map[uint16]*mqttPending) map[mqttPendingRef]struct{} {
	if len(pending) == 0 {
		return nil
	}
	refs := make(map[mqttPendingRef]struct{}, len(pending))
	for pi, ack := range pending {
		refs[mqttPendingRef{pi, ack.sseq, ack.jsDur}] = struct{}{}
	}
	return refs
}

// mqttDeferredKey identifies a JS message delivery withheld by outbound flow
// control: the consumer durable it came from and its stream sequence.
type mqttDeferredKey struct {
	jsDur string
	sseq  uint64
}

type mqttConnectProto struct {
	rd    time.Duration
	ka    uint16 // client-requested keep alive (seconds), 0 = none
	will  *mqttWill
	flags byte
	// MQTT 5.0 CONNECT properties (nil for 3.1.1).
	props *mqttProperties
}

type mqttIOReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

type mqttReader struct {
	reader mqttIOReader
	buf    []byte
	pos    int
	pstart int
	pbuf   []byte
}

type mqttWriter struct {
	bytes.Buffer
}

type mqttWill struct {
	topic   []byte
	subject []byte
	mapped  []byte
	message []byte
	qos     byte
	retain  bool
	// MQTT 5.0 Will properties (nil for 3.1.1).
	props *mqttProperties
}

// mqttSharedSubPrefix is the MQTT 5.0 Shared Subscription topic filter prefix
// ("$share/{ShareName}/{filter}"). Spec5 [4.8.2].
var mqttSharedSubPrefix = []byte("$share/")

// mqttSharedNames returns the deterministic, cluster-wide names for a shared
// subscription group on a given underlying NATS subject: the queue-group name
// (also used as the JS consumer DeliverGroup), the consumer's deliver subject,
// and the durable consumer name. Keyed by a hash of ShareName + subject so all
// members converge on one consumer; 16 chars to make an accidental collision
// (which would merge two distinct shared subscriptions) negligible. Spec5 [4.8.2].
func mqttSharedNames(group, subject string) (queue, deliver, durable string) {
	qkey := getHashSize(group+" "+subject, 16)
	queue = mqttSharedQueuePrefix + qkey
	deliver = mqttSharedSubDeliverPrefix + qkey
	durable = mqttSharedConsumerDurablePrefix + qkey
	return queue, deliver, durable
}

type mqttFilter struct {
	filter string
	qos    byte
	// v5 Retain Handling subscription option (bits 4-5 of the options byte):
	// 0 = send retained at subscribe, 1 = send only if the subscription is new,
	// 2 = never. Zero value (0) matches 3.1.1 behavior. Spec5 [3.8.3.1].
	retainHandling byte
	// v5 No Local subscription option (bit 2): when set, a message must not be
	// delivered back to the connection that published it. Spec5 [3.8.3.1].
	noLocal bool
	// v5 Retain As Published subscription option (bit 3): when set, a
	// live-forwarded message keeps the RETAIN flag it was published with. Spec5
	// [3.8.3.1].
	retainAsPublished bool
	// v5 Subscription Identifier from the SUBSCRIBE properties block (0 = none).
	// The packet's single identifier applies to every filter it carries. Spec5
	// [3.8.2.1.2].
	subID int
	// v5 (UN)SUBACK reason code for this filter (zero value is success).
	reason byte
	// v5 human-readable failure detail, set where a filter is rejected. The
	// (UN)SUBACK packet-level Reason String carries the first failed filter's
	// text. Spec5 [3.9.2.2.2].
	reasonStr string
	// shared is set when the filter is an MQTT 5.0 Shared Subscription
	// ("$share/{ShareName}/{filter}", v5 only). group holds the ShareName. When
	// shared, filter holds the NATS subject of the underlying topic filter only
	// (the "$share/" prefix and ShareName are stripped), so all existing
	// per-subject logic applies unchanged. Spec5 [4.8.2].
	shared bool
	group  string
	// Used only for tracing and should not be used after parsing of (un)sub protocols.
	ttopic []byte
}

// sessionKey returns the key used for this filter in the session's subscription
// maps and as the NATS subscription sid. A shared subscription embeds its
// ShareName so distinct groups (and a plain subscription) on the same subject do
// not collide; a plain subscription just uses its NATS subject (which never
// contains '/'). Spec5 [4.8.2].
func (f *mqttFilter) sessionKey() string {
	if f.shared {
		return string(mqttSharedSubPrefix) + f.group + "/" + f.filter
	}
	return f.filter
}

type mqttPublish struct {
	topic   []byte
	subject []byte
	mapped  []byte
	msg     []byte
	sz      int
	pi      uint16
	flags   byte
	// props is the raw MQTT 5.0 properties block (variable-int length prefix +
	// body, exactly as received on the wire), captured so it can be forwarded
	// unaltered to v5 subscribers. nil when absent or empty (3.1.1, or a v5
	// PUBLISH with no properties). Spec5 [3.3.2.3].
	props []byte
}

// When we re-encode incoming MQTT PUBLISH messages for NATS delivery, we add
// the following headers:
//   - "Nmqtt-Pub" (*always) indicates that the message originated from MQTT, and
//     contains the original message QoS.
//   - "Nmqtt-Subject" contains the original MQTT subject from mqttParsePub.
//   - "Nmqtt-Mapped" contains the mapping during mqttParsePub.
//
// When we submit a PUBREL for delivery, we add a "Nmqtt-PubRel" header that
// contains the PI.
const (
	// NATS header that indicates that the message originated from MQTT and
	// stores the published message QOS.
	mqttNatsHeader = "Nmqtt-Pub"

	// NATS headers to store retained message metadata (along with the original
	// message as binary).
	mqttNatsRetainedMessageTopic  = "Nmqtt-RTopic"
	mqttNatsRetainedMessageOrigin = "Nmqtt-ROrigin"
	mqttNatsRetainedMessageFlags  = "Nmqtt-RFlags"
	mqttNatsRetainedMessageSource = "Nmqtt-RSource"
	// Authenticated No Local origin marker for the session that published the
	// retained message (see mqttNatsHeaderOrigin). Absent for older stored
	// messages or when v5 is disabled.
	mqttNatsRetainedMessageSessionHash = "Nmqtt-RSession"
	// Raw MQTT 5.0 properties block for a retained message (base64), so v5
	// properties are replayed to subscribers. Absent for older stored messages.
	mqttNatsRetainedMessageProps = "Nmqtt-RProps"

	// Absolute expiry deadline (Unix nanoseconds, decimal) for a retained message
	// that was published with an MQTT 5.0 Message Expiry Interval. Carried with
	// the stored message so the remaining interval can be computed on replay
	// regardless of which server (or cache) serves it. Absent when the retained
	// message has no expiry. Spec5 [3.3.2.3.3].
	mqttNatsRetainedMessageExpiry = "Nmqtt-RExp"

	// NATS header that indicates that the message is an MQTT PubRel and stores
	// the PI.
	mqttNatsPubRelHeader = "Nmqtt-PubRel"

	// NATS headers to store the original MQTT subject and the subject mapping.
	mqttNatsHeaderSubject = "Nmqtt-Subject"
	mqttNatsHeaderMapped  = "Nmqtt-Mapped"

	// NATS header carrying the raw MQTT 5.0 PUBLISH properties block
	// (base64-encoded, since the block is binary and NATS header values are
	// CRLF-delimited text) so end-to-end properties survive the NATS/JetStream
	// hop and can be re-emitted to v5 subscribers.
	mqttNatsHeaderProps = "Nmqtt-Props"

	// NATS header carrying an authenticated No Local origin marker (an HMAC of
	// the publishing session hash, subject, and payload under a per-server
	// secret), so the v5 No Local option can suppress delivering a QoS 1/2
	// message back to the connection that published it after it crosses the
	// JetStream store, where the origin client is otherwise lost. Because it is
	// an HMAC, a client that can write the stream subjects cannot forge it. Absent
	// when the publisher
	// has no session (e.g. an internal Will client) or v5 is off. Spec5 [3.8.3.1].
	mqttNatsHeaderOrigin = "Nmqtt-Origin"

	// NATS header (value "1") marking that the message was published with the
	// RETAIN flag set, so the v5 Retain As Published subscription option can keep
	// that flag on a live forward after the original flag is cleared for normal
	// delivery. Present only for retained publishes when v5 is enabled. Spec5
	// [3.8.3.1].
	mqttNatsHeaderRetain = "Nmqtt-Ret"
)

type mqttParsedPublishNATSHeader struct {
	qos      byte
	subject  []byte
	mapped   []byte
	retained bool   // published with RETAIN set (from Nmqtt-Ret), for Retain As Published
	props    []byte // raw MQTT 5.0 properties block (decoded from Nmqtt-Props)
	origin   []byte // authenticated No Local origin marker (from Nmqtt-Origin)
}

func (s *Server) startMQTT() {
	if s.isShuttingDown() {
		return
	}

	sopts := s.getOpts()
	o := &sopts.MQTT

	var hl net.Listener
	var err error

	port := o.Port
	if port == -1 {
		port = 0
	}
	hp := net.JoinHostPort(o.Host, strconv.Itoa(port))
	s.mu.Lock()
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	// MQTT 5.0 is always enabled in mqtt-nats-bridge.
	s.mqtt.v5Enabled = true
	// Per-server secret keying the No Local origin HMAC. Best-effort: if the read
	// fails the key stays nil and No Local simply never suppresses (fail-open).
	if s.mqtt.originKey == nil {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err == nil {
			s.mqtt.originKey = key
		}
	}
	hl, err = natsListen("tcp", hp)
	s.mqtt.listenerErr = err
	if err != nil {
		s.mu.Unlock()
		s.Fatalf("Unable to listen for MQTT connections: %v", err)
		return
	}
	if port == 0 {
		o.Port = hl.Addr().(*net.TCPAddr).Port
	}
	s.mqtt.listener = hl
	scheme := "mqtt"
	if o.TLSConfig != nil {
		scheme = "tls"
	}
	s.Noticef("Listening for MQTT clients on %s://%s:%d", scheme, o.Host, o.Port)
	go s.acceptConnections(hl, "MQTT", func(conn net.Conn) { s.createMQTTClient(conn, nil) }, nil)
	s.mu.Unlock()
}

// This is similar to createClient() but has some modifications specifi to MQTT clients.
// The comments have been kept to minimum to reduce code size. Check createClient() for
// more details.
func (s *Server) createMQTTClient(conn net.Conn, ws *websocket) *client {
	opts := s.getOpts()

	maxPay := int32(opts.MaxPayload)
	maxSubs := int32(opts.MaxSubs)
	if maxSubs == 0 {
		maxSubs = -1
	}
	now := time.Now()

	mqtt := &mqtt{
		rejectQoS2Pub:    opts.MQTT.rejectQoS2Pub,
		downgradeQoS2Sub: opts.MQTT.downgradeQoS2Sub,
	}
	c := &client{srv: s, nc: conn, rawConn: conn, mpay: maxPay, msubs: maxSubs, start: now, last: now, mqtt: mqtt, ws: ws}
	c.headers = true
	c.mqtt.pp = &mqttPublish{}
	// MQTT clients don't send NATS CONNECT protocols. So make it an "echo"
	// client, but disable verbose and pedantic (by not setting them).
	c.opts.Echo = true

	c.registerWithAccount(s.globalAccount())

	s.mu.Lock()
	// Check auth, override if applicable.
	authRequired := s.info.AuthRequired || s.mqtt.authOverride
	s.totalClients++
	s.mu.Unlock()

	c.mu.Lock()
	if authRequired {
		c.flags.set(expectConnect)
	}
	c.initClient()
	c.Debugf("Client connection created")
	c.mu.Unlock()

	s.mu.Lock()
	if !s.isRunning() || s.ldm {
		if s.isShuttingDown() {
			conn.Close()
		}
		s.mu.Unlock()
		return c
	}

	if opts.MaxConn < 0 || (opts.MaxConn > 0 && len(s.clients) >= opts.MaxConn) {
		s.mu.Unlock()
		c.maxConnExceeded()
		return nil
	}
	s.clients[c.cid] = c
	// Reserve the shutdown-drain lifecycle token under the same s.mu that makes
	// the client visible to a Shutdown snapshot; released via c.finish() from
	// readLoop's defer or abortSetup. Every early return below goes through
	// abortSetup so the token is never leaked.
	s.clientWG.Add(1)

	// Websocket TLS handshake is already done when getting to this function.
	tlsRequired := opts.MQTT.TLSConfig != nil && ws == nil
	s.mu.Unlock()

	c.mu.Lock()

	// In case connection has already been closed
	if c.isClosed() {
		c.mu.Unlock()
		c.abortSetup(WriteError)
		return nil
	}

	var pre []byte
	if tlsRequired && opts.AllowNonTLS {
		pre = make([]byte, 4)
		c.nc.SetReadDeadline(time.Now().Add(secondsToDuration(mqttTLSTimeout(opts.MQTT.TLSTimeout))))
		n, _ := io.ReadFull(c.nc, pre[:])
		c.nc.SetReadDeadline(time.Time{})
		pre = pre[:n]
		if n > 0 && pre[0] == 0x16 {
			tlsRequired = true
		} else {
			tlsRequired = false
		}
	}

	if tlsRequired {
		if len(pre) > 0 {
			c.nc = &tlsMixConn{c.nc, bytes.NewBuffer(pre)}
			pre = nil
		}

		// Perform server-side TLS handshake.
		if err := c.doTLSServerHandshake(tlsHandshakeMQTT, opts.MQTT.TLSConfig, opts.MQTT.TLSTimeout, opts.MQTT.TLSPinnedCerts); err != nil {
			c.mu.Unlock()
			c.abortSetup(TLSHandshakeError)
			return nil
		}
	}

	if authRequired {
		timeout := opts.AuthTimeout
		// Possibly override with MQTT specific value.
		if opts.MQTT.AuthTimeout != 0 {
			timeout = opts.MQTT.AuthTimeout
		}
		c.setAuthTimer(secondsToDuration(timeout))
	}

	// No Ping timer for MQTT clients...

	// readLoop is tracked by clientWG (token reserved at registration, released
	// by its defer via c.finish), so Shutdown Phase 1 can wait for client cleanup
	// independently of the background grWG goroutines. writeLoop stays on grWG.
	go c.readLoop(pre)
	s.startGoRoutine(func() { c.writeLoop() })

	if tlsRequired {
		c.Debugf("TLS handshake complete")
		cs := c.nc.(*tls.Conn).ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tls.CipherSuiteName(cs.CipherSuite))
	}

	c.mu.Unlock()

	return c
}

// Given the mqtt options, we check if any auth configuration
// has been provided. If so, possibly create users/nkey users and
// store them in s.mqtt.users/nkeys.
// Also update a boolean that indicates if auth is required for
// mqtt clients.
// Server lock is held on entry.
func (s *Server) mqttConfigAuth(opts *MQTTOpts) {
	mqtt := &s.mqtt
	// If any of those is specified, we consider that there is an override.
	mqtt.authOverride = opts.Username != _EMPTY_ || opts.Token != _EMPTY_ || opts.NoAuthUser != _EMPTY_
}

// Validate the mqtt related options.
func validateMQTTOptions(o *Options) error {
	mo := &o.MQTT
	// If no port is defined, we don't care about other options
	if mo.Port == 0 {
		return nil
	}
	// We have to force the server name to be explicitly set and be unique when
	// in cluster mode.
	if o.ServerName == _EMPTY_ && (o.Cluster.Port != 0 || o.Gateway.Port != 0) {
		return errMQTTServerNameMustBeSet
	}
	// If there is a NoAuthUser, we need to have Users defined and
	// the user to be present.
	if mo.NoAuthUser != _EMPTY_ {
		if err := validateNoAuthUser(o, mo.NoAuthUser); err != nil {
			return err
		}
	}
	// Token/Username not possible if there are users/nkeys
	if len(o.Users) > 0 || len(o.Nkeys) > 0 {
		if mo.Username != _EMPTY_ {
			return errMQTTUserMixWithUsersNKeys
		}
		if mo.Token != _EMPTY_ {
			return errMQTTTokenMixWIthUsersNKeys
		}
	}
	if mo.AckWait < 0 {
		return errMQTTAckWaitMustBePositive
	}
	if mo.JSAPITimeout < 0 {
		return errMQTTJSAPITimeoutMustBePositive
	}
	// Validate the max protocol version for all configuration paths (config
	// file parsing already checks this, but programmatic Options do not). 0
	// means default (3.1.1 only); 4 and 5 are the supported explicit values.
	// Any other value would make the runtime cap reject even 3.1.1 connects.
	switch mo.MaxProtocolVersion {
	case 0, mqttProtoLevel, mqttProtoLevel5:
	default:
		return fmt.Errorf("mqtt max_protocol_version must be 0 (default), 4 or 5, got %d", mo.MaxProtocolVersion)
	}
	// Topic Alias Maximum: 0 = use default, -1 = disabled, 1..65535 explicit.
	// Config-file parsing already checks this; programmatic Options do not.
	if mo.TopicAliasMaximum < -1 || mo.TopicAliasMaximum > 0xFFFF {
		return fmt.Errorf("mqtt topic_alias_maximum must be in [-1..%d] (0 = default %d, -1 disables), got %d",
			0xFFFF, mqttDefaultTopicAliasMax, mo.TopicAliasMaximum)
	}
	// Receive Maximum: 0 = use default, -1 (or config 0) = disabled, 1..65535
	// explicit. Config-file parsing already checks this; programmatic Options do
	// not.
	if mo.ReceiveMaximum < -1 || mo.ReceiveMaximum > 0xFFFF {
		return fmt.Errorf("mqtt receive_maximum must be in [-1..%d] (0 = default %d, -1 disables), got %d",
			0xFFFF, mqttDefaultReceiveMax, mo.ReceiveMaximum)
	}
	// Keep Alive Maximum: 0 = no override, 1..65535 explicit. No -1 sentinel is
	// needed (unlike topic_alias_maximum): there is no non-zero default, so
	// unset and disabled are the same thing.
	if mo.KeepAliveMaximum < 0 || mo.KeepAliveMaximum > 0xFFFF {
		return fmt.Errorf("mqtt keep_alive_maximum must be in [0..%d] (0 = no override), got %d", 0xFFFF, mo.KeepAliveMaximum)
	}
	// If strictly standalone and there is no JS enabled, then it won't work...
	// For leafnodes, we could either have remote(s) and it would be ok, or no
	// remote but accept from a remote side that has "hub" property set, which
	// then would ok too. So we fail only if we have no leafnode config at all.
	if !o.JetStream && o.Cluster.Port == 0 && o.Gateway.Port == 0 &&
		o.LeafNode.Port == 0 && len(o.LeafNode.Remotes) == 0 {
		return errMQTTStandaloneNeedsJetStream
	}
	if err := validatePinnedCerts(mo.TLSPinnedCerts); err != nil {
		return fmt.Errorf("mqtt: %v", err)
	}
	if mo.ConsumerReplicas > 0 && mo.StreamReplicas > 0 && mo.ConsumerReplicas > mo.StreamReplicas {
		return fmt.Errorf("mqtt: consumer_replicas (%v) cannot be higher than stream_replicas (%v)",
			mo.ConsumerReplicas, mo.StreamReplicas)
	}
	return nil
}

// Returns true if this connection is from a MQTT client.
// Lock held on entry.
func (c *client) isMqtt() bool {
	return c.mqtt != nil
}

// If this is an MQTT client, returns the session client ID,
// otherwise returns the empty string.
// Lock held on entry
func (c *client) getMQTTClientID() string {
	if !c.isMqtt() {
		return _EMPTY_
	}
	return c.mqtt.cid
}

// Parse protocols inside the given buffer.
// This is invoked from the readLoop.
func (c *client) mqttParse(buf []byte) error {
	c.mu.Lock()
	s := c.srv
	trace := c.trace
	connected := c.flags.isSet(connectReceived)
	mqtt := c.mqtt
	r := mqtt.r
	var rd time.Duration
	if mqtt.cp != nil {
		rd = mqtt.cp.rd
		if rd > 0 {
			r.reader.SetReadDeadline(time.Time{})
		}
	}
	hasMappings := c.in.flags.isSet(hasMappings)
	c.mu.Unlock()

	r.reset(buf)

	var err error
	var b byte
	var pl int
	var complete bool

	for err == nil && r.hasMore() {

		// Keep track of the starting of the packet, in case we have a partial
		r.pstart = r.pos

		// Read packet type and flags
		if b, err = r.readByte("packet type"); err != nil {
			break
		}

		// Packet type
		pt := b & mqttPacketMask

		// If client was not connected yet, the first packet must be
		// a mqttPacketConnect otherwise we fail the connection.
		if !connected && pt != mqttPacketConnect {
			// If the buffer indicates that it may be a websocket handshake
			// but the client is not websocket, it means that the client
			// connected to the MQTT port instead of the Websocket port.
			if bytes.HasPrefix(buf, []byte("GET ")) && !c.isWebsocket() {
				err = errMQTTNotWebsocketPort
			} else {
				err = fmt.Errorf("the first packet should be a CONNECT (%v), got %v", mqttPacketConnect, pt)
			}
			break
		}
		if err = mqttCheckFixedHeaderFlags(pt, b&mqttPacketFlagMask); err != nil {
			break
		}

		maxLen := int32(jwt.NoLimit)
		if !connected {
			maxLen = atomic.LoadInt32(&c.mpay)
		} else if mps := c.mqtt.maxInPacketSize; mps > 0 {
			// Enforce the Maximum Packet Size advertised in the CONNACK. A larger
			// packet is cut off before its body is buffered. [MQTT-3.2.2-15].
			maxLen = int32(mps)
		}
		pl, complete, err = r.readPacketLen(maxLen)
		if err != nil || !complete {
			if err == ErrMaxPayload {
				// Tell a connected v5 client why before the close (no-op for 3.1.1
				// and pre-CONNECT); maxPayloadViolation then logs and closes.
				if connected {
					c.mqttEnqueueDisconnect(mqttReasonPacketTooLarge,
						fmt.Sprintf("packet size %d exceeds maximum packet size %d", pl, maxLen))
				}
				c.maxPayloadViolation(pl, maxLen)
			}
			break
		}
		if err = mqttCheckRemainingLength(pt, c.mqtt.proto, pl); err != nil {
			break
		}

		// A definitely rejected record CAS means the session subject MOVED, so this
		// broker's ownership belief may be obsolete — and BEFORE DISPATCH is the only
		// place that can act on it: a QoS 1 PUBLISH routed and acked from a displaced
		// connection cannot be retracted afterwards. Per packet rather than per read,
		// because a save the PREVIOUS packet in this same buffer triggered (an
		// UNSUBSCRIBE's voided-register flush) can arm the marker between the two.
		if connected {
			if proceed, serr := c.mqttGateDispatchOnRejection(pt); !proceed {
				if serr != nil {
					// No verdict: fail closed through the ordinary parse-error path, so
					// the connection is really torn down and the close bookkeeping runs.
					err = serr
					break
				}
				// Foreign or displaced: this connection has been disposed, its one
				// DISCONNECT attempted (or deliberately suppressed — pre-CONNACK, or
				// already claimed) and closed. Return as the graceful DISCONNECT branch
				// does: an error here would put the footer in the race for the REASON
				// the client is left with — the claim would refuse whichever attempt is
				// second, so the cost is a wrong reason rather than a second packet.
				// The footer's own pre-CONNACK exposure is closed elsewhere, by
				// mqttAbortAcceptedConnect clearing connectReceived.
				return nil
			}
		}

		switch pt {
		// Packets that we receive back when we act as the "sender": PUBACK,
		// PUBREC, PUBCOMP.
		case mqttPacketPubAck:
			var pi uint16
			pi, _, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBACK", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubAck(pi)
			}

		case mqttPacketPubRec:
			var pi uint16
			var reason byte
			pi, reason, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBREC", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = c.mqttProcessPubRec(pi, reason)
			}

		case mqttPacketPubComp:
			var pi uint16
			pi, _, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBCOMP", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				c.mqttProcessPubComp(pi)
			}

		// Packets where we act as the "receiver": PUBLISH, PUBREL, SUBSCRIBE, UNSUBSCRIBE.
		case mqttPacketPub:
			pp := c.mqtt.pp
			pp.flags = b & mqttPacketFlagMask
			err = c.mqttParsePub(r, pl, pp, hasMappings)
			if trace {
				c.traceInOp("PUBLISH", errOrTrace(err, mqttPubTrace(pp)))
				if err == nil {
					c.mqttTraceMsg(pp.msg)
				}
			}
			if err == nil {
				err = s.mqttProcessPub(c, pp, trace)
			}

		case mqttPacketPubRel:
			var pi uint16
			pi, _, err = mqttParsePubResponsePacket(r, c.mqtt.proto, pl)
			if trace {
				c.traceInOp("PUBREL", errOrTrace(err, fmt.Sprintf("pi=%v", pi)))
			}
			if err == nil {
				err = s.mqttProcessPubRel(c, pi, trace)
			}

		case mqttPacketSub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			var subs []*subscription
			pi, filters, err = c.mqttParseSubs(r, b, pl)
			if trace {
				c.traceInOp("SUBSCRIBE", errOrTrace(err, mqttSubscribeTrace(pi, filters)))
			}
			if err == nil {
				subs, err = c.mqttProcessSubs(filters)
				c.mqttResolveAfterPacket()
			}
			if err == nil {
				// Trace only if the SUBACK was actually sent (a tiny client
				// Maximum Packet Size can suppress it).
				if c.mqttEnqueueSubAck(pi, filters) && trace {
					c.traceOutOp("SUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
				c.mqttSendRetainedMsgsToNewSubs(subs)
			}

		case mqttPacketUnsub:
			var pi uint16 // packet identifier
			var filters []*mqttFilter
			pi, filters, err = c.mqttParseUnsubs(r, b, pl)
			if trace {
				c.traceInOp("UNSUBSCRIBE", errOrTrace(err, mqttUnsubscribeTrace(pi, filters)))
			}
			var unsubAckRestore *mqttUnsubAckRestore
			if err == nil {
				unsubAckRestore, err = c.mqttProcessUnsubs(filters)
				c.mqttResolveAfterPacket()
				// Unconditional: an UNSUBSCRIBE that cancelled a register-pending
				// delivery leaves the cleanup obligation with the delivery goroutine,
				// which was refused the fence while this packet held it — and that is
				// true whether the packet itself succeeded.
				c.mqttFlushVoidedRegs()
			}
			if err == nil {
				// After the resolution above, because an ambiguous write proven
				// durable by it turns this packet's 0x83 reasons back into the true
				// ones. Trace only if the UNSUBACK was actually sent (a tiny client
				// Maximum Packet Size can suppress it, and a displaced connection is
				// not acknowledged at all).
				if c.mqttAckUnsubscribe(pi, filters, unsubAckRestore) && trace {
					c.traceOutOp("UNSUBACK", []byte(fmt.Sprintf("pi=%v", pi)))
				}
			}

		// Packets that we get both as a receiver and sender: PING, CONNECT, DISCONNECT
		case mqttPacketPing:
			if trace {
				c.traceInOp("PINGREQ", nil)
			}
			c.mqttEnqueuePingResp()
			if trace {
				c.traceOutOp("PINGRESP", nil)
			}

		case mqttPacketConnect:
			// It is an error to receive a second connect packet
			if connected {
				err = errMQTTSecondConnectPacket
				break
			}
			var rc byte
			var cp *mqttConnectProto
			var sessp bool
			start := r.pos
			rc, cp, err = c.mqttParseConnect(r, hasMappings)
			// CONNECT has no trailing length check of its own; a lying
			// properties length would silently desync the packet stream.
			if err == nil && rc == 0 && r.pos-start != pl {
				err = fmt.Errorf("connect packet length mismatch: consumed %v bytes, remaining length is %v", r.pos-start, pl)
			}
			// Add the client id to the client's string, regardless of error.
			// We may still get the client_id if the call above fails somewhere
			// after parsing the client ID itself.
			c.ncs.Store(fmt.Sprintf("%s - %q", c, c.mqtt.cid))
			if trace && cp != nil {
				c.traceInOp("CONNECT", errOrTrace(err, c.mqttConnectTrace(cp)))
			}
			if rc != 0 {
				var rs string
				if err != nil {
					rs = err.Error()
				}
				sent, _ := c.mqttEnqueueConnAck(rc, sessp, rs)
				if sent && trace {
					c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
				}
			} else if err != nil {
				// A malformed CONNECT has no return code. v5 expects a CONNACK
				// with the failure reason before the close; 3.1.1 just closes.
				// Spec5 [3.1.4.1], [MQTT-3.1.2-19].
				if c.mqtt.proto == mqttProtoLevel5 {
					reason := mqttConnAckReasonFromConnectErr(err)
					sent, _ := c.mqttEnqueueConnAck(reason, false, err.Error())
					if sent && trace {
						c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", false, reason)))
					}
				}
			} else {
				if err = s.mqttProcessConnect(c, cp, trace); err != nil {
					err = fmt.Errorf("unable to connect: %v", err)
					// If the success CONNACK already went out, mark connected so
					// the v5 error path below sends a DISCONNECT with the reason
					// instead of a bare close. Read under the client lock: the
					// writeLoop mutates c.flags while flushing that CONNACK.
					c.mu.Lock()
					connected = c.flags.isSet(connectReceived)
					c.mu.Unlock()
				} else {
					// Add this debug statement so users running in Debug mode
					// will have the client id printed here for the first time.
					c.Debugf("Client connected")
					connected = true
					rd = cp.rd
				}
			}

		case mqttPacketDisconnect:
			// By default a DISCONNECT discards the Will. Spec [MQTT-3.1.2-8].
			discardWill := true
			if c.mqtt.proto == mqttProtoLevel5 && pl > 0 {
				// v5 DISCONNECT carries a reason code and optional properties.
				// Spec5 [3.14]. Reason 0x04 ("Disconnect with Will Message")
				// instructs the server to publish the Will instead.
				var rcode byte
				if rcode, err = r.readByte("disconnect reason code"); err != nil {
					break
				}
				var props *mqttProperties
				if pl > 1 {
					if props, err = r.readProperties(mqttPacketDisconnect); err != nil {
						break
					}
				}
				if rcode == mqttReasonDisconnectWithWill {
					discardWill = false
				}
				// Spec5 [3.14.2.2.2]: a DISCONNECT may update the Session Expiry
				// Interval. Raising it from 0 is a Protocol Error; ignore that
				// override rather than fail an already-closing connection.
				if props != nil && props.present[mqttPropSessionExpiry] {
					c.mu.Lock()
					sess := c.mqtt.sess
					// The connection's own copy moves with it, whether or not it still
					// owns the session below: it is what classifies this connection's
					// Will. Same Protocol Error rule, applied to its own value.
					if c.mqtt.sessionExpiry != 0 || props.sessionExpiry == 0 {
						c.mqtt.sessionExpiry = props.sessionExpiry
					}
					c.mu.Unlock()
					if sess != nil {
						sess.mu.Lock()
						// Only if this client still owns the session: after a takeover
						// sess.c is the new client, and a late DISCONNECT from the old
						// connection must not clobber the live session's interval.
						if sess.c == c && (sess.expiryInterval != 0 || props.sessionExpiry == 0) {
							sess.expiryInterval = props.sessionExpiry
						}
						sess.mu.Unlock()
					}
				}
			}
			if trace {
				c.traceInOp("DISCONNECT", nil)
			}
			if discardWill {
				c.mqttDiscardWill()
			} else {
				// Reason 0x04 makes the Will owed from this point, recorded before
				// the teardown below runs and before any same-client-ID CONNECT can
				// take the session over: from here the Will is no longer a takeover's
				// to discard. Spec5 [3.14.4].
				c.mqttOweWill(c.mqttSessionExpiry())
			}
			s.mqttHandleClosedClient(c)
			c.closeConnection(ClientClosed)
			return nil

		default:
			err = fmt.Errorf("%w: received unknown packet type %d", errMQTTMalformedPacket, pt>>4)
		}

	}
	if err == nil && rd > 0 {
		r.reader.SetReadDeadline(time.Now().Add(rd))
	} else if err != nil && err != ErrMaxPayload && connected && c.mqtt.proto == mqttProtoLevel5 {
		// Best-effort: tell the v5 client why we are about to close. The read
		// loop closes the connection right after this returns. ErrMaxPayload is
		// excluded: it already sent a DISCONNECT 0x95 and closed above.
		c.mqttEnqueueDisconnect(mqttDisconnectReasonFromErr(err), err.Error())
	}
	return err
}

func mqttCheckFixedHeaderFlags(packetType, flags byte) error {
	var expected byte
	switch packetType {
	case mqttPacketConnect, mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubComp,
		mqttPacketPing, mqttPacketDisconnect:
		expected = 0
	case mqttPacketPubRel, mqttPacketSub, mqttPacketUnsub:
		expected = 0x2
	case mqttPacketPub:
		return nil
	default:
		return nil
	}
	if flags != expected {
		return fmt.Errorf("%w: invalid fixed header flags %x for packet type %x", errMQTTMalformedPacket, flags, packetType)
	}
	return nil
}

func mqttCheckRemainingLength(packetType, proto byte, pl int) error {
	v5 := proto == mqttProtoLevel5
	var expected int
	switch packetType {
	case mqttPacketConnect, mqttPacketPub, mqttPacketSub, mqttPacketUnsub:
		return nil
	case mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubRel, mqttPacketPubComp:
		if v5 {
			// v5: packet identifier (2) + optional reason code + optional
			// properties. Spec5 [3.4.2.1].
			if pl < 2 {
				return fmt.Errorf("%w: invalid remaining length %d for packet type %x", errMQTTMalformedPacket, pl, packetType)
			}
			return nil
		}
		expected = 2
	case mqttPacketDisconnect:
		if v5 {
			// v5: 0 (normal disconnect) or reason code + optional properties.
			// Spec5 [3.14.2.1].
			return nil
		}
		expected = 0
	case mqttPacketPing:
		expected = 0
	default:
		return nil
	}
	if pl != expected {
		return fmt.Errorf("%w: invalid remaining length %d for packet type %x", errMQTTMalformedPacket, pl, packetType)
	}
	return nil
}

func (c *client) mqttTraceMsg(msg []byte) {
	maxTrace := c.srv.getOpts().MaxTracedMsgLen
	if maxTrace > 0 && len(msg) > maxTrace {
		c.Tracef("<<- MSG_PAYLOAD: [\"%s...\"]", msg[:maxTrace])
	} else {
		c.Tracef("<<- MSG_PAYLOAD: [%q]", msg)
	}
}

// The MQTT client connection has been closed, or the DISCONNECT packet was received.
// For a "clean" session, we will delete the session, otherwise, simply removing
// the binding. We will also send the "will" message if applicable.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttHandleClosedClient(c *client) {
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	// If asm or sess are nil, it means that we have failed a client
	// before it was associated with a session, so nothing more to do.
	if asm == nil || sess == nil {
		return
	}

	// Whatever closed this connection, classify what its Will is owed BEFORE
	// contending for the session gate below. A same-client-ID CONNECT may be
	// taking the session over right now, and both sides decide the Will's fate;
	// this is the transition that settles which of them observed the close first.
	// Already terminal (a normal DISCONNECT, or a takeover that discarded it) is
	// a no-op. Spec5 [3.1.2.5].
	c.mqttOweWill(c.mqttSessionExpiry())
	asm.beforeTeardownLock(sess.id)

	// Add this session to the locked map for the rest of the execution.
	//
	// A failed acquisition is CLASSIFIED, never assumed: lockSession fails both
	// for a rebind (a takeover owns everything below) and for plain contention —
	// and an uncertain-write resolver now holds this gate across a JetStream
	// readback, which can outlast the bounded retry. Reading that as a takeover
	// silently dropped the whole teardown of a session that was still ours: no
	// detach, no persisted disconnect time, no expiry, no Will. So while this
	// connection is still the owner, contention parks the teardown when the
	// session is unresolved — exactly what winning the gate would have done, and
	// the resolver holding it discharges the parked action before releasing —
	// and otherwise simply keeps trying: every holder is bounded (a CONNECT
	// rebinds early, flipping the owner check; resolvers are bounded by their
	// JetStream timeouts), so the loop terminates.
	for {
		err := asm.lockSessionForTeardown(sess, c)
		if err == nil {
			break
		}
		sess.mu.Lock()
		stillOurs := sess.c == c
		sess.mu.Unlock()
		if !stillOurs {
			// The session now belongs to another connection, so none of the
			// teardown below is ours to do — but an owed Will still has to be
			// discharged, and nothing else will do it. See mqttPublishOwedWill.
			s.mqttPublishOwedWill(c, asm)
			return
		}
		// Still ours and possibly unresolved: the holder is (or is about to be)
		// a resolver. Park the obligations with their original deadlines, as the
		// gated path below would; resolution discharges them. Parking needs no
		// gate — it only writes session memory — but it must be ATOMIC with the
		// uncertainty check: the gate-holding resolver can clear the uncertainty
		// and run its final discharge scan at any instant, and an action parked
		// after that scan would be stranded forever. deferDisconnectIfUncertain
		// makes check and park one critical section; a refusal because the
		// session resolved meanwhile falls through to retaking the gate (the
		// resolver is finishing), with the Will restored for the ordinary
		// teardown that follows.
		if s.mqttDeferDisconnectEx(c, asm, sess, true) {
			return
		}
	}
	defer asm.unlockSession(sess)

	// An unresolved record write makes every teardown decision below unsafe: a
	// stale sess.seq would drive a second failing disconnect save, a clear() that
	// deletes the durables and the record before its CAS could fail, or a Will
	// takeover fence derived from a sequence nobody has classified. Record the MQTT
	// obligations with their ORIGINAL deadlines, detach the client, and let
	// resolution discharge them. Unresolved is NOT takeover, so nothing may be
	// dropped and nothing may be deleted. See sess_uncertain.go.
	if sess.isSeqUncertain() {
		s.mqttDeferDisconnect(c, asm, sess)
		return
	}

	// A NEVER-EXPIRING session makes no disconnect save below, so nothing else ever
	// records that this connection ended — and the delayed-Will record fence reads a
	// newer record with DisconnectedAt == 0 as a live newer connection and CANCELS a Will
	// this client is owed. Every later write on this session would publish exactly that
	// shape: the purge below, a void-verdict release handoff, a rejection probe.
	//
	// The RECORD FENCE is what makes the stamp effective, and `sess.mu` alone is not: an
	// off-readLoop writer (a register store's `persistVoidedRegs`, which takes the fence
	// without the ownership gate) can already have MARSHALLED `DisconnectedAt = 0` and be
	// mid-store, and that body landing after the origin sequence is captured below is a
	// newer record reading as connected. Holding the fence means every earlier marshal has
	// completed AND advanced `sess.seq` — so the capture below is at or past it — while
	// every later marshal needs this fence and therefore carries the stamp.
	//
	// Only for never-expire: the finite branch keeps its own in-transaction stamp, whose
	// save corrects the record immediately, and a clean close discards the record outright.
	sess.mu.Lock()
	neverExpire := sess.expiryInterval == mqttSessionNeverExpire
	sess.mu.Unlock()
	if neverExpire {
		sess.rejectionWindowHook(mqttRejectionWindowStamp)
		// NEVER the blocking form. Waiting here is what generated two defects rather than
		// one fix: the wait is exactly long enough for the writer being waited on to go
		// AMBIGUOUS — publishing seqUncertain before it releases, with its own immediate
		// resolver failing against the ownership gate this close holds — and long enough
		// (a JetStream timeout) that a park taken afterwards timestamps the close seconds
		// late, which can re-open an already-elapsed Will Delay for a reconnect to cancel.
		// A refusal does not skip the stamp — see the else branch, which stamps anyway,
		// unordered — it only gives up the ORDERING against a body already marshalled inside
		// an open transaction, when there is one. Same non-blocking reasoning as every other
		// off-path record write (see persistVoidedRegs). This is the ONE non-blocking site
		// that does not retry: it takes its degraded arm immediately, so waiter priority (see
		// sess_recordtx.go) gives that arm more occasions — including with the fence FREE but
		// reserved for an announced blocking writer. That arm is not cheaper: no transaction
		// is open at the refusal, but the prioritized writer can then take sess.mu, open its
		// transaction and marshal DisconnectedAt = 0 before this fallback stamps, which is
		// the same one-body residual reached from the other side. More occasions of a stated
		// residual, not a new behaviour; docs/CONFORMANCE.md carries it.
		if tok, ok := sess.tryBeginRecordTx(); ok {
			// Re-checked WITH THE FENCE HELD even so: acquiring it proves every earlier
			// marshal completed, but not that none of them armed uncertainty between the
			// entry check above and this instant. Stamping and detaching on a sequence
			// nobody has classified is what that check exists to withhold.
			unresolved := sess.isSeqUncertain()
			if !unresolved {
				sess.mu.Lock()
				sess.disconnectedAt = time.Now().Unix()
				sess.mu.Unlock()
			}
			if err := sess.endRecordTx(tok); err != nil {
				c.Errorf("MQTT: %v", err)
			}
			if unresolved {
				// Park after the release: the deferred discharge takes the fence itself.
				s.mqttDeferDisconnect(c, asm, sess)
				return
			}
		} else {
			// An unavailable fence still stamps, just without the ordering. Skipping it entirely
			// would leave the REJECTION PROBE — a writer this change adds, and the reason
			// the stamp exists at all — free to persist current memory with
			// DisconnectedAt = 0 at a newer sequence, which the Will fence reads as a
			// reconnect: the holder we could not order against can be rejected at the
			// unchanged base, arming the marker, and the probe then lands after the origin
			// capture. So every marshal from HERE on carries the disconnect time, and the
			// residual is one body this stamp could not order against, and which waiting
			// for would re-open the two defects the wait caused. On the OPEN-transaction
			// refusal that body was marshalled before the refusal; on the waiter-priority
			// refusal nothing is open yet, but the prioritized writer can open and marshal
			// in the interval before this stamp takes sess.mu, so the exposure is the same
			// one body rather than none.
			//
			// No uncertainty re-check on this arm: without the fence there is no sound one
			// to make, and an unsound sample acted on is check-then-act. The stamp is a
			// memory field, and every other teardown decision below already runs with that
			// same pre-existing exposure.
			sess.mu.Lock()
			sess.disconnectedAt = time.Now().Unix()
			sess.mu.Unlock()
		}
	}

	asm.mu.Lock()
	// Clear the client from the session, but session may stay.
	sess.mu.Lock()
	sess.c = nil
	// The Session Expiry Interval (set at CONNECT, possibly updated by a v5
	// DISCONNECT) is authoritative here: 0 means the session ends at connection
	// close. For 3.1.1 this is 0 iff the session is clean, so behavior is unchanged.
	expiry := sess.expiryInterval
	idHash := sess.idHash
	// Captured BEFORE the clear() below, which re-mints: a delayed Will this
	// connection enqueues belongs to the incarnation that is ending, and the
	// displaced-broker cancel scopes itself by exactly that. (The clean path never
	// enqueues one, but reading the generation after a re-mint would still be wrong.)
	sessGen := sess.gen
	sess.mu.Unlock()
	doClean := expiry == 0
	// If the session ends at connection close, then we remove from the account
	// manager, and we will call clear() outside of any lock.
	if doClean {
		asm.removeSession(sess, false)
	}
	// Remove in case it was in the flappers map.
	asm.removeSessFromFlappers(sess.id)
	asm.mu.Unlock()

	// This needs to be done outside of any lock.
	if doClean {
		if err := sess.clear(true); err != nil {
			// A LATE ambiguity — armed after the entry check the discard already passed —
			// deleted nothing, so the discard is recoverable: park it and restore the
			// residency removeSession took above. Without that the record, the consumers,
			// the shared group durables and this generation's registers survive with NO
			// resident object, and nothing reaps them — reconcileResidentRecordState and CONNECT
			// both scan as.sessions, and sweepSessionExpiries skips an ExpiryInterval == 0
			// record — so only a reconnect on this client ID ever would.
			//
			// The Will is NOT parked: mqttHandleWill below still runs and publishes it
			// immediately, exactly as it does today after a failed clear(). See
			// reparkRefusedDiscard.
			//
			// FALL THROUGH rather than return. The !doClean block is skipped anyway, and
			// this connection's Will is still owed.
			if !errors.Is(err, errMQTTClearRefusedUnresolved) || !s.reparkRefusedDiscard(asm, sess, time.Now().Unix()) {
				c.Errorf("%s", err.Error())
			}
		}
	} else {
		// Session survives the disconnect: drop pending QoS 1/2 state for shared
		// group consumers without acking, so a resume does not NAK-steal messages
		// that AckWait redelivers to other members (and so PIs are freed). Spec5 [4.8.2].
		sess.purgeSharedPending()
	}
	if !doClean && expiry != mqttSessionNeverExpire {
		// Finite Session Expiry Interval: keep the session (so a reconnect can
		// resume it) and arm a cleanup timer. Persist the disconnect time so the
		// deadline survives a restart; a failed save only loses restart-survival.
		// Spec5 [3.1.2.11.2].
		//
		// The transaction opens BEFORE the disconnect time is set, not around the
		// save alone: a transaction that begins after its own mutation cannot make
		// that mutation invisible to another writer. See sess_recordtx.go.
		tok := sess.beginRecordTx()
		sess.mu.Lock()
		sess.disconnectedAt = time.Now().Unix()
		sess.mu.Unlock()
		if err := sess.save(tok); err != nil {
			c.Debugf("Unable to persist session expiry deadline: %v", err)
		}
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
		asm.scheduleSessionExpiry(s, idHash, time.Duration(expiry)*time.Second)
	}

	// Capture the persisted session-record sequence as the delayed-Will takeover
	// fence baseline. For a finite-expiry session the disconnect save above bumped
	// it; for a never-expire session no disconnect save runs, so it is the last
	// prior save's sequence — still a valid baseline, since any reconnect/takeover
	// saves again with a strictly higher sequence. Captured here (post-save, before
	// any reconnect) so a later higher session sequence means reconnect/takeover or
	// a sweep, never an ordinary in-connection save. The epoch names the incarnation
	// so the fence only compares this sequence within it. See mqttWillOutbox.
	sess.mu.Lock()
	originSessionSeq := sess.seq
	sess.mu.Unlock()
	originSessEpoch := asm.sessStreamEpoch

	// Now handle the "will". This function will be a no-op if there is no "will" to
	// send. The effective Session Expiry Interval bounds any Will Delay.
	s.mqttHandleWill(c, expiry, sessGen, originSessionSeq, originSessEpoch)
}

// Updates the MaxAckPending for all MQTT sessions, updating the
// JetStream consumers and updating their max ack pending and forcing
// a expiration of pending messages.
//
// Runs from a server configuration reload routine.
// No lock held on entry.
func (s *Server) mqttUpdateMaxAckPending(newmaxp uint16) {
	msm := &s.mqtt.sessmgr
	s.accounts.Range(func(k, _ any) bool {
		accName := k.(string)
		msm.mu.RLock()
		asm := msm.sessions[accName]
		msm.mu.RUnlock()
		if asm == nil {
			// Move to next account
			return true
		}
		asm.mu.RLock()
		for _, sess := range asm.sessions {
			sess.mu.Lock()
			sess.maxp = newmaxp
			sess.mu.Unlock()
		}
		asm.mu.RUnlock()
		return true
	})
}

func (s *Server) mqttGetJSAForAccount(acc string) *mqttJSA {
	sm := &s.mqtt.sessmgr

	sm.mu.RLock()
	asm := sm.sessions[acc]
	sm.mu.RUnlock()

	if asm == nil {
		return nil
	}

	asm.mu.RLock()
	jsa := &asm.jsa
	asm.mu.RUnlock()
	return jsa
}

func (s *Server) mqttStoreQoSMsgForAccountOnNewSubject(hdr int, msg []byte, acc, subject string) {
	if s == nil || hdr <= 0 {
		return
	}
	h := mqttParsePublishNATSHeader(msg[:hdr])
	if h == nil || h.qos == 0 {
		return
	}
	jsa := s.mqttGetJSAForAccount(acc)
	if jsa == nil {
		return
	}
	jsa.storeMsg(mqttStreamSubjectPrefix+subject, hdr, msg)
}

func mqttParsePublishNATSHeader(headerBytes []byte) *mqttParsedPublishNATSHeader {
	if len(headerBytes) == 0 {
		return nil
	}

	pubValue := getHeader(mqttNatsHeader, headerBytes)
	if len(pubValue) == 0 {
		return nil
	}
	h := &mqttParsedPublishNATSHeader{
		qos:     pubValue[0] - '0',
		subject: getHeader(mqttNatsHeaderSubject, headerBytes),
		mapped:  getHeader(mqttNatsHeaderMapped, headerBytes),
		origin:  getHeader(mqttNatsHeaderOrigin, headerBytes),
	}
	// Retain As Published marker: the contract is exactly "1"; anything else
	// (absent, or a forged Nmqtt-Ret:0 / junk from a NATS publisher) is not
	// retained. Spec5 [3.8.3.1].
	if ret := getHeader(mqttNatsHeaderRetain, headerBytes); len(ret) == 1 && ret[0] == '1' {
		h.retained = true
	}
	// MQTT 5.0 properties block, carried base64-encoded. This header can come
	// from an untrusted source (a NATS publisher can set Nmqtt-Pub/Nmqtt-Props
	// on a message an MQTT client is subscribed to), so the decoded block is
	// re-validated before it may be forwarded verbatim into an outbound PUBLISH
	// (see mqttValidateForwardProps). Best effort: anything invalid is simply
	// dropped, never a delivery failure and never a malformed outbound packet.
	if enc := getHeader(mqttNatsHeaderProps, headerBytes); len(enc) > 0 {
		if raw, err := base64.StdEncoding.DecodeString(string(enc)); err == nil {
			h.props = mqttValidateForwardProps(raw)
		}
	}
	return h
}

// mqttValidateForwardProps returns raw only if it is a well-formed MQTT 5.0
// properties block that is safe to forward to a subscriber: it must parse
// cleanly with no trailing bytes and must not contain a Topic Alias (hop-by-hop,
// never forwarded) or a Subscription Identifier (per-hop and server-generated at
// delivery time; a publisher-supplied block must never smuggle one, so it is
// disallowed in the inbound PUBLISH context). Otherwise it returns nil so the
// message is delivered without properties. This protects against a crafted
// Nmqtt-Props header from a non-MQTT publisher making the server emit a malformed
// or forbidden PUBLISH.
func mqttValidateForwardProps(raw []byte) []byte {
	if len(raw) == 0 {
		return nil
	}
	r := &mqttReader{}
	r.reset(raw)
	props, err := r.readProperties(mqttPacketPub)
	if err != nil || r.hasMore() {
		return nil
	}
	// Subscription Identifier is already rejected by readProperties (not allowed
	// in the PUBLISH context); Topic Alias parses but must not be forwarded.
	if props != nil && props.present[mqttPropTopicAlias] {
		return nil
	}
	return raw
}

// mqttVarIntFromSlice decodes an MQTT variable byte integer from the front of b.
// It returns the value and the number of bytes consumed, or (0, -1) if b does
// not begin with a complete, valid varint. Spec5 [1.5.5].
func mqttVarIntFromSlice(b []byte) (int, int) {
	m, v := 1, 0
	for n := 0; n < len(b) && n < 4; n++ {
		d := b[n]
		v += int(d&0x7f) * m
		if d&0x80 == 0 {
			return v, n + 1
		}
		m *= 0x80
	}
	return 0, -1
}

// mqttPropValueOffset walks a well-formed MQTT 5.0 properties block (length
// prefix + body) and returns the byte offset of the value of the first
// occurrence of property target, or -1 if the block carries no such property or
// cannot be walked. The block is expected to have already been validated (by
// readProperties / mqttValidateForwardProps), so property lengths are trusted;
// anything unexpected returns -1 rather than risk a bad rewrite.
func mqttPropValueOffset(props []byte, target byte) int {
	plen, n := mqttVarIntFromSlice(props)
	if n < 0 {
		return -1
	}
	i, end := n, n+plen
	if end > len(props) {
		return -1
	}
	for i < end {
		prop := props[i]
		i++
		valueOff := i
		switch prop {
		case mqttPropPayloadFormat, mqttPropRequestProblemInfo, mqttPropRequestResponseInfo,
			mqttPropMaxQoS, mqttPropRetainAvailable, mqttPropWildcardSubAvailable,
			mqttPropSubIDAvailable, mqttPropSharedSubAvailable:
			i++
		case mqttPropServerKeepAlive, mqttPropReceiveMaximum, mqttPropTopicAliasMax,
			mqttPropTopicAlias:
			i += 2
		case mqttPropMessageExpiry, mqttPropSessionExpiry, mqttPropWillDelay, mqttPropMaxPacketSize:
			i += 4
		case mqttPropContentType, mqttPropResponseTopic, mqttPropAssignedClientID,
			mqttPropAuthMethod, mqttPropResponseInfo, mqttPropServerReference,
			mqttPropReasonString, mqttPropCorrelationData, mqttPropAuthData:
			if i+2 > end {
				return -1
			}
			i += 2 + int(binary.BigEndian.Uint16(props[i:]))
		case mqttPropUserProperty:
			// Two length-prefixed strings (key then value).
			for k := 0; k < 2; k++ {
				if i+2 > end {
					return -1
				}
				i += 2 + int(binary.BigEndian.Uint16(props[i:]))
			}
		case mqttPropSubscriptionID:
			_, m := mqttVarIntFromSlice(props[i:])
			if m < 0 {
				return -1
			}
			i += m
		default:
			return -1
		}
		if i > end {
			return -1
		}
		if prop == target {
			return valueOff
		}
	}
	return -1
}

// mqttMessageExpiry returns the Message Expiry Interval (seconds) carried in a
// raw MQTT 5.0 properties block and whether the property was present. The two
// differ semantically: an absent property means the message never expires,
// whereas an explicit value of 0 means it expires immediately. Spec5 [3.3.2.3.3].
func mqttMessageExpiry(props []byte) (uint32, bool) {
	off := mqttPropValueOffset(props, mqttPropMessageExpiry)
	if off < 0 {
		return 0, false
	}
	return binary.BigEndian.Uint32(props[off:]), true
}

// mqttMessageExpiryTTL returns the JetStream per-message TTL (seconds) to apply
// to a stored message given its properties: 0 when the message has no expiry
// (never reaped by TTL), otherwise at least 1 (JetStream's minimum). Flooring an
// explicit expiry of 0 to 1s ensures an "expire immediately" message is still
// reaped rather than kept forever; onward delivery is additionally gated by
// mqttForwardExpiry. Spec5 [3.3.2.3.3].
func mqttMessageExpiryTTL(props []byte) uint32 {
	if me, ok := mqttMessageExpiry(props); ok {
		if me < 1 {
			return 1
		}
		return me
	}
	return 0
}

// mqttSetMessageExpiry returns a copy of the raw MQTT 5.0 properties block with
// the Message Expiry Interval value replaced by rem. If the block carries no
// Message Expiry Interval, props is returned unchanged (no copy), preserving the
// verbatim fast path for messages that do not use expiry. Spec5 [3.3.2.3.3].
func mqttSetMessageExpiry(props []byte, rem uint32) []byte {
	off := mqttPropValueOffset(props, mqttPropMessageExpiry)
	if off < 0 {
		return props
	}
	out := make([]byte, len(props))
	copy(out, props)
	binary.BigEndian.PutUint32(out[off:], rem)
	return out
}

// mqttForwardExpiry computes the outbound properties for a stored v5 PUBLISH,
// honoring the MQTT 5.0 Message Expiry Interval given the message's JetStream
// store time (storeUnixNano). It returns the (possibly rewritten) properties and
// whether the message has already expired. Spec5 [3.3.2.3.3]: the PUBLISH sent
// onward MUST carry the interval reduced by the time waited in the server; if
// the interval has elapsed before onward delivery the server MUST NOT deliver
// the message and MUST drop its stored copy. Returns props unchanged when there
// is no expiry to adjust. Elapsed time is measured on this server's wall clock;
// as with JetStream per-message TTLs, accuracy across a cluster assumes
// synchronized clocks.
func mqttForwardExpiry(props []byte, storeUnixNano int64) (out []byte, expired bool) {
	off := mqttPropValueOffset(props, mqttPropMessageExpiry)
	if off < 0 {
		return props, false
	}
	orig := int64(binary.BigEndian.Uint32(props[off:]))
	// An explicit interval of 0 means the message expires immediately: it is
	// already expired for any onward (stored) delivery, whatever the elapsed
	// time. Checked before the elapsed/store-time guards so a same-second
	// delivery cannot forward it. Spec5 [3.3.2.3.3].
	if orig == 0 {
		return props, true
	}
	if storeUnixNano <= 0 {
		return props, false
	}
	elapsed := (time.Now().UnixNano() - storeUnixNano) / int64(time.Second)
	if elapsed <= 0 {
		return props, false
	}
	if elapsed >= orig {
		return props, true
	}
	out = make([]byte, len(props))
	copy(out, props)
	binary.BigEndian.PutUint32(out[off:], uint32(orig-elapsed))
	return out, false
}

// mqttInjectSubID returns a raw v5 properties block (variable-int length prefix
// + body) equal to props with a Subscription Identifier property (0x0B, varint
// id) appended to the body and the length prefix re-encoded, synthesizing a
// fresh block when props is empty. Offset-based rewrites (mqttSetMessageExpiry /
// mqttForwardExpiry) must run on the original block first; only a Topic Alias
// (mqttInjectTopicAlias) may be appended after this. props is returned unchanged
// when the id is out of range, the block is unsafe to forward (see
// mqttValidateForwardProps), or the injected block would exceed the maximum
// varint length. Spec5 [3.8.2.1.2].
func mqttInjectSubID(props []byte, id int) []byte {
	if id <= 0 || id > mqttMaxVarInt {
		return props
	}
	// The Subscription Identifier property: id 0x0B followed by its varint value.
	suffix := newMQTTWriter(5)
	suffix.WriteByte(mqttPropSubscriptionID)
	suffix.WriteVarInt(id)
	sb := suffix.Bytes()

	// An absent (nil) or empty (single 0 length byte) block: synthesize one that
	// carries just the identifier. Checked before validation so an empty input is
	// not mistaken for an invalid block.
	var body []byte
	if n := len(props); n > 1 {
		// A publisher-supplied block must be safe to forward verbatim before we
		// extend it (WritePublishHeader writes the result unchanged); a valid
		// length prefix can still wrap an invalid or unsafe body.
		if mqttValidateForwardProps(props) == nil {
			return props
		}
		plen, off := mqttVarIntFromSlice(props)
		if off < 0 || off+plen != n {
			return props
		}
		body = props[off:]
	}

	newLen := len(body) + len(sb)
	if newLen > mqttMaxVarInt {
		return props
	}
	w := newMQTTWriter(5 + newLen)
	w.WriteVarInt(newLen)
	w.Write(body)
	w.Write(sb)
	return w.Bytes()
}

// mqttInjectTopicAlias returns a raw v5 properties block (varint length prefix +
// body) equal to props with a Topic Alias property (0x23, 2-byte value) appended
// and the length prefix re-encoded, synthesizing a fresh block when props is
// nil/empty. ok is false (props returned unchanged) when the block cannot be
// safely extended; the caller must then send the full topic unaliased rather
// than an empty-topic frame with no alias. Unlike mqttInjectSubID it performs
// only the structural prefix/length check, NOT mqttValidateForwardProps: it runs
// after mqttInjectSubID, whose output legitimately carries a Subscription
// Identifier the forward validator rejects, and every block reaching here was
// already validated upstream. Caller precondition: props must not already carry
// a Topic Alias (this appends blindly) — inbound aliases are stripped before
// forwarding and mqttValidateForwardProps rejects NATS-carried blocks bearing
// one, so no current caller violates this. Spec5 [3.3.2.3.4].
func mqttInjectTopicAlias(props []byte, alias uint16) ([]byte, bool) {
	// The Topic Alias property: id 0x23 followed by its 2-byte value.
	sb := []byte{mqttPropTopicAlias, byte(alias >> 8), byte(alias)}

	// An absent (nil) or empty (single 0 length byte) block synthesizes one that
	// carries just the alias; a longer block must have a length prefix consistent
	// with its size before we extend it.
	var body []byte
	if n := len(props); n > 1 {
		plen, off := mqttVarIntFromSlice(props)
		if off < 0 || off+plen != n {
			return props, false
		}
		body = props[off:]
	}

	newLen := len(body) + len(sb)
	if newLen > mqttMaxVarInt {
		return props, false
	}
	w := newMQTTWriter(5 + newLen)
	w.WriteVarInt(newLen)
	w.Write(body)
	w.Write(sb)
	return w.Bytes(), true
}

// mqttStripTopicAlias returns a copy of the raw properties block (varint length
// prefix + body, the pp.props format) with the Topic Alias property (id +
// 2-byte value) removed and the length prefix re-encoded. It returns props
// unchanged when no alias is present, and nil when the alias was the only
// property (an empty block is never captured). Topic aliases are hop-local and
// MUST NOT be forwarded to subscribers or into retained storage. The result is
// always a fresh allocation, which doubles as the required copy out of the
// transient reader buffer. Spec5 [3.3.2.3.4].
func mqttStripTopicAlias(props []byte) []byte {
	off := mqttPropValueOffset(props, mqttPropTopicAlias)
	if off < 0 {
		return props
	}
	_, n := mqttVarIntFromSlice(props)
	// Drop the 3 alias bytes: the id byte at off-1 and its 2-byte value. The
	// body is everything after the length prefix minus those 3 bytes.
	newLen := len(props) - n - 3
	if newLen <= 0 {
		return nil
	}
	w := newMQTTWriter(5 + newLen)
	w.WriteVarInt(newLen)
	w.Write(props[n : off-1])
	w.Write(props[off+2:])
	return w.Bytes()
}

func mqttParsePubRelNATSHeader(headerBytes []byte) uint16 {
	if len(headerBytes) == 0 {
		return 0
	}

	pubrelValue := getHeader(mqttNatsPubRelHeader, headerBytes)
	if len(pubrelValue) == 0 {
		return 0
	}
	pi, _ := strconv.ParseUint(string(pubrelValue), 10, 16)
	return uint16(pi)
}

// Returns the MQTT sessions manager for a given account.
// If new, creates the required JetStream streams/consumers
// for handling of sessions and messages.
func (s *Server) getOrCreateMQTTAccountSessionManager(c *client) (*mqttAccountSessionManager, error) {
	sm := &s.mqtt.sessmgr

	c.mu.Lock()
	acc := c.acc
	c.mu.Unlock()
	accName := acc.GetName()

	sm.mu.RLock()
	asm, ok := sm.sessions[accName]
	sm.mu.RUnlock()

	if ok {
		return asm, nil
	}

	// We will pass the quitCh to the account session manager if we happen to create it.
	s.mu.Lock()
	quitCh := s.quitCh
	s.mu.Unlock()

	// Not found, now take the write lock and check again
	sm.mu.Lock()
	defer sm.mu.Unlock()
	asm, ok = sm.sessions[accName]
	if ok {
		return asm, nil
	}
	// Need to create one here.
	asm, err := s.mqttCreateAccountSessionManager(acc, quitCh)
	if err != nil {
		return nil, err
	}
	sm.sessions[accName] = asm
	return asm, nil
}

// Creates JS streams/consumers for handling of sessions and messages for this account.
//
// Global session manager lock is held on entry.
func (s *Server) mqttCreateAccountSessionManager(acc *Account, quitCh chan struct{}) (*mqttAccountSessionManager, error) {
	accName := acc.GetName()

	opts := s.getOpts()
	c := s.createInternalAccountClient()
	c.acc = acc

	id := s.NodeName()

	mqttJSAPITimeout := opts.MQTT.JSAPITimeout
	if mqttJSAPITimeout == 0 {
		mqttJSAPITimeout = mqttDefaultJSAPITimeout
	}

	replicas := opts.MQTT.StreamReplicas
	if replicas <= 0 {
		replicas = s.mqttDetermineReplicas()
	}
	qname := fmt.Sprintf("[ACC:%s] MQTT ", accName)
	as := &mqttAccountSessionManager{
		sessions:        make(map[string]*mqttSession),
		sessByHash:      make(map[string]*mqttSession),
		sessLocked:      make(map[string]struct{}),
		flappers:        make(map[string]time.Time),
		pendingExpiries: make(map[string]*time.Timer),
		reconcileWM:     make(map[string]*sessionReconcileState),
		jsa: mqttJSA{
			id:      id,
			c:       c,
			rplyr:   mqttJSARepliesPrefix + id + ".",
			sendq:   newIPQueue[*mqttJSPubMsg](s, qname+"send"),
			nuid:    nuid.New(),
			quitCh:  quitCh,
			timeout: mqttJSAPITimeout,
		},
		rmsCache: &sync.Map{},
	}
	// JetStream domain. Unlike the NATS Server this code came from, the broker is
	// a plain client of exactly one external JetStream, so there is a single
	// source of truth: -js-domain (MQTTNB_JS_DOMAIN). There are no leafnode or
	// per-account topologies to resolve between. Empty means the domain of the
	// server we are connected to, which is the common case.
	//
	// The value is validated at startup (config.Validate) as a single subject
	// token, because it is interpolated both into the JS API subjects
	// ($JS.<domain>.API.*, see prefixDomain) and into the session subject prefix
	// used to store sessions in the $MQTT_sess stream.
	as.jsa.domain = opts.MQTT.JsDomain
	if as.jsa.domain != _EMPTY_ {
		as.domainTk = as.jsa.domain + "."
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q in domain %q", replicas, accName, as.jsa.domain)
	} else {
		s.Noticef("Creating MQTT streams/consumers with replicas %v for account %q", replicas, accName)
	}

	var subs []*subscription
	var success bool
	closeCh := make(chan struct{})

	defer func() {
		if success {
			return
		}
		for _, sub := range subs {
			c.processUnsub(sub.sid)
		}
		close(closeCh)
	}()

	// We create all subscriptions before starting the go routine that will do
	// sends otherwise we could get races.
	// Note that using two different clients (one for the subs, one for the
	// sends) would cause other issues such as registration of recent subs in
	// the "sub" client would be invisible to the check for GW routed replies
	// (shouldMapReplyForGatewaySend) since the client there would be the "sender".

	jsa := &as.jsa
	sid := int64(1)
	// This is a subscription that will process all JS API replies. We could split to
	// individual subscriptions if needed, but since there is a bit of common code,
	// that seemed like a good idea to be all in one place.
	if err := as.createSubscription(jsa.rplyr+">",
		as.processJSAPIReplies, &sid, &subs); err != nil {
		return nil, err
	}

	// We create the subscription on "$MQTT.sub.<nuid>" to limit the subjects
	// that a user would allow permissions on.
	rmsubj := mqttSubPrefix + nuid.Next()
	if err := as.createSubscription(rmsubj, as.processRetainedMsg, &sid, &subs); err != nil {
		return nil, err
	}

	// The session-record watch's delivery subject. Every session-record write in the
	// account arrives here, which is how this broker learns that a peer took one of
	// its sessions over — the at-most-once `$MQTT.JSA.*.SP.*.*` notification this
	// replaces could simply be lost, stranding a displaced client indefinitely.
	// Under "$MQTT.sub." and on the SERVICE connection, like the retained consumer,
	// so the recommended client ACL denying "$MQTT.>" stays correct.
	// See sess_record_watch.go.
	srwsubj := mqttSubPrefix + nuid.Next()
	if err := as.createSubscription(srwsubj, as.processSessionRecordDelivery, &sid, &subs); err != nil {
		return nil, err
	}

	// No more creation of subscriptions past this point otherwise RACEs may happen.

	// Start the go routine that will send JS API requests.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.sendJSAPIrequests(s, c, accName, closeCh)
	})

	// Start the go routine that will clean up cached retained messages that expired.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.cleanupRetainedMessageCache(s, closeCh)
	})

	// Provision the nine broker-owned $MQTT_* streams through the one
	// convergence shape (ensureBrokerStream): create-or-lose, an UNCONDITIONAL
	// authoritative re-read, then per-owner validation over that re-read — a
	// nonconforming stream is a startup failure with recreate guidance, never
	// something to mutate in place. The ordered spec table is the only route: a
	// stream cannot be provisioned outside the helper without deleting its entry,
	// which the table-coverage test refuses. Per-stream specials (the session
	// epoch generation, gate hooks and adoption; the retained-messages consumer)
	// live in the entries, not around this loop.
	wantReplicas := 0
	if opts.MQTT.StreamReplicas != 0 {
		wantReplicas = replicas
	}
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	// A max_protocol_version:4 broker refuses every MQTT 5 CONNECT, so it never
	// emits a per-message Nats-TTL (Message Expiry) and must not require the
	// capability of a pre-existing stream. Everything else accepts MQTT 5.0.
	requireMsgTTL := opts.MQTT.MaxProtocolVersion != mqttProtoLevel
	// Recorded by the $MQTT_rmsgs post step so a later failure can roll it back: the
	// two per-broker ephemeral consumers are provisioned as one unit.
	var retainedConsumerName string
	for _, spec := range as.brokerStreamSpecs(s, accName, replicas, wantReplicas, maxAckPending, requireMsgTTL, rmsubj, &retainedConsumerName) {
		si, err := ensureBrokerStream(jsa, spec.cfg, spec.validate, spec.preCreate, spec.observe)
		if err != nil {
			as.rollbackEphemeralConsumers(retainedConsumerName, _EMPTY_)
			return nil, fmt.Errorf("MQTT %s stream for account %q: %w", spec.txt, accName, err)
		}
		if spec.post != nil {
			if err := spec.post(si); err != nil {
				// A post step that failed may still have created its consumer — the
				// retained one records its name before issuing the request precisely
				// so this path can remove it. Without this the loop's own failures
				// leak exactly what the post-loop rollback was added to prevent.
				as.rollbackEphemeralConsumers(retainedConsumerName, _EMPTY_)
				return nil, err
			}
		}
	}

	// The session-record watch, created only once EVERY stream has provisioned and
	// validated, and rolling the retained-messages consumer back if it fails: an
	// ephemeral consumer left behind by a failed attempt is deleted by nothing, and
	// every connection retries this whole function. See
	// mqttProvisionEphemeralConsumers.
	if err := as.mqttProvisionEphemeralConsumers(accName, srwsubj, retainedConsumerName); err != nil {
		return nil, err
	}

	// Re-arm session-expiry timers, without delaying the connecting client. Then
	// re-sweep periodically: expiries catch deadlines that never got a timer (e.g.
	// records orphaned when their owner crashed while the client was connected),
	// and the orphaned-consumer sweep reconciles durables leaked in crash windows
	// (re-run so it also catches orphans skipped by the grace window at startup).
	// reconcileResidentRecordState rides along here but is deliberately NOT part of
	// sweepSessionExpiries: that walks persisted records and skips ExpiryInterval 0
	// or never-expire, which are exactly the two cases that park a deferred clear()
	// or a deferred Will. See sess_uncertain.go.
	s.startGoRoutine(func() {
		defer s.grWG.Done()
		as.sweepSessionExpiries(s)
		as.sweepOrphanedConsumers(s)
		as.sweepOrphanedQoS2State(s)
		tt := time.NewTicker(mqttSessionExpirySweepInterval())
		defer tt.Stop()
		for {
			select {
			case <-tt.C:
				as.reconcileResidentRecordState(s)
				as.sweepSessionExpiries(s)
				as.sweepOrphanedConsumers(s)
				as.sweepOrphanedQoS2State(s)
			case <-closeCh:
				return
			case <-s.quitCh:
				return
			}
		}
	})

	// Run the delayed-Will claim loop on its own cadence. Tracked by willClaimWG
	// (not grWG) so shutdown Phase 2 drains it before waiting on publisher workers.
	s.willClaimWG.Add(1)
	go func() {
		as.willClaimLoop(s, closeCh)
	}()

	// Set this so that on defer we don't cleanup.
	success = true

	return as, nil
}

func (s *Server) mqttDetermineReplicas() int {
	// If not clustered, then replica will be 1.
	if !s.JetStreamIsClustered() {
		return 1
	}
	opts := s.getOpts()
	replicas := 0
	for _, u := range opts.Routes {
		host := u.Hostname()
		// If this is an IP just add one.
		if net.ParseIP(host) != nil {
			replicas++
		} else {
			addrs, _ := net.LookupHost(host)
			replicas += len(addrs)
		}
	}
	if replicas < 1 {
		replicas = 1
	} else if replicas > 3 {
		replicas = 3
	}
	return replicas
}

//////////////////////////////////////////////////////////////////////////////
//
// JS APIs related functions
//
//////////////////////////////////////////////////////////////////////////////

func (jsa *mqttJSA) newRequest(kind, subject string, hdr int, msg []byte) (any, error) {
	return jsa.newRequestEx(kind, subject, _EMPTY_, hdr, msg)
}

func (jsa *mqttJSA) prefixDomain(subject string) string {
	if jsa.domain != _EMPTY_ {
		// rewrite js api prefix with domain
		if sub := strings.TrimPrefix(subject, JSApiPrefix+"."); sub != subject {
			subject = fmt.Sprintf("$JS.%s.API.%s", jsa.domain, sub)
		}
	}
	return subject
}

func (jsa *mqttJSA) newRequestEx(kind, subject, cidHash string, hdr int, msg []byte) (any, error) {
	responses, err := jsa.newRequestExMulti(kind, subject, cidHash, []int{hdr}, [][]byte{msg})
	if err != nil {
		return nil, err
	}
	if len(responses) != 1 {
		return nil, fmt.Errorf("unreachable: invalid number of responses (%d)", len(responses))
	}
	return responses[0].value, nil
}

// newRequestExMulti sends multiple messages on the same subject and waits for
// all responses. It returns the same number of responses in the same order as
// msgs parameter. In case of a timeout it returns an error as well as all
// responses received as a sparsely populated array, matching msgs, with nils
// for the values that have not yet been received.
//
// Note that each response may represent an error and should be inspected as
// such by the caller.
func (jsa *mqttJSA) newRequestExMulti(kind, subject, cidHash string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	subjects := make([]string, len(msgs))
	for i := range subjects {
		subjects[i] = subject
	}
	return jsa.newRequestExMultiSubjectsCID(kind, cidHash, subjects, hdrs, msgs)
}

// newRequestExMultiSubjects is newRequestExMulti with a PER-REQUEST subject, which a
// batch of consumer-info lookups needs: each names its own consumer in the subject rather
// than in a body. Same pipelining, same all-or-timeout semantics; a nil response entry
// means that one request went unanswered.
func (jsa *mqttJSA) newRequestExMultiSubjects(kind string, subjects []string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	return jsa.newRequestExMultiSubjectsCID(kind, _EMPTY_, subjects, hdrs, msgs)
}

func (jsa *mqttJSA) newRequestExMultiSubjectsCID(kind, cidHash string, subjects []string, hdrs []int, msgs [][]byte) ([]*mqttJSAResponse, error) {
	if len(hdrs) != len(msgs) || len(subjects) != len(msgs) {
		return nil, fmt.Errorf("unreachable: invalid number of messages (%d), subjects (%d) or header offsets (%d)",
			len(msgs), len(subjects), len(hdrs))
	}
	responseCh := make(chan *mqttJSAResponse, len(msgs))

	// Generate and queue all outgoing requests, have all results reported to
	// responseCh, and store a map of reply subjects to the original subjects'
	// indices.
	r2i := map[string]int{}
	for i, msg := range msgs {
		hdr := hdrs[i]
		var sb strings.Builder
		// Either we use nuid.Next() which uses a global lock, or our own nuid object, but
		// then it needs to be "write" protected. This approach will reduce across account
		// contention since we won't use the global nuid's lock.
		jsa.mu.Lock()
		uid := jsa.nuid.Next()
		sb.WriteString(jsa.rplyr)
		jsa.mu.Unlock()

		sb.WriteString(kind)
		sb.WriteByte(btsep)
		if cidHash != _EMPTY_ {
			sb.WriteString(cidHash)
			sb.WriteByte(btsep)
		}
		sb.WriteString(uid)
		reply := sb.String()

		// Add responseCh to the reply channel map. It will be cleaned out on
		// timeout (see below), or in processJSAPIReplies upon receiving the
		// response.
		jsa.replies.Store(reply, responseCh)

		jsa.sendq.push(&mqttJSPubMsg{
			subj:  jsa.prefixDomain(subjects[i]),
			reply: reply,
			hdr:   hdr,
			msg:   msg,
		})
		r2i[reply] = i
	}

	// Wait for all responses to come back, or for the timeout to expire. We
	// don't want to use time.After() which causes memory growth because the
	// timer can't be stopped and will need to expire to then be garbage
	// collected.
	c := 0
	responses := make([]*mqttJSAResponse, len(msgs))
	start := time.Now()
	t := time.NewTimer(jsa.timeout)
	defer t.Stop()
	for {
		select {
		case r := <-responseCh:
			i := r2i[r.reply]
			responses[i] = r
			c++
			if c == len(msgs) {
				return responses, nil
			}

		case <-jsa.quitCh:
			return nil, ErrServerNotRunning

		case <-t.C:
			var reply string
			now := time.Now()
			for reply = range r2i { // preserve the last value for Errorf
				jsa.replies.Delete(reply)
			}

			var first string
			if len(subjects) > 0 {
				first = subjects[0]
			}
			if len(msgs) == 1 {
				return responses, fmt.Errorf("timeout after %v: request type %q on %q (reply=%q)", now.Sub(start), kind, first, reply)
			} else {
				return responses, fmt.Errorf("timeout after %v: request type %q on %q: got %d out of %d", now.Sub(start), kind, first, c, len(msgs))
			}
		}
	}
}

func (jsa *mqttJSA) sendAck(ackSubject string) {
	// Send to the ack subject with no payload.
	jsa.sendMsg(ackSubject, nil)
}

// confirmAck acknowledges a delivery and WAITS for JetStream to confirm the ack
// was applied (the JetStream "double ack"): the ack is published with a reply
// subject, and JetStream answers on it once the consumer state has advanced.
//
// sendAck above is fire-and-forget, which is the right trade almost everywhere:
// a lost ack costs one redelivery. It is the WRONG trade wherever a durable
// record is deleted because "the message was acked" — a lost ack would then
// leave the delivery re-offerable with the state that governs it already gone.
// Only such call sites should use this; it costs a JS API round trip.
func (jsa *mqttJSA) confirmAck(ackSubject string) error {
	if ackSubject == _EMPTY_ {
		return fmt.Errorf("no ack subject")
	}
	// hdr -1: an ack is not a JS API request and must carry no client-info header.
	_, err := jsa.newRequest(mqttJSAAckConfirm, ackSubject, -1, AckAck)
	return err
}

func (jsa *mqttJSA) sendMsg(subj string, msg []byte) {
	if subj == _EMPTY_ {
		return
	}
	// We pass -1 for the hdr so that the send loop does not need to
	// add the "client info" header. This is not a JS API request per se.
	jsa.sendq.push(&mqttJSPubMsg{subj: subj, msg: msg, hdr: -1})
}

func (jsa *mqttJSA) createEphemeralConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiConsumerCreateT, cfg.Stream)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

func (jsa *mqttJSA) createDurableConsumer(cfg *CreateConsumerRequest) (*JSApiConsumerCreateResponse, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	subj := fmt.Sprintf(JSApiDurableCreateT, cfg.Stream, cfg.Config.Durable)
	ccri, err := jsa.newRequest(mqttJSAConsumerCreate, subj, 0, cfgb)
	if err != nil {
		return nil, err
	}
	ccr := ccri.(*JSApiConsumerCreateResponse)
	return ccr, ccr.ToError()
}

// if noWait is specified, does not wait for the JS response, returns nil
func (jsa *mqttJSA) deleteConsumer(streamName, consName string, noWait bool) (*JSApiConsumerDeleteResponse, error) {
	subj := fmt.Sprintf(JSApiConsumerDeleteT, streamName, consName)
	if noWait {
		jsa.sendMsg(subj, nil)
		return nil, nil
	}

	cdri, err := jsa.newRequest(mqttJSAConsumerDel, subj, 0, nil)
	if err != nil {
		return nil, err
	}
	cdr := cdri.(*JSApiConsumerDeleteResponse)
	return cdr, cdr.ToError()
}

func (jsa *mqttJSA) createStream(cfg *StreamConfig) (*StreamInfo, bool, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, false, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamCreate, fmt.Sprintf(JSApiStreamCreateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, false, err
	}
	scr := scri.(*JSApiStreamCreateResponse)
	return scr.StreamInfo, scr.DidCreate, scr.ToError()
}

func (jsa *mqttJSA) updateStream(cfg *StreamConfig) (*StreamInfo, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamUpdate, fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, err
	}
	scr := scri.(*JSApiStreamUpdateResponse)
	return scr.StreamInfo, scr.ToError()
}

func (jsa *mqttJSA) lookupStream(name string) (*StreamInfo, error) {
	slri, err := jsa.newRequest(mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, name), 0, nil)
	if err != nil {
		return nil, err
	}
	slr := slri.(*JSApiStreamInfoResponse)
	return slr.StreamInfo, slr.ToError()
}

// streamSubjects returns the subjects in streamName matching filter, mapped to
// their message counts, via STREAM.INFO's subjects_filter. It reads no message
// bodies at all, which is the whole reason the release claim lives on a sibling
// subject: the discard has to enumerate a session's inbound QoS 2 state without
// pulling every held payload (up to max_payload each) across the JS API on a
// disconnect path.
//
// A nil map means the filter matched nothing. Callers must keep their filter
// narrow enough that the response cannot be paged: JetStream truncates the list
// at JSMaxSubjectDetails and reports the remainder only through the paging
// fields, which this deliberately does not follow (see mqttQoS2HoldWalkFilter,
// whose subject space is bounded by the 16-bit packet identifier).
func (jsa *mqttJSA) streamSubjects(streamName, filter string) (map[string]uint64, error) {
	req, err := json.Marshal(&JSApiStreamInfoRequest{SubjectsFilter: filter})
	if err != nil {
		return nil, err
	}
	slri, err := jsa.newRequest(mqttJSAStreamLookup, fmt.Sprintf(JSApiStreamInfoT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	slr := slri.(*JSApiStreamInfoResponse)
	if err := slr.ToError(); err != nil {
		return nil, err
	}
	if slr.StreamInfo == nil {
		return nil, fmt.Errorf("stream info for %q unavailable", streamName)
	}
	return slr.StreamInfo.State.Subjects, nil
}

// purgeStream removes all messages in streamName matching filter (a subject,
// possibly wildcarded). If noWait, the request is fired without waiting for a
// response (like deleteConsumer/deleteMsg's no-wait paths).
func (jsa *mqttJSA) purgeStream(streamName, filter string, noWait bool) error {
	preq := JSApiStreamPurgeRequest{Subject: filter}
	req, _ := json.Marshal(preq)
	subj := jsa.prefixDomain(fmt.Sprintf(JSApiStreamPurgeT, streamName))
	if noWait {
		jsa.sendq.push(&mqttJSPubMsg{
			subj: subj,
			msg:  req,
		})
		return nil
	}
	spri, err := jsa.newRequest(mqttJSAStreamPurge, subj, 0, req)
	if err != nil {
		return err
	}
	spr := spri.(*JSApiStreamPurgeResponse)
	return spr.ToError()
}

// lookupConsumer returns the ConsumerInfo for one durable, or an error. A
// JSConsumerNotFoundErr is an AUTHORITATIVE absence and is returned as such by
// the callers that need to tell "gone" from "could not tell"; every other error
// is indeterminate.
func (jsa *mqttJSA) lookupConsumer(streamName, consName string) (*ConsumerInfo, error) {
	cii, err := jsa.newRequest(mqttJSAConsumerLookup, fmt.Sprintf(JSApiConsumerInfoT, streamName, consName), 0, nil)
	if err != nil {
		return nil, err
	}
	cir := cii.(*JSApiConsumerInfoResponse)
	if err := cir.ToError(); err != nil {
		return nil, err
	}
	return cir.ConsumerInfo, nil
}

// consumerList returns ConsumerInfo for every consumer of the stream, walking
// the paged CONSUMER.LIST API to completion.
// lookupConsumersMulti fetches several consumers' info in ONE pipelined batch, in the
// order given. Each entry is nil where that consumer does not exist; ok is false for any
// request whose response could not be read, so a caller can tell "gone" from "unknown"
// — a distinction the resume path turns into a delivery decision.
//
// The serial sibling above is fine for one lookup. This exists because the resume barrier
// needs one per DURABLE before the CONNACK, and a session with a few thousand
// subscriptions would otherwise pay a few thousand serial round trips to reconnect.
func (jsa *mqttJSA) lookupConsumersMulti(streamName string, durables []string) (infos []*ConsumerInfo, ok []bool, err error) {
	subjects := make([]string, 0, len(durables))
	hdrs := make([]int, 0, len(durables))
	msgs := make([][]byte, 0, len(durables))
	for _, dur := range durables {
		subjects = append(subjects, fmt.Sprintf(JSApiConsumerInfoT, streamName, dur))
		hdrs = append(hdrs, 0)
		msgs = append(msgs, nil)
	}
	all, err := jsa.newRequestExMultiSubjects(mqttJSAConsumerLookup, subjects, hdrs, msgs)
	infos = make([]*ConsumerInfo, len(durables))
	ok = make([]bool, len(durables))
	for i, v := range all {
		if i >= len(durables) || v == nil {
			continue // no response: not ok, so the caller treats it as indeterminate
		}
		cir, isCIR := v.value.(*JSApiConsumerInfoResponse)
		if !isCIR {
			continue
		}
		if aerr := cir.ToError(); aerr != nil {
			// Only "consumer not found" proves absence; anything else is a failed read.
			if isErrorOtherThan(aerr, JSConsumerNotFoundErr) {
				continue
			}
			ok[i] = true // absent, authoritatively
			continue
		}
		infos[i], ok[i] = cir.ConsumerInfo, true
	}
	return infos, ok, err
}

func (jsa *mqttJSA) consumerList(streamName string) ([]*ConsumerInfo, error) {
	var all []*ConsumerInfo
	for {
		req, err := json.Marshal(&JSApiConsumersRequest{ApiPagedRequest: ApiPagedRequest{Offset: len(all)}})
		if err != nil {
			return nil, err
		}
		cli, err := jsa.newRequest(mqttJSAConsumerList, fmt.Sprintf(JSApiConsumerListT, streamName), 0, req)
		if err != nil {
			return nil, err
		}
		clr := cli.(*JSApiConsumerListResponse)
		if clr.Error != nil {
			return nil, clr.ToError()
		}
		all = append(all, clr.Consumers...)
		if len(all) >= clr.Total || len(clr.Consumers) == 0 {
			return all, nil
		}
	}
}

func (jsa *mqttJSA) loadLastMsgFor(streamName string, subject string) (*StoredMsg, error) {
	if h := mqttTestLoadLastMsgHookV.Load(); h != nil {
		if err := (*h)(streamName, subject); err != nil {
			return nil, err
		}
	}
	mreq := &JSApiMsgGetRequest{LastFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

// loadMsgsMulti loads messages by EXACT sequence in one pipelined batch, preserving the
// order of seqs in the result. The by-subject sibling below cannot serve this: a source
// delivery is identified by its stream sequence, not by a subject.
//
// Used by the resume barrier, where the alternative is one synchronous round trip per
// in-flight delivery before the CONNACK. A nil entry means that request produced no
// response, which the caller must treat as indeterminate rather than as absence.
func (jsa *mqttJSA) loadMsgsMulti(streamName string, seqs []uint64) ([]*JSApiMsgGetResponse, error) {
	marshaled := make([][]byte, 0, len(seqs))
	headerBytes := make([]int, 0, len(seqs))
	for _, seq := range seqs {
		bb, err := json.Marshal(&JSApiMsgGetRequest{Seq: seq})
		if err != nil {
			return nil, err
		}
		marshaled = append(marshaled, bb)
		headerBytes = append(headerBytes, 0)
	}
	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), _EMPTY_, headerBytes, marshaled)
	responses := make([]*JSApiMsgGetResponse, len(all))
	for i, v := range all {
		if v != nil {
			responses[i] = v.value.(*JSApiMsgGetResponse)
		}
	}
	return responses, err
}

func (jsa *mqttJSA) loadLastMsgForMulti(streamName string, subjects []string) ([]*JSApiMsgGetResponse, error) {
	marshaled := make([][]byte, 0, len(subjects))
	headerBytes := make([]int, 0, len(subjects))
	for _, subject := range subjects {
		mreq := &JSApiMsgGetRequest{LastFor: subject}
		bb, err := json.Marshal(mreq)
		if err != nil {
			return nil, err
		}
		marshaled = append(marshaled, bb)
		headerBytes = append(headerBytes, 0)
	}

	all, err := jsa.newRequestExMulti(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), _EMPTY_, headerBytes, marshaled)
	// all has the same order as subjects, preserve it as we unmarshal
	responses := make([]*JSApiMsgGetResponse, len(all))
	for i, v := range all {
		if v != nil {
			responses[i] = v.value.(*JSApiMsgGetResponse)
		}
	}
	return responses, err
}

func (jsa *mqttJSA) loadNextMsgFor(streamName string, subject string) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{NextFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

// loadNextMsgFromSeq returns the first message on the given subject (which may
// contain wildcards) with a sequence >= seq, allowing a bounded walk over all
// messages of a subject filter.
func (jsa *mqttJSA) loadNextMsgFromSeq(streamName, subject string, seq uint64) (*StoredMsg, error) {
	if h := mqttTestLoadNextMsgHookV.Load(); h != nil {
		if err := (*h)(streamName, subject, seq); err != nil {
			return nil, err
		}
	}
	mreq := &JSApiMsgGetRequest{Seq: seq, NextFor: subject}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

func (jsa *mqttJSA) loadMsg(streamName string, seq uint64) (*StoredMsg, error) {
	mreq := &JSApiMsgGetRequest{Seq: seq}
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}
	lmri, err := jsa.newRequest(mqttJSAMsgLoad, fmt.Sprintf(JSApiMsgGetT, streamName), 0, req)
	if err != nil {
		return nil, err
	}
	lmr := lmri.(*JSApiMsgGetResponse)
	return lmr.Message, lmr.ToError()
}

func (jsa *mqttJSA) storeMsgNoWait(subject string, hdrLen int, msg []byte) {
	jsa.sendq.push(&mqttJSPubMsg{
		subj: subject,
		msg:  msg,
		hdr:  hdrLen,
	})
}

func (jsa *mqttJSA) storeMsg(subject string, headers int, msg []byte) (*JSPubAckResponse, error) {
	if h := mqttTestStoreMsgHookV.Load(); h != nil {
		if err := (*h)(subject, headers, msg); err != nil {
			return nil, err
		}
	}
	smri, err := jsa.newRequest(mqttJSAMsgStore, subject, headers, msg)
	if err != nil {
		return jsa.storeMsgResult(subject, nil, err)
	}
	smr := smri.(*JSPubAckResponse)
	return jsa.storeMsgResult(subject, smr, smr.ToError())
}

// storeMsgResult applies the post-store fault seam. It exists because the
// pre-store hook above models the wrong half of the interesting failure: a store
// that never happened leaves nothing behind, whereas the case that actually
// wedges state is a store that LANDED (and may already have been delivered) whose
// response the caller never saw. Only a post-store seam can produce that.
func (jsa *mqttJSA) storeMsgResult(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
	if h := mqttTestStoreMsgResultHookV.Load(); h != nil {
		return (*h)(subject, resp, err)
	}
	return resp, err
}

func (jsa *mqttJSA) storeSessionMsg(domainTk, cidHash string, hdr int, msg []byte) (*JSPubAckResponse, error) {
	if mqttTestSessStoreSuppressV.Load() {
		// Test seam: the request never reaches the stream, and the caller sees an
		// error it cannot classify. This is the "issued but did not land" half of
		// ambiguity — the result hook models the other half (landed, response lost) —
		// and the two must be distinguishable, because the reconciliation the
		// readback owes differs completely between them.
		return nil, errors.New("mqtt test: session record store suppressed")
	}
	// Compute subject where the session is being stored
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash

	// Passing cidHash will add it to the JS reply subject, so that we can use
	// it in the JS API reply handler.
	smri, err := jsa.newRequestEx(mqttJSASessPersist, subject, cidHash, hdr, msg)
	if err != nil {
		return nil, err
	}
	smr := smri.(*JSPubAckResponse)
	return smr, smr.ToError()
}

func (jsa *mqttJSA) loadSessionMsg(domainTk, cidHash string) (*StoredMsg, error) {
	subject := mqttSessStreamSubjectPrefix + domainTk + cidHash
	return jsa.loadLastMsgFor(mqttSessStreamName, subject)
}

func (jsa *mqttJSA) deleteMsg(stream string, seq uint64, wait bool) error {
	if h := mqttTestDeleteMsgHookV.Load(); h != nil {
		if err := (*h)(stream, seq); err != nil {
			return err
		}
	}
	dreq := JSApiMsgDeleteRequest{Seq: seq, NoErase: true}
	req, _ := json.Marshal(dreq)
	subj := jsa.prefixDomain(fmt.Sprintf(JSApiMsgDeleteT, stream))
	if !wait {
		jsa.sendq.push(&mqttJSPubMsg{
			subj: subj,
			msg:  req,
		})
		return nil
	}
	dmi, err := jsa.newRequest(mqttJSAMsgDelete, subj, 0, req)
	if err != nil {
		return err
	}
	dm := dmi.(*JSApiMsgDeleteResponse)
	return dm.ToError()
}

//////////////////////////////////////////////////////////////////////////////
//
// Account Sessions Manager related functions
//
//////////////////////////////////////////////////////////////////////////////

// Returns true if `err` is not nil and does not match the api error with ErrorIdentifier id
func isErrorOtherThan(err error, id ErrorIdentifier) bool {
	return err != nil && !IsNatsErr(err, id)
}

// jsAPIError extracts the JetStream API error from err, seeing through %w
// wrapping. IsNatsErr does a bare type assertion and therefore does not.
func jsAPIError(err error) *ApiError {
	var ae *ApiError
	if errors.As(err, &ae) {
		return ae
	}
	return nil
}

// isJSErrCausedBy reports whether err is the JetStream API error identified by
// id whose interpolated cause is `cause`.
//
// A few JetStream API errors are generic containers: JSStreamStoreFailedF
// (10077) and JSStreamMsgDeleteFailedF (10057) both have Description "{err}",
// so the only record of what actually went wrong is the cause flattened into
// ApiError.Description. The error identity is matched structurally on ErrCode;
// the cause then needs a text comparison, which nats-server gives no way to
// avoid. Confining it here is the point of this helper: comparing the RENDERED
// ApiError.Error() ("<description> (<code>)") instead — as the QoS2 dedup path
// used to — breaks on any change to the outer formatting, and a bare substring
// match over-matches (a cause that merely mentions the sentinel text is not the
// sentinel). So: exact Description, or the ": "+cause suffix a cause wrapped
// before ApiError construction produces. Nothing else.
//
// A change to the flattened cause wording itself still defeats recognition.
func isJSErrCausedBy(err error, id ErrorIdentifier, cause error) bool {
	if err == nil || cause == nil {
		return false
	}
	ae := jsAPIError(err)
	if ae == nil {
		return false
	}
	want, ok := ApiErrors[id]
	if !ok || want == nil || ae.ErrCode != want.ErrCode {
		return false
	}
	ct := cause.Error()
	return ae.Description == ct || strings.HasSuffix(ae.Description, ": "+ct)
}

// Process JS API replies.
//
// Can run from various go routines (consumer's loop, system send loop, etc..).
func (as *mqttAccountSessionManager) processJSAPIReplies(_ *subscription, pc *client, _ *Account, subject, _ string, msg []byte) {
	token := tokenAt(subject, mqttJSATokenPos)
	if token == _EMPTY_ {
		return
	}
	jsa := &as.jsa
	chi, ok := jsa.replies.Load(subject)
	if !ok {
		return
	}
	jsa.replies.Delete(subject)
	ch := chi.(chan *mqttJSAResponse)
	out := func(value any) {
		ch <- &mqttJSAResponse{reply: subject, value: value}
	}
	switch token {
	case mqttJSAStreamCreate:
		var resp = &JSApiStreamCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamUpdate:
		var resp = &JSApiStreamUpdateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamLookup:
		var resp = &JSApiStreamInfoResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAStreamPurge:
		var resp = &JSApiStreamPurgeResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerCreate:
		var resp = &JSApiConsumerCreateResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerDel:
		var resp = &JSApiConsumerDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgStore, mqttJSASessPersist:
		var resp = &JSPubAckResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgLoad:
		var resp = &JSApiMsgGetResponse{}
		if err := json.Unmarshal(msg, &resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerLookup:
		var resp = &JSApiConsumerInfoResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAConsumerList:
		var resp = &JSApiConsumerListResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAMsgDelete:
		var resp = &JSApiMsgDeleteResponse{}
		if err := json.Unmarshal(msg, resp); err != nil {
			resp.Error = NewJSInvalidJSONError(err)
		}
		out(resp)
	case mqttJSAAckConfirm:
		// A double-ack confirmation carries no body: its ARRIVAL is the answer.
		// Anything else would be a protocol change, so nothing is parsed here —
		// there is no error form to misread as a success.
		out(nil)
	default:
		pc.Warnf("Unknown reply code %q", token)
	}
}

// This will both load all retained messages and process updates from the cluster.
//
// Run from various go routines (JS consumer, etc..).
// No lock held on entry.
func (as *mqttAccountSessionManager) processRetainedMsg(_ *subscription, c *client, _ *Account, subject, reply string, rmsg []byte) {
	// Unlike upstream, the shim's msgParts (shim_engine.go) returns the pure
	// payload with no trailing wire-protocol "\r\n" to strip (frameNATSMsg
	// builds rmsg from nats.go's already-decoded m.Data, which carries no such
	// terminator) — stripping here would truncate the last 2 payload bytes.
	h, m := c.msgParts(rmsg)
	rm, err := mqttDecodeRetainedMessage(subject, h, m)
	if err != nil {
		// A record this broker cannot decode is an anomaly, not a message meant
		// for someone else: the only format written is the header format, and no
		// mechanism relies any more on servers silently ignoring what they cannot
		// parse. Fail CLOSED — dropping the callback alone would leave a valid
		// predecessor indexed and cached for a subject whose authoritative stream
		// record is unreadable, so the index entry goes too. The index is keyed by
		// the logical retained subject, not by the stream subject the callback
		// carries. This matches loadRetainedMessages, so the live and the
		// post-restart views of a corrupt record agree.
		c.Warnf("Rejecting retained message on %q: %v", subject, err)
		as.removeRetainedMsg(strings.TrimPrefix(subject, mqttRetainedMsgsStreamSubject), 0)
		return
	}
	if strings.IndexByte(rm.Subject, 0x7f) >= 0 {
		c.Warnf("Skipping retained message for subject %q: unsupported character 0x7f", rm.Subject)
		return
	}
	// Get the stream sequence for this message.
	seq, _, _, _, _ := ackReplyInfo(reply)
	if c.srv != nil {
		if gate := c.srv.mqtt.sessmgr.testRetainedApplyGate; gate != nil {
			gate(rm.Subject, len(m) == 0)
		}
	}
	if len(m) == 0 {
		// An empty payload is the tombstone: it IS the deletion, and the store
		// has already evicted the record it replaces (MaxMsgsPer=1 enforcement
		// runs on every store, empty body or not), so there is nothing further to
		// delete. Spec [MQTT-3.3.1-11].
		as.removeRetainedMsg(rm.Subject, 0)
	} else {
		// Add this retained message. The `rm.Msg` references some buffer that we
		// don't own. But addRetainedMsg() will take care of making a copy of
		// `rm.Msg` it `rm` ends-up being stored in the cache.
		as.addRetainedMsg(rm.Subject, seq, rm)
	}
}

// evictOutcome is what one decision attempt of the eviction worker concluded.
type evictOutcome uint8

const (
	// evictNoSession: no resident object for the client — terminal. An observation
	// that declined to spawn while ANY object was resident is consumed against the
	// object now resident; one that arrived with none resident took the no-local
	// branch.
	evictNoSession evictOutcome = iota
	// evictStale: nothing moved strictly past the resident session's acknowledged
	// record — terminal. Covers the equality/own-record case: no remote write can
	// land AT our acknowledged sequence within a stream epoch, so equality is
	// never a takeover.
	evictStale
	// evictBusy: the ownership gate or the record fence is held; the caller
	// retries on a bounded budget, then exits accepting a lost eviction.
	evictBusy
	// evictFresher: a strictly newer trigger arrived past this iteration's
	// knowledge; the caller re-loads.
	evictFresher
	// evictApplied: the session was evicted; the caller runs the notify/close and
	// (takeover only) Will-cancel tail.
	evictApplied
	// evictOwnLanding: the record carries the bytes of THIS session's own
	// unresolved attempt — a store that landed with its response lost. Nothing is
	// evicted and nothing is decided: the ambiguity resolver owns that write, and
	// once it adopts the sequence the same trigger reads stale. Terminal for this
	// attempt, and deliberately NOT a consumed decision, so the record-state
	// backstop re-fires if the resolver never gets there.
	//
	// Without this arm the record would be classified by LINEAGE against the
	// acknowledged pair, which is stale precisely because the attempt is
	// unresolved: our own write {c,a} reads unrelated to acknowledged {a,p} and
	// evicts the rightful client with 0x8E over its own write. The digest is the
	// only thing that identifies an attempt as ours (sess_fork_lineage.go), which
	// is why the comparison is the resolver's own, not a lineage question.
	evictOwnLanding
)

// evictDecision carries one decision attempt's outcome and, on evictApplied, the
// verdict's vocabulary and the state captured for the tail that runs after the
// locks are released.
type evictDecision struct {
	out  evictOutcome
	sess *mqttSession
	// tok/tokHeld: the record transaction still OPEN when the decision section
	// returned. The caller must release it with the full endRecordTx AFTER this
	// decision's as.mu section has returned — the full release can synchronously
	// run the void-verdict handoff, which reacquires as.mu.
	tok     recordTx
	tokHeld bool
	voc     mqttDisposalVocabulary
	forked  bool
	// displacedGen/displacedSeq scope the takeover path's Will-outbox cancel to
	// exactly the Wills the displaced incarnation enqueued. See willCancelScope.
	displacedGen string
	displacedSeq uint64
	ec           *client
}

// evictDecideOnce is the eviction worker's decision: ONE as.mu critical section
// with nothing blocking inside. It resolves the resident object FRESH (the worker
// is per-client, not per-object, so a replacement installed mid-run is simply the
// object decided against), OBSERVES the ownership gate without ever claiming it,
// proves adoption quiescent through the record fence, and only then classifies the
// decoded record against the freshest acknowledged pair.
//
// Holding as.mu across the section is what makes the gate observation sound:
// lockSession itself requires as.mu, so no gate can be NEWLY taken while this runs;
// a gate ALREADY held defers the worker (evictBusy) — a deliberate narrowing versus
// the old synchronous handler, which barged into a gate-held probe.
//
// usable=false is the license path: the observation licensed the eviction and the
// record could not narrow it (absent, undecodable, load error, or below the
// trigger), so decisionSeq is the OBSERVATION level and the verdict concedes to
// takeover. The freshness arm still applies on this path — a trigger that
// advanced past the observation this iteration answers is delivered knowledge,
// not a lost observation, and the re-load may find the record readable at the
// new level.
//
// No lock held on entry.
// loaded is the raw record this iteration read, or nil when the load failed or
// found nothing. It is carried alongside the decoded ps because the digest arm
// compares BYTES — the decoded form cannot answer "are these the bytes we sent".
func (as *mqttAccountSessionManager) evictDecideOnce(cIDHash string, st *sessionReconcileState,
	usable bool, ps *mqttPersistedSession, decisionSeq uint64, loaded *StoredMsg) (d evictDecision) {

	as.mu.Lock()
	defer as.mu.Unlock()
	sess, ok := as.sessByHash[cIDHash]
	if !ok {
		d.out = evictNoSession
		return d
	}
	if _, held := as.sessLocked[sess.id]; held {
		// A CONNECT, close, probe, SUBSCRIBE or UNSUBSCRIBE owns the session's
		// record state; its writes are CAS-arbitrated, so deferring costs at most
		// a lost eviction, which the observation contract already allows.
		d.out = evictBusy
		return d
	}
	tok, acquired := sess.tryBeginRecordTx()
	if !acquired {
		d.out = evictBusy
		return d
	}
	d.sess, d.tok, d.tokHeld = sess, tok, true
	// The abnormal-exit fence guard (LIFO: runs before the as.mu unlock above). A
	// panic or Goexit escaping the seam below must release the fence, and the full
	// endRecordTx is illegal here — it can synchronously run the void-verdict
	// handoff, which reacquires as.mu. abandonRecordTx takes only sess.mu (the
	// established as.mu → sess.mu order) and skips the handoff, whose skipped
	// liveness edge the periodic backstop (reconcileResidentRecordState →
	// flushVoidVerdictGap) owns. Every NORMAL return hands the open token to the
	// caller instead, which releases it fully after as.mu.
	normalReturn := false
	defer func() {
		if !normalReturn {
			_ = sess.abandonRecordTx(tok)
			d.tokHeld = false
		}
	}()
	// Test seam: fired with as.mu held and the record fence open — the window an
	// abnormal exit must clean up from. Handlers may panic or Goexit; they must
	// not block (they pin as.mu).
	if h := as.testEvictPostFence; h != nil {
		h(cIDHash)
	}
	// Fence acquired proves adoption QUIESCENT: every sess.seq/lineage adoption on
	// a resident, concurrently reachable object happens inside the writer's open
	// recordTx, so no adoption is in flight and a DELAYED acknowledgment (store
	// returned, tx still open until it adopts) cannot be raced past — which is
	// what retires the ordering hazard where a remote observation is handled
	// before the local write's own PubAck adopted its sequence. What it does NOT
	// prove is that no AMBIGUOUS attempt is outstanding: a save whose store landed
	// but whose response was lost arms seqUncertain, closes its transaction, and
	// leaves this pair stale behind a free fence. That staleness only ever NARROWS
	// the verdict — a sibling of the unadopted attempt reads unrelated against the
	// stale pair and CONCEDES to the takeover claim (the stated one-hop
	// recognition miss, exactly today's behavior), while a sibling of the
	// acknowledged record itself still classifies forked (a true positive). No
	// record shape can manufacture a false forked verdict from a stale pair, so
	// the eviction proceeds; the unresolved attempt's own convergence stays with
	// the resolver and the rejection probe. See sess_persist_evict_test.go's
	// unresolved-attempt pair test and docs/CONFORMANCE.md.
	//
	// The one shape that staleness does NOT merely narrow is our own unresolved
	// attempt arriving back as a delivered record: that is not a sibling of
	// anything, it IS our write, and only its digest says so. Sampled in the same
	// sess.mu section as the pair, so no resolver can adopt between the two reads
	// and leave the arm deciding on a mismatched snapshot.
	sess.mu.Lock()
	seqNow, gen, wid, pwid := sess.seq, sess.gen, sess.wid, sess.pwid
	uncertain, pw := sess.seqUncertain, sess.pendingWrite
	var pwBase uint64
	var pwDigest [sha256.Size]byte
	if pw != nil {
		pwBase, pwDigest = pw.baseSeq, pw.digest
	}
	sess.mu.Unlock()
	// Strict: eviction requires the record to have moved PAST us.
	if decisionSeq <= seqNow {
		d.out = evictStale
		normalReturn = true
		return d
	}
	// The digest arm, ahead of every classification: while a write of ours is
	// unresolved, a record carrying its exact bytes is that write landing with its
	// response lost — the resolver's own identity test (sess_uncertain.go), reused
	// verbatim so the two cannot drift. Deciding it here would be the false
	// eviction the old SP path avoided by never delivering our own writes at all.
	// A NON-matching record under seqUncertain keeps today's behavior exactly: it
	// falls through to lineage against the stale pair, which can only narrow.
	if uncertain && pw != nil && loaded != nil &&
		loaded.Sequence > pwBase && mqttRecordDigest(loaded.Data) == pwDigest {
		d.out = evictOwnLanding
		normalReturn = true
		return d
	}
	// The SAME refusal when there are no bytes to compare. An unusable load — absent,
	// unreadable, undecodable, or below the trigger — normally concedes to the
	// takeover vocabulary and decides at the observation level, which is right when
	// the observation can only have come from a peer. It is NOT right while a write
	// of ours is unresolved and could itself be what was observed: the trigger can be
	// our own landed-but-unacknowledged write (the backstop reaches this path with no
	// record at all, deliberately, so the Origin filter cannot screen it), and
	// conceding would evict the rightful client over its own write on the one
	// occasion the digest cannot speak.
	//
	// Refusing costs only latency: the periodic pass settles the ambiguity before it
	// ever reaches the ownership backstop again, and the undecided trigger is
	// re-fired afterwards against an adopted sequence, where staleness or lineage
	// decides it properly.
	if uncertain && pw != nil && !usable && decisionSeq > pwBase {
		d.out = evictOwnLanding
		normalReturn = true
		return d
	}
	// A strictly newer trigger than this iteration's own knowledge deserves a
	// fresh read — on the unusable-load path included: an observation that
	// advanced past the trigger this iteration answers is DELIVERED knowledge,
	// and a record unreadable at the old level can be readable (and classifiable)
	// at the new one, so conceding here would cancel a Will on knowledge already
	// superseded (holistic round 2). Requiring the advance to pass decisionSeq
	// (= max(target, smsg.Sequence) when the record was usable, the observation
	// level otherwise) bounds the loop by observation count.
	if st.evictLatest > decisionSeq {
		d.out = evictFresher
		normalReturn = true
		return d
	}
	// Classify HERE, at decision time, from the freshest pair — never from a pair
	// sampled at load time, which a local save adopting between the load and this
	// section would falsify. The license path concedes.
	voc := mqttDisposalTakenOver
	if usable {
		voc = disposalForRemotePersist(ps, gen, wid, pwid)
	}
	// The remote record owns the session now; our expiry timer must not fire and
	// delete its state. Spec5 [3.1.2.11.2].
	as.cancelSessionExpiryLocked(cIDHash)
	as.removeSession(sess, false)
	sess.mu.Lock()
	d.displacedGen, d.displacedSeq = sess.gen, sess.seq
	ec := sess.c
	if ec != nil {
		as.addSessToFlappers(sess.id)
		ec.Warnf("MQTT: session %q %s", sess.id, voc.logReason)
		// Disassociate the client from the session so that on client close,
		// nothing will be done with regards to cleaning up the session, such as
		// deleting stream, etc..
		sess.c = nil
	}
	sess.mu.Unlock()
	d.voc, d.forked, d.ec = voc, voc.reason == mqttDisposalForked.reason, ec
	d.out = evictApplied
	normalReturn = true
	return d
}

// evictDisplacedOnRemotePersist evicts the resident local session after
// the session-record watch observed a record write at-or-past its acknowledged
// record — classifying the surviving record FIRST, so a demonstrated successor is
// reported as a takeover (0x8E, delayed-Will outbox cancelled) while a proven
// same-base sibling — the leader-transition fork — is reported as 0x89 "server
// busy" (inviting the reconnect that resolves the ambiguity) and destroys NO
// durable Will state, because nothing arbitrated a session continuation.
//
// It is per-CLIENT, not per-object: at most one runs per client (st.evictRunning),
// and each iteration resolves the resident object fresh under as.mu, so a
// replacement installed mid-run is simply the object the next iteration decides
// against. This is RECOGNITION plus in-process coalescing; the durable
// arbitrations remain the record CAS and the rejection probe, and a lost
// observation is never replayed.
//
// st is the *sessionReconcileState the callback registered for cIDHash; it is
// passed in (not re-fetched) so the cleanup targets exactly that entry.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) evictDisplacedOnRemotePersist(s *Server, cIDHash string, st *sessionReconcileState) {
	applied := false
	completed := false
	// cleanup clears the coalescing flag, decrements the exact state the callback
	// incremented, and prunes through the one helper — which keeps the entry alive
	// while a session is still resident, because an undecided trigger left by a
	// refusal is precisely what the record-state backstop re-fires. See
	// pruneReconcileEntryLocked.
	cleanup := func() {
		as.mu.Lock()
		st.evictRunning = false
		if st.inFlight > 0 {
			st.inFlight--
		}
		as.pruneReconcileEntryLocked(cIDHash, st)
		as.mu.Unlock()
	}
	// observe fires the completion seam AFTER cleanup, on the normal and abnormal
	// paths both, carrying whether an eviction was applied — so a test awaiting a
	// "no eviction" outcome asserts on the worker's DECISION, not its absence.
	observe := func() {
		as.mu.RLock()
		h := as.testEvictComplete
		as.mu.RUnlock()
		if h != nil {
			h(cIDHash, applied)
		}
	}
	// Abnormal exits only (a panic or Goexit escaping a test seam): the normal
	// path funnels through the in-loop terminal cleanup, whose atomic re-check
	// keeps a late trigger from stranding; this guard accepts trigger loss to keep
	// the flag and counter from leaking.
	defer func() {
		if completed {
			return
		}
		cleanup()
		observe()
	}()

	// consumed is the trigger level this worker's last iteration decided against;
	// the terminal cleanup re-checks st.latest against it under as.mu.
	var consumed uint64
	for {
		as.mu.RLock()
		// The trigger this iteration answers. Read fresh each iteration: a newer
		// resident-delivered observation only advances it (no-resident
		// observations advance only the reconcile watermark, never this).
		target := st.evictLatest
		pauseBefore := as.testEvictPauseBeforeLoad
		as.mu.RUnlock()
		if pauseBefore != nil {
			pauseBefore(cIDHash)
		}

		// ONE read per iteration; the observation is at-most-once and the read is
		// bounded, so JetStream unavailability delays only this eviction.
		smsg, err := as.jsa.loadSessionMsg(as.domainTk, cIDHash)
		if err != nil && isErrorOtherThan(err, JSNoMessageFoundErr) {
			s.Warnf("MQTT remote-persist eviction: unable to load session record: %v", err)
		}
		// The license rule: the eviction's LICENSE is the observation — a remote
		// write happened — and the read can only NARROW the claim, never revoke
		// the license. Absent, undecodable, a load error, or a record BELOW the
		// trigger (older than the write that licensed the eviction — classifying
		// it could reproduce the exact wrong 0x8E-plus-cancel this path exists to
		// prevent) all concede: the decision runs at the OBSERVATION level with
		// the takeover vocabulary. Decode outside any lock.
		var ps *mqttPersistedSession
		usable := false
		decisionSeq := target
		// The raw record travels separately from the decoded one and is kept even
		// when the record is BELOW the trigger or undecodable: the digest arm asks
		// whether these are the bytes this session sent, which is a question about
		// the load, not about the license.
		var loaded *StoredMsg
		if err == nil && smsg != nil {
			loaded = smsg
			if smsg.Sequence >= target {
				if ps = mqttDecodeSessionRecord(smsg); ps != nil {
					usable = true
					decisionSeq = smsg.Sequence
				}
			}
		}

		as.mu.RLock()
		pauseAfter := as.testEvictPauseAfterLoad
		as.mu.RUnlock()
		if pauseAfter != nil {
			pauseAfter(cIDHash)
		}

		// The decision, on a bounded busy-retry budget. Each attempt re-resolves
		// residency fresh; the sleeps run outside every lock.
		var d evictDecision
		for attempt := 0; ; attempt++ {
			d = as.evictDecideOnce(cIDHash, st, usable, ps, decisionSeq, loaded)
			if d.tokHeld {
				// Full release, strictly AFTER the decision's as.mu section
				// returned: endRecordTx can synchronously run the void-verdict
				// handoff, which takes as.mu; the other order self-deadlocks.
				if rerr := d.sess.endRecordTx(d.tok); rerr != nil {
					s.Warnf("MQTT remote-persist eviction: unable to release record transaction: %v", rerr)
				}
			}
			if d.out != evictBusy || attempt+1 >= mqttTeardownLockAttempts {
				break
			}
			time.Sleep(mqttTeardownLockItvl)
		}
		// consumed is what this iteration actually DECIDED against. A busy
		// refusal and a no-resident exit decided nothing: crediting them with the
		// opportunistically loaded record's sequence would let an observation at
		// exactly that level — delivered after the last failed attempt — read as
		// already consumed at cleanup, stranding a genuine trigger behind the
		// clearing flag. They consume only the trigger they answered.
		consumed = decisionSeq
		if d.out == evictBusy || d.out == evictNoSession {
			consumed = target
		}
		// evictDecided is the OTHER half of that accounting, and it is what the
		// record-state backstop reads. `consumed` governs this worker's own loop;
		// evictDecided says whether anything ever reached a verdict for this client.
		// A refusal — busy, or our own landing deferred to the resolver — leaves it
		// behind evictLatest deliberately, which is the re-fire signal. Advanced
		// only where a decision really happened.
		decided := d.out == evictApplied || d.out == evictStale || d.out == evictNoSession
		if decided {
			as.mu.Lock()
			if decisionSeq > st.evictDecided {
				st.evictDecided = decisionSeq
			}
			as.mu.Unlock()
		}

		switch d.out {
		case evictApplied:
			applied = true
			if d.ec != nil {
				// Notify and close AFTER every lock release: the notify writes to
				// the socket under c.mu with no deadline. The shared tail claims
				// the connection's one DISCONNECT and honors the CONNACK gate, so
				// this cannot append a second DISCONNECT to one a racing
				// displacement already sent; 3.1.1 closes silently. The object is
				// already deresidented, so a replacement CONNECT proceeds
				// regardless of this write's duration. Spec5 [3.1.4].
				d.ec.mqttNotifyDisplaced(d.voc)
				go d.ec.closeConnection(d.voc.closeReason)
			}
			if !d.forked {
				// Cancel any delayed Will the displaced incarnation enqueued —
				// on the TAKEOVER verdict only. A forked verdict proved nothing
				// arbitrated a session continuation, so this path must not
				// destroy the durable outbox record; the fire-time fences still
				// decide its fate. Inline: this goroutine is already off the
				// delivery path and admitted/tracked, and running it before the
				// completion seam is what lets tests assert the record's final
				// state. Spec5 [MQTT-3.1.2-8].
				_ = as.cancelWillOutboxForIncarnation(s, cIDHash, d.displacedGen, d.displacedSeq)
			}
		case evictFresher:
			continue
		}

		// Terminal cleanup, in-loop: clearing the coalescing flag and consuming a
		// late trigger must be ONE as.mu decision, or an observation that declined
		// to spawn (flag still true) while this worker was past its last check
		// would strand. If a resident object exists and a strictly newer trigger
		// arrived, this same worker loops — with a fresh busy budget, possibly
		// against a replacement object — instead of exiting.
		if h := func() func(string) {
			as.mu.RLock()
			defer as.mu.RUnlock()
			return as.testEvictCleanupEntry
		}(); h != nil {
			h(cIDHash)
		}
		as.mu.Lock()
		_, resident := as.sessByHash[cIDHash]
		if resident && st.evictLatest > consumed {
			as.mu.Unlock()
			continue
		}
		st.evictRunning = false
		if st.inFlight > 0 {
			st.inFlight--
		}
		as.pruneReconcileEntryLocked(cIDHash, st)
		as.mu.Unlock()
		break
	}
	completed = true
	observe()
}

// reconcileRemoteSessionExpiry reconciles this replica's local session-expiry
// timer from the authoritative persisted record after the session-record watch
// observed a remote session persist for a client this replica does not own
// locally. It runs on a spawned, admitted worker (never inline in the delivery
// callback) because loadSessionMsg is a blocking JSA request.
//
// st is the *sessionReconcileState the callback incremented for idHash; it is
// passed in (not re-fetched) so the inFlight decrement targets exactly that entry.
// observedSeq is the PubAck sequence the callback saw. All timer decisions are made
// atomically under as.mu: a newer observation (watermark) or a live local session
// (ownership) makes this worker leave the timer untouched, because expireSession's
// in-memory path does not re-validate the deadline and could otherwise clear a
// newer generation early. Spec5 [3.1.2.11.2].
//
// No lock held on entry.
func (as *mqttAccountSessionManager) reconcileRemoteSessionExpiry(s *Server, idHash string, observedSeq uint64, st *sessionReconcileState) {
	// Lifecycle-tied prune: decrement the exact state the callback incremented, then
	// prune through the ONE helper. Deleting on `inFlight == 0` alone — which this
	// path used to do — is what let a no-local worker erase a RESIDENT session's
	// undecided eviction trigger, since the two kinds share one entry. Runs on every
	// exit path, so a worker parked before mu keeps its own entry alive and can never
	// resume into a pruned-away watermark. See pruneReconcileEntryLocked.
	defer func() {
		as.mu.Lock()
		if st.inFlight > 0 {
			st.inFlight--
		}
		as.pruneReconcileEntryLocked(idHash, st)
		as.mu.Unlock()
	}()

	// Re-read the authoritative record. Missing/undecodable leaves nothing to
	// reconcile (mirrors loadExpiringSessionRecord's warning behavior).
	smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			s.Warnf("MQTT remote session-expiry reconciliation: unable to load session record: %v", err)
		}
		return
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		s.Warnf("MQTT remote session-expiry reconciliation: unable to decode session record at sequence %v: %v", smsg.Sequence, err)
		return
	}
	// Defensive: never arm from a snapshot older than the write we observed. In
	// practice loadSessionMsg returns the subject's last message, whose sequence is
	// >= observedSeq unless the record was deleted (handled above as not-found), so
	// this is a guard rather than a live path.
	if smsg.Sequence < observedSeq {
		return
	}

	// Test seam: park after the load, before the decision lock, so worker-ordering
	// tests can race a newer worker or a local reconnect/re-disconnect ahead.
	as.mu.RLock()
	pause := as.testReconcilePause
	as.mu.RUnlock()
	if pause != nil {
		pause(idHash)
	}

	as.mu.Lock()
	defer as.mu.Unlock()
	// Shutdown: do not install new timers.
	if as.pendingExpiries == nil {
		return
	}
	// Worker-ordering guard: a newer observation for this client supersedes us. Equal
	// is fine — a duplicate observation applies the same decision idempotently.
	if observedSeq < st.latest {
		return
	}
	// Ownership guard: a local session appeared after the callback handed work off (a
	// reconnect, possibly followed by a local re-disconnect that already armed the
	// correct later timer). Its lifecycle owns the timer; leave it untouched.
	if _, ok := as.sessByHash[idHash]; ok {
		return
	}
	// Decide from the authoritative record.
	if ps.DisconnectedAt == 0 || ps.ExpiryInterval == 0 || ps.ExpiryInterval == mqttSessionNeverExpire {
		// Connected, zero-expiry, or never-expire: nothing should expire this
		// session; drop any local timer armed from a superseded record.
		as.cancelSessionExpiryLocked(idHash)
		return
	}
	if remaining := time.Until(time.Unix(ps.DisconnectedAt+int64(ps.ExpiryInterval), 0)); remaining > 0 {
		as.scheduleSessionExpiryLocked(s, idHash, remaining)
		return
	}
	// Already due: arm a 0-delay timer that fires on its own goroutine into the
	// record-based cleanup path (its own connecting guard + sequence-targeted
	// delete). Do not call expireSessionFromRecord inline — it would re-enter mu and
	// issue JSA requests under the lock.
	as.scheduleSessionExpiryLocked(s, idHash, 0)
}

// Adds this client ID to the flappers map, and if needed start the timer
// for map cleanup.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) addSessToFlappers(clientID string) {
	as.flappers[clientID] = time.Now()
	if as.flapTimer == nil {
		as.flapTimer = time.AfterFunc(mqttFlapCleanItvl, func() {
			as.mu.Lock()
			defer as.mu.Unlock()
			// In case of shutdown, this will be nil
			if as.flapTimer == nil {
				return
			}
			now := time.Now()
			for cID, tm := range as.flappers {
				if now.Sub(tm) > mqttSessJailDur {
					delete(as.flappers, cID)
				}
			}
			as.flapTimer.Reset(mqttFlapCleanItvl)
		})
	}
}

// Remove this client ID from the flappers map.
//
// Lock held on entry.
func (as *mqttAccountSessionManager) removeSessFromFlappers(clientID string) {
	delete(as.flappers, clientID)
	// Do not stop/set timer to nil here. Better leave the timer run at its
	// regular interval and detect that there is nothing to do. The timer
	// will be stopped on shutdown.
}

// storeSessionRecordCAS rewrites a session record expecting the given stream
// sequence, so a concurrent writer (e.g. a reconnect elsewhere) wins and this
// rewrite is dropped.
func (as *mqttAccountSessionManager) storeSessionRecordCAS(ps *mqttPersistedSession, idHash string, seq uint64) error {
	_, err := mqttStoreSessionRecordCAS(&as.jsa, as.domainTk, ps, idHash, seq)
	return err
}

// mqttStoreSessionRecordCAS is storeSessionRecordCAS's body, on the JSA rather
// than the account manager and returning the new sequence. The teardown claim
// needs both: it runs from sess.clear(), which is handed no account context, and
// it must know the sequence its marker landed at in order to release it. See
// sess_teardown.go.
func mqttStoreSessionRecordCAS(jsa *mqttJSA, domainTk string, ps *mqttPersistedSession, idHash string, seq uint64) (uint64, error) {
	b, _ := json.Marshal(ps)
	return mqttStoreSessionRecordCASBody(jsa, domainTk, b, idHash, seq)
}

// mqttStoreSessionRecordCASBody is the same CAS store given the record body
// verbatim, for callers that already hold the exact bytes.
func mqttStoreSessionRecordCASBody(jsa *mqttJSA, domainTk string, b []byte, idHash string, seq uint64) (uint64, error) {
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatUint(seq, 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	bb.Write(b)
	resp, err := jsa.storeSessionMsg(domainTk, idHash, hdr, bb.Bytes())
	if err != nil {
		return 0, err
	}
	return resp.Sequence, nil
}

// scheduleSessionExpiry arms a timer to clean up a disconnected client's session
// once its MQTT 5.0 Session Expiry Interval elapses. The timer is keyed by the
// client ID hash (the same token used in the session-persist stream) so both a
// local reconnect and a remote takeover on another server can cancel it via
// cancelSessionExpiry / cancelSessionExpiryLocked. Spec5 [3.1.2.11.2].
//
// Deadlines survive a restart via the persisted disconnect time and
// sweepSessionExpiries. A surviving replica also re-arms the timer when it
// observes the persisted disconnect: the session-record watch reconciles the
// deadline for a session it does not own locally, so the session no longer
// lingers until a reconnect or the periodic sweep if the owning broker dies.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) scheduleSessionExpiry(s *Server, idHash string, delay time.Duration) {
	as.mu.Lock()
	defer as.mu.Unlock()
	as.scheduleSessionExpiryLocked(s, idHash, delay)
}

// scheduleSessionExpiryLocked is scheduleSessionExpiry for callers already holding
// as.mu (e.g. the remote-expiry reconciliation worker, which must decide and arm
// atomically under the lock). Spec5 [3.1.2.11.2].
func (as *mqttAccountSessionManager) scheduleSessionExpiryLocked(s *Server, idHash string, delay time.Duration) {
	// nil map => account manager is shutting down; do not schedule.
	if as.pendingExpiries == nil {
		return
	}
	// Defensive: replace any existing timer for this client ID hash.
	if old, ok := as.pendingExpiries[idHash]; ok {
		old.Stop()
	}
	as.pendingExpiries[idHash] = time.AfterFunc(delay, func() { as.expireSession(s, idHash) })
}

// cancelSessionExpiry stops and removes a pending session-expiry timer for the
// given client ID hash, if any. Called when a new connection resumes the session
// before the interval elapses. Returns true if one was cancelled. Idempotent.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) cancelSessionExpiry(idHash string) bool {
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.cancelSessionExpiryLocked(idHash)
}

// cancelSessionExpiryLocked is cancelSessionExpiry for callers already holding
// as.mu (e.g. the session-persist callback that cancels on a remote takeover).
func (as *mqttAccountSessionManager) cancelSessionExpiryLocked(idHash string) bool {
	t, ok := as.pendingExpiries[idHash]
	if !ok {
		return false
	}
	t.Stop()
	delete(as.pendingExpiries, idHash)
	return true
}

// expireSession removes and clears a session whose Session Expiry Interval has
// elapsed, unless it was cancelled (reconnect/takeover) or the account manager is
// shutting down. Reuses the same removeSession + clear path as a clean disconnect.
// A session not in the in-memory maps (timer re-armed by the startup sweep) is
// re-verified against the persisted record first. Spec5 [3.1.2.11.2].
//
// No lock held on entry.
func (as *mqttAccountSessionManager) expireSession(s *Server, idHash string) {
	as.mu.Lock()
	// nil map => account manager shutdown; the entry (if any) is being torn down.
	if as.pendingExpiries == nil {
		as.mu.Unlock()
		return
	}
	t, ok := as.pendingExpiries[idHash]
	if !ok {
		// Cancelled between the timer firing and acquiring the lock.
		as.mu.Unlock()
		return
	}
	// We own this expiry now; drop the timer entry so a concurrent reconnect's
	// cancelSessionExpiry becomes a no-op.
	t.Stop()
	delete(as.pendingExpiries, idHash)
	sess, inMem := as.sessByHash[idHash]
	as.mu.Unlock()
	if !inMem {
		as.expireSessionFromRecord(s, idHash)
		return
	}

	// Guard against a concurrent reconnect: lockSession(sess, nil) fails if a
	// CONNECT is mid-processing or a client is bound — the reconnect then owns it.
	if err := as.lockSession(sess, nil); err != nil {
		return
	}
	// Re-check that no client re-bound before we acquired the session lock. The
	// binding is the ownership gate's business, which this path holds, so it is
	// decided here rather than inside the record transaction below.
	sess.mu.Lock()
	rebound := sess.c != nil
	sess.mu.Unlock()
	if rebound {
		as.unlockSession(sess)
		return
	}
	// If the persisted record was bumped past our sequence, our in-memory copy is
	// stale: a resume (and possibly a re-disconnect) happened on another server and
	// its best-effort cancel to this server may have been lost. Do NOT act on our
	// stale state — drop the local copy and let the record-based path decide from
	// the authoritative record: it skips a still-connected or not-yet-due session
	// (re-arming as needed) and cleans up a due one using the record's own sequence
	// and consumers. A record that is not newer (or absent from a failed
	// best-effort disconnect save) leaves our in-memory view authoritative, so the
	// fired timer still cleans up and finite expiry is never silently skipped.
	// Spec5 [3.1.2.11.2].
	//
	// A higher record sequence is NOT evidence of a remote owner while this session's
	// own write is unresolved: an ambiguous write that actually LANDED with its
	// response lost leaves sess.seq stale and the record's sequence higher, so this
	// condition would be true for our own write and would evict the object — with its
	// pendingWrite, its retained continuations and its staged void-register verdicts —
	// before clear() could refuse. Sequence is not identity (see sess_uncertain.go,
	// which classifies by DIGEST); so while unresolved, fall through to the clear()
	// attempt, which refuses with the sentinel and parks the discard, and let
	// resolution decide ours-versus-foreign with the test that can actually tell them
	// apart. A foreign verdict evicts and drops the obligation; an "ours" verdict
	// adopts the landed sequence and discharges the discard against it.
	//
	// ONE uninterrupted record transaction covers the load, the sess.seq read and the
	// decision. A mutex sample alone is check-then-act — the fence is what serializes
	// record writers — and arming uncertainty requires a save, which requires this
	// fence, so nothing can change the decision's inputs underneath it. The seq read
	// belongs INSIDE for the same reason: a save landing between an outside sample and
	// the fence advances the real sequence and yields a false "superseded". See
	// sess_recordtx.go.
	tok := sess.beginRecordTx()
	var loadedSeq uint64
	if smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash); err == nil {
		loadedSeq = smsg.Sequence
	}
	sess.mu.Lock()
	seq := sess.seq
	uncertain := sess.seqUncertain
	sess.mu.Unlock()
	superseded := !uncertain && loadedSeq > seq
	// Test seam, fired with the fence still HELD and the load already done, carrying the
	// sequences the decision was made from: that pair is what states "one uninterrupted
	// transaction" as an assertion instead of a race.
	if h := mqttTestExpireDecisionHookV.Load(); h != nil {
		(*h)(sess, loadedSeq, seq, superseded)
	}
	// Released WITHOUT ending the generation — nothing was mutated — and before clear(),
	// which opens a transaction of its own because it ends the generation. An ambiguity
	// armed in that gap lands on clear()'s entry or in-fence refusal, so both branches
	// converge on the sentinel and the recovery below.
	if terr := sess.endRecordTx(tok); terr != nil {
		s.Errorf("MQTT: %v", terr)
	}
	if superseded {
		// Our copy is stale: drop it and hand off to the record-based path, which
		// guards against a concurrent CONNECT itself (its connecting check and a
		// sequence-CAS'd delete), so we release the session lock before delegating.
		as.removeSession(sess, true)
		as.unlockSession(sess)
		as.expireSessionFromRecord(s, idHash)
		return
	}

	// Authoritative cleanup: hold the session lock through clear() so a concurrent
	// CONNECT cannot restore the (possibly stale) persisted record and race the
	// teardown. Delete the session state (consumers, record), matching the
	// clean-disconnect path in mqttHandleClosedClient.
	defer as.unlockSession(sess)
	as.removeSession(sess, true)
	if err := sess.clear(true); err != nil {
		// The refusal deleted nothing, and the evicted object would take the unresolved
		// write's pendingWrite, its retained continuations and its staged void-register
		// verdicts with it — all of them lost outright, since only this object can classify
		// that write. This path has NO early uncertainty check, so the write is often
		// already unresolved when the timer fires rather than going ambiguous under us;
		// either way the sentinel says nothing was deleted, which is what makes the discard
		// recoverable. Park it with the deadline the client actually earned and restore
		// residency; the deferred as.unlockSession above then releases the gate and runs the
		// void-verdict handoff for a session that is resident again. Keyed on the SENTINEL
		// alone. See reparkRefusedDiscard.
		sess.mu.Lock()
		disconnectedAt := sess.disconnectedAt
		sess.mu.Unlock()
		if errors.Is(err, errMQTTClearRefusedUnresolved) && s.reparkRefusedDiscard(as, sess, disconnectedAt) {
			s.Warnf("MQTT session expiry for %q found the record state unresolved; the discard stays parked until it is classified", sess.id)
			return
		}
		s.Warnf("MQTT session expiry cleanup for %q failed: %v", sess.id, err)
	}
}

// mqttTestExpireDecisionHookV fires inside expireSession's record transaction, immediately
// after the superseded decision is computed and while the fence is still held. See
// expireSession.
var mqttTestExpireDecisionHookV atomic.Pointer[func(sess *mqttSession, loadedSeq, seq uint64, superseded bool)]

func setMQTTTestExpireDecisionHook(h func(sess *mqttSession, loadedSeq, seq uint64, superseded bool)) func() {
	prev := mqttTestExpireDecisionHookV.Load()
	if h == nil {
		mqttTestExpireDecisionHookV.Store(nil)
	} else {
		mqttTestExpireDecisionHookV.Store(&h)
	}
	return func() { mqttTestExpireDecisionHookV.Store(prev) }
}

// loadExpiringSessionRecord loads and decodes the persisted record for idHash
// and reports whether its Session Expiry deadline has elapsed (due). A resume
// anywhere rewrites the record with DisconnectedAt=0, so a stale timer reading it
// finds nothing due — the record is authoritative. If the deadline has not yet
// passed (a newer disconnect refreshed it) it re-arms the timer and returns
// due=false. Returns the decoded record and its stream sequence for the caller.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) loadExpiringSessionRecord(s *Server, idHash string) (ps *mqttPersistedSession, seq uint64, due bool) {
	smsg, err := as.jsa.loadSessionMsg(as.domainTk, idHash)
	if err != nil {
		// Not found => the session was cleared or resumed-then-cleaned; done.
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			s.Warnf("MQTT session expiry: unable to load session record: %v", err)
		}
		return nil, 0, false
	}
	ps = &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		s.Warnf("MQTT session expiry: unable to decode session record at sequence %v: %v", smsg.Sequence, err)
		return nil, 0, false
	}
	// A connected client (or a session that no longer expires) leaves nothing to do.
	if ps.DisconnectedAt == 0 || ps.ExpiryInterval == 0 || ps.ExpiryInterval == mqttSessionNeverExpire {
		return ps, smsg.Sequence, false
	}
	// Not due yet (e.g. the record was refreshed by a newer disconnect after this
	// timer was armed): re-arm for the remainder instead of dropping the cleanup.
	if remaining := time.Until(time.Unix(ps.DisconnectedAt+int64(ps.ExpiryInterval), 0)); remaining > 0 {
		as.scheduleSessionExpiry(s, idHash, remaining)
		return ps, smsg.Sequence, false
	}
	return ps, smsg.Sequence, true
}

// expireSessionFromRecord handles an expiry timer firing for a session not in
// the in-memory maps (re-armed by the startup sweep). The persisted record is
// authoritative: a reconnect anywhere rewrites it with a zero disconnect time,
// so re-reading it right before acting keeps a stale timer harmless.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) expireSessionFromRecord(s *Server, idHash string) {
	ps, seq, due := as.loadExpiringSessionRecord(s, idHash)
	if !due {
		return
	}
	// Skip if a CONNECT for this client ID is being processed on this server; its
	// save will reset the disconnect time (and the record delete below is CAS-ed
	// on the sequence we just read, so a concurrent remote resume wins).
	as.mu.Lock()
	_, connecting := as.sessLocked[ps.ID]
	as.mu.Unlock()
	if connecting {
		return
	}

	// A claimed teardown is FINISHED, never re-claimed. Unlike clear() — whose CAS
	// against a stale in-memory sequence is itself the guard — this path reads the
	// record fresh, so its claim WOULD succeed and would overwrite a peer's marker
	// with our nonce, leaving the peer unable to recognize its own release. See
	// sess_teardown.go.
	// Same rule as the CONNECT restore: nothing may build a name or a purge filter
	// from an unvalidated stored generation. See mqttSessGenFromRecord.
	recGen, gerr := mqttSessGenFromRecord(ps.Gen)
	if gerr != nil {
		s.Warnf("MQTT session expiry: refusing to act on %q: %v", ps.ID, gerr)
		return
	}
	ps.Gen = recGen

	if ps.Teardown != nil {
		if err := as.finishAdoptedTeardown(ps, idHash, seq, s.getOpts()); err != nil {
			s.Warnf("MQTT session expiry: finishing the claimed teardown of %q failed: %v", ps.ID, err)
		}
		return
	}

	// Build a transient session from the record — enough state for clear() to
	// delete the consumers and the record itself — and clean up.
	sess := mqttSessionCreate(&as.jsa, ps.ID, idHash, ps.Gen, seq, ps.WID, ps.PWID, s.getOpts())
	sess.asm = as
	sess.domainTk = as.domainTk
	sess.clean = ps.Clean
	sess.subs = ps.Subs
	sess.sharedSubs = ps.SharedSubs
	sess.cons = ps.Cons
	// The restored lists came OUT of the record, so they are already durable there; only
	// a later change reopens the gap.
	sess.cancelledRegNonces = mqttNonceSet(ps.CancelledRegs)
	sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
	sess.restoreDrainsFromRecord(ps)
	sess.pubRelConsumer = ps.PubRel
	sess.q2AffConsumer = ps.Q2Aff
	sess.q2AffGen = ps.Q2AffGen
	// clear()'s late-uncertainty refusal is UNREACHABLE here, so no recovery is written
	// for it: seqUncertain is armed only by a failed save on this very object
	// (saveWithDetail), and this object is transient — mqttSessionCreate starts no
	// goroutine, it is registered nowhere, and nothing writes through it before the
	// clear() below. If that proof were ever wrong the refusal is still fail-closed:
	// the session is not destroyed on a sequence nobody classified, only left for the
	// next sweep.
	if err := sess.clear(true); err != nil {
		s.Warnf("MQTT session expiry cleanup for %q failed: %v", ps.ID, err)
	}
}

// sweepSessionExpiries re-arms expiry timers from the persisted session records,
// so deadlines survive a restart; overdue sessions get an immediate timer.
// expireSession re-verifies each record before deleting anything, so racing a
// reconnect or another server's sweep is safe. Runs from a go routine at account
// session manager creation (the first MQTT connect for the account after a
// restart) and then periodically. Spec5 [3.1.2.11.2].
//
// This is fallback and startup recovery, not the normal cross-replica handoff:
// when a broker dies with the cluster still running, the session-record watch's
// remote-expiry reconciliation re-arms surviving replicas near the original
// deadline. The sweep still catches sessions no replica observed (e.g. a persist
// that landed before an account manager existed on any survivor).
//
// No lock held on entry.
func (as *mqttAccountSessionManager) sweepSessionExpiries(s *Server) {
	if h := mqttTestSessExpirySweepPassHookV.Load(); h != nil {
		defer (*h)(as)
	}
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	var swept int
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			// No (more) messages, or the stream/server is going away: done.
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT session expiry sweep ended: %v", err)
			}
			break
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			s.Warnf("MQTT session expiry sweep: unable to decode session record at sequence %v, skipping: %v", smsg.Sequence, err)
			continue
		}
		// Only sessions with a finite interval can expire.
		if ps.ExpiryInterval == 0 || ps.ExpiryInterval == mqttSessionNeverExpire {
			continue
		}
		idHash := getHash(ps.ID)
		if ps.DisconnectedAt == 0 {
			// The record claims a connected client. If we own the record and
			// have no session for it in memory, the owner (us) went down while
			// the client was connected and its disconnect was never recorded:
			// stamp it as of now so the finite interval eventually applies.
			// The CAS write defers to any concurrent reconnect; an in-memory
			// session (bound or freshly disconnected) manages its own state.
			// The ownership gate applies only in a cluster (where every node
			// sweeps, and server names are explicit): standalone, a generated
			// node name changes across restarts and would never match.
			if s.JetStreamIsClustered() && ps.Origin != s.NodeName() {
				continue
			}
			as.mu.RLock()
			_, inMem := as.sessByHash[idHash]
			as.mu.RUnlock()
			if inMem {
				continue
			}
			ps.DisconnectedAt = time.Now().Unix()
			// A raw record write, so it carries a lineage like any other. ps was
			// decoded from the loaded record, so its CURRENT WID is this write's
			// parent — set PWID from it BEFORE minting the new one, or the write
			// parents itself. See sess_fork_lineage.go.
			ps.PWID, ps.WID = ps.WID, mqttNewRecordWID()
			if err := as.storeSessionRecordCAS(ps, idHash, smsg.Sequence); err != nil {
				continue
			}
		}
		remaining := time.Until(time.Unix(ps.DisconnectedAt+int64(ps.ExpiryInterval), 0))
		if remaining < 0 {
			remaining = 0
		}
		// Arm only if absent: a timer armed by a live disconnect (or an earlier
		// sweep) is at least as up to date as this record snapshot.
		as.mu.Lock()
		if as.pendingExpiries != nil {
			if _, exists := as.pendingExpiries[idHash]; !exists {
				as.pendingExpiries[idHash] = time.AfterFunc(remaining, func() { as.expireSession(s, idHash) })
				swept++
			}
		}
		as.mu.Unlock()
	}
	if swept > 0 {
		s.Debugf("MQTT session expiry sweep re-armed %v timer(s)", swept)
	}
}

// sweepOrphanedConsumers deletes MQTT-owned durables on the messages stream
// that no session references anymore: a crash between a confirmed record
// save/delete and the consumer teardown (UNSUBSCRIBE, session end, 3.1.1
// forced leave) leaks the durable forever. Consumers are listed BEFORE the
// record walk: a shared join persists its record before (re)creating the
// group durable — and repairs a lost race with a delete by re-issuing the
// idempotent create — so a listed consumer the walk does not reference was
// orphaned at list time. Plain session durables are created before the record
// save that references them, so only consumers older than
// mqttOrphanSweepGrace are eligible; consumers held by local in-memory
// sessions count as referenced too. Any indeterminate scan aborts the sweep
// (fail closed: never delete on a partial view). Runs from the account
// session manager's sweep go routine. Spec5 [4.8.2].
func (as *mqttAccountSessionManager) sweepOrphanedConsumers(s *Server) {
	consumers, err := as.jsa.consumerList(mqttStreamName)
	if err != nil {
		s.Debugf("MQTT orphaned consumer sweep ended: %v", err)
		return
	}
	// Candidates: MQTT-shaped (a shared group durable, or a session durable
	// delivering to "$MQTT.sub.") and older than the grace window. Anything
	// else on the stream is not ours to judge. A consumer the clustered LIST
	// could not report (Missing) is not a candidate until a later sweep sees it.
	cutoff := time.Now().Add(-mqttOrphanSweepGrace)
	candidates := make(map[string]struct{})
	for _, ci := range consumers {
		if ci == nil || ci.Config == nil || !ci.Created.Before(cutoff) {
			continue
		}
		if !strings.HasPrefix(ci.Name, mqttSharedConsumerDurablePrefix) &&
			!strings.HasPrefix(ci.Config.DeliverSubject, mqttSubPrefix) {
			continue
		}
		candidates[ci.Name] = struct{}{}
	}
	if len(candidates) == 0 {
		return
	}
	// A local session's in-memory state may be newer than its persisted record
	// (a save may be in flight); count its consumers as referenced.
	as.mu.RLock()
	sesss := make([]*mqttSession, 0, len(as.sessions))
	for _, sess := range as.sessions {
		sesss = append(sesss, sess)
	}
	as.mu.RUnlock()
	for _, sess := range sesss {
		sess.mu.Lock()
		for _, cc := range sess.cons {
			delete(candidates, cc.Durable)
		}
		// A draining durable is referenced: it exists precisely because started
		// deliveries are still owed on it, and reaping it would delete them with it
		// (the messages stream is interest-retained). See unsub_drain.go.
		for _, dur := range sess.drainDurablesLocked() {
			delete(candidates, dur)
		}
		for key := range sess.sharedSubs {
			for _, dur := range mqttSharedDurablesForKey(key) {
				delete(candidates, dur)
			}
		}
		sess.mu.Unlock()
	}
	// Walk every session record; any referenced durable is not an orphan.
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	for seq := uint64(1); len(candidates) > 0; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT orphaned consumer sweep ended: %v", err)
				return
			}
			break
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			// An undecodable record may still reference candidates: abort.
			s.Warnf("MQTT orphaned consumer sweep: unable to decode session record at sequence %v: %v", smsg.Sequence, err)
			return
		}
		for _, cc := range ps.Cons {
			delete(candidates, cc.Durable)
		}
		for _, pd := range ps.Drains {
			if pd != nil && pd.Cons != nil {
				delete(candidates, pd.Cons.Durable)
			}
		}
		for key := range ps.SharedSubs {
			for _, dur := range mqttSharedDurablesForKey(key) {
				delete(candidates, dur)
			}
		}
	}
	for dur := range candidates {
		if _, err := as.jsa.deleteConsumer(mqttStreamName, dur, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			s.Warnf("MQTT orphaned consumer sweep: unable to delete consumer %q: %v", dur, err)
			continue
		}
		s.Debugf("MQTT orphaned consumer sweep deleted consumer %q", dur)
	}
}

// Helper to create a subscription. It updates the sid and array of subscriptions.
func (as *mqttAccountSessionManager) createSubscription(subject string, cb msgHandler, sid *int64, subs *[]*subscription) error {
	sub, err := as.jsa.c.processSub([]byte(subject), nil, []byte(strconv.FormatInt(*sid, 10)), cb, false)
	if err != nil {
		return err
	}
	*sid++
	*subs = append(*subs, sub)
	return nil
}

// A timer loop to cleanup up expired cached retained messages for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) cleanupRetainedMessageCache(s *Server, closeCh chan struct{}) {
	tt := time.NewTicker(mqttRetainedCacheTTL)
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
			// Set a limit to the number of retained messages to scan since we
			// lock as for it. Since the map enumeration gives random order we
			// should eventually clean up everything.
			i, maxScan := 0, 10*1000
			now := time.Now()
			as.rmsCache.Range(func(key, value any) bool {
				rm := value.(*mqttRetainedMsg)
				// Evict when the cache entry aged out, or when the message
				// itself reached its MQTT 5.0 Message Expiry deadline (its
				// stored copy is TTL-reaped by JetStream at the same time).
				if now.After(rm.expiresFromCache) ||
					(!rm.expires.IsZero() && now.After(rm.expires)) {
					as.rmsCache.Delete(key)
				}
				i++
				return i < maxScan
			})

		case <-closeCh:
			return
		case <-s.quitCh:
			return
		}
	}
}

// mqttJSARequestHeader builds the fixed Nats-Request-Info header prefixed to
// every JS API request for an account.
//
// The ClientInfo names the account and nothing else -- in particular it must
// never carry a Cluster. Upstream set ClientInfo.Cluster from its own cluster
// name so an embedded, clustered JetStream would place assets locally. This
// broker has no cluster of its own: the only value available here is its node id
// (a hostname, or a generated "mnb-<nuid>"), which is not a NATS cluster name
// and would be nonsense to a server reading it for placement.
//
// Nothing was ever actually misrouted by the old code, because
// processInboundClientMsg strips this header before publishing (it is a trusted
// server-internal header, meaningless from an external client). But serializing
// a fabricated cluster name into every request was dead weight waiting to be
// believed the moment that strip moved or a code path bypassed it. The broker is
// a plain client; the server places.
func mqttJSARequestHeader(accName string) []byte {
	b, _ := json.Marshal(ClientInfo{Account: accName})
	hdrStart := bytes.Buffer{}
	hdrStart.WriteString(hdrLine)
	http.Header{ClientInfoHdr: []string{string(b)}}.Write(&hdrStart)
	hdrStart.WriteString(CR_LF)
	hdrStart.WriteString(CR_LF)
	return hdrStart.Bytes()
}

// Loop to send JS API requests for a given MQTT account.
// The closeCh is used by the caller to be able to interrupt this routine
// if the rest of the initialization fails, since the quitCh is really
// only used when the server shutdown.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) sendJSAPIrequests(s *Server, c *client, accName string, closeCh chan struct{}) {
	as.mu.RLock()
	sendq := as.jsa.sendq
	quitCh := as.jsa.quitCh
	acc := c.acc
	as.mu.RUnlock()

	// The account session manager does not have a suhtdown API per-se, instead,
	// we will cleanup things when this go routine exits after detecting that the
	// server is shutdown or the initialization of the account manager failed.
	defer func() {
		as.mu.Lock()
		if as.flapTimer != nil {
			as.flapTimer.Stop()
			as.flapTimer = nil
		}
		// Stop any pending session-expiry timers and disable further
		// scheduling/firing; the nil map makes expireSession a no-op for a timer
		// already mid-callback. Spec5 [3.1.2.11.2].
		for _, t := range as.pendingExpiries {
			t.Stop()
		}
		as.pendingExpiries = nil
		// Drop reconciliation ordering state. Any worker still in flight holds its
		// own *sessionReconcileState pointer and decrements it directly, and its
		// map-identity check finds this nil map (delete on nil is a no-op), so
		// nilling here is safe. New workers cannot start: admitWillWorker is closed.
		as.reconcileWM = nil
		as.mu.Unlock()
	}()

	hdrb := mqttJSARequestHeader(accName)

	for {
		select {
		case <-sendq.ch:
			pmis := sendq.pop()
			for _, r := range pmis {
				var nsize int

				msg := r.msg
				// If r.hdr is set to -1, it means that there is no need for any header.
				if r.hdr != -1 {
					bb := bytes.Buffer{}
					if r.hdr > 0 {
						// This means that the header has been set by the caller and is
						// already part of `msg`, so simply set c.pa.hdr to the given value.
						c.pa.hdr = r.hdr
						nsize = len(msg)
						msg = append(msg, _CRLF_...)
					} else {
						// We need the ClientInfo header, so add it here.
						bb.Write(hdrb)
						c.pa.hdr = bb.Len()
						bb.Write(r.msg)
						nsize = bb.Len()
						bb.WriteString(_CRLF_)
						msg = bb.Bytes()
					}
					c.pa.hdb = []byte(strconv.Itoa(c.pa.hdr))
				} else {
					c.pa.hdr = -1
					c.pa.hdb = nil
					nsize = len(msg)
					msg = append(msg, _CRLF_...)
				}

				c.pa.subject = []byte(r.subj)
				c.pa.reply = []byte(r.reply)
				c.pa.size = nsize
				c.pa.szb = []byte(strconv.Itoa(nsize))
				c.pa.mapped = nil

				if acc.hasMappings() {
					if changed := c.selectMappedSubject(); changed {
						c.traceOutOp("MAPPINGS", fmt.Appendf(nil, "%s -> %s", c.pa.mapped, c.pa.subject))
					}
				}
				c.processInboundClientMsg(msg)
				c.flushClients(0)
			}
			sendq.recycle(&pmis)

		case <-closeCh:
			return
		case <-quitCh:
			return
		}
	}
}

// Add/Replace this message from the retained messages map.
// If a message for this topic already existed, the existing record is updated
// with the provided information.
// Lock not held on entry.
func (as *mqttAccountSessionManager) addRetainedMsg(key string, sseq uint64, rm *mqttRetainedMsg) {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.retmsgs == nil {
		as.retmsgs = stree.NewSubjectTree[mqttRetainedMsgRef]()
	} else {
		// Check if we already had one retained message. If so, update the existing one.
		if erf, exists := as.retmsgs.Find(stringToBytes(key)); exists {
			// Update the stream sequence with the new value.
			erf.sseq = sseq
			// Update the in-memory retained message cache but only for messages
			// that are already in the cache, i.e. have been (recently) used.
			// If that is the case, we ask setCachedRetainedMsg() to make a copy
			// of rm.Msg bytes slice.
			as.setCachedRetainedMsg(key, rm, true, true)
			return
		}
	}
	as.retmsgs.Insert([]byte(key), mqttRetainedMsgRef{sseq: sseq})
}

// Remove the retained message stored with the `subject` key from the map/cache.
// Callers that mean "remove whatever the index currently holds" pass `seq == 0`:
// the retained stream's consumer callback, the publishing broker's synchronous
// local seed (mqttHandlePubRetain), and the decode-failure eviction above. Those
// writers are NOT arbitrated against each other — none of them compares the
// sequence it observed with the one the index holds, so a delayed application
// can remove or restore a generation the index has already moved past (finding
// F-059). A caller that has a specific record in mind passes `seq > 0`, and the
// removal happens only if that exact sequence is still the indexed one.
// This function returns the sequence associated with the existing retained
// message that is being removed (used with `seq == 0`) and returns 0 if the
// retained message was not removed from the map (not found or sequence did not
// match).
func (as *mqttAccountSessionManager) removeRetainedMsg(subject string, seq uint64) uint64 {
	as.mu.Lock()
	defer as.mu.Unlock()
	rm, ok := as.retmsgs.Find(stringToBytes(subject))
	if !ok || (seq > 0 && rm.sseq != seq) {
		if seq == 0 {
			// "Remove whatever is there" has to include the cached copy, which
			// can outlive the index entry: an exact-filter subscription caches
			// unconditionally (loadRetainedMessages) while only the consumer
			// callback and the local seed ever index, so cache-present with
			// index-absent is a reachable state — the first F-059 arm. Returning
			// here without the delete would leave a decode-failure eviction
			// fail-OPEN in exactly that state, since a later add inserts the
			// index without refreshing the cache and the stale predecessor
			// becomes servable again.
			as.rmsCache.Delete(subject)
		}
		return 0
	}
	rm, _ = as.retmsgs.Delete(stringToBytes(subject))
	as.rmsCache.Delete(subject)
	return rm.sseq
}

// First check if this session's client ID is already in the "locked" map,
// which if it is the case means that another client is now bound to this
// session and this should return an error.
// If not in the "locked" map, but the client is not bound with this session,
// then same error is returned.
// Finally, if all checks ok, then the session's ID is added to the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSession(sess *mqttSession, c *client) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	var fail bool
	if _, fail = as.sessLocked[sess.id]; !fail {
		sess.mu.Lock()
		fail = sess.c != c
		sess.mu.Unlock()
	}
	if fail {
		return fmt.Errorf("another session is in use with client ID %q", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// lockSessionForVoidHandoff takes the session gate as NOBODY for a release-triggered
// void-verdict attempt and, when it cannot, gives up the handoff loop's ownership IN THE
// SAME as.mu CRITICAL SECTION.
//
// That atomicity is the whole point, and it closes a lost wakeup the two-step form has: a
// gate owner deletes its sessLocked entry under as.mu and only then asks whether a handoff
// loop is running. Clearing the flag after as.mu was released lets that owner observe a loop
// that is about to exit, suppress its own trigger, and leave the staged revision with no
// successor at all — on an unbound, never-expiring session, indefinitely. Done here instead,
// the owner either released BEFORE this section — in which case this attempt finds the gate
// free and wins it — or reads the flag AFTER it, already clear, and fires its own loop.
//
// Lock order is the established as.mu → sess.mu, same as lockSession.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSessionForVoidHandoff(sess *mqttSession) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	// RESIDENCY by identity, in the same critical section as the gate itself, which is what
	// makes it a claim rather than a stale sample: a trigger's session pointer can be
	// evicted before its turn — a remote persist, an accept rollback, an expiry, a foreign
	// disposition — and that object's record belongs to whoever owns it now. Claiming the
	// shared client-ID gate for it would hold a replacement CONNECT off across a JetStream
	// store for a session this broker no longer has. Identity, not client ID: a CONNECT may
	// already have installed a restored replacement under the same ID.
	fail := as.sessions[sess.id] != sess
	if !fail {
		if _, fail = as.sessLocked[sess.id]; !fail {
			sess.mu.Lock()
			fail = sess.c != nil
			sess.mu.Unlock()
		}
	}
	if fail {
		sess.mu.Lock()
		sess.voidHandoffActive = false
		sess.mu.Unlock()
		return fmt.Errorf("session %q is in use; its void-verdict handoff yields", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// lockSessionForRecordRejection takes the session gate as NOBODY for a periodic
// rejection probe, and it is the SOLE unboundness check on that path.
//
// Residency BY IDENTITY, in the same as.mu critical section as the claim, is what makes
// the probe's foreign classification true: the periodic pass snapshots session
// pointers, and plain lockSession checks only sessLocked and sess.c — so a stale object
// could take the gate after a same-broker replacement had been installed and saved, and
// then read that locally authored record as another owner's and dispose of the live
// session. Identity, not client ID: a CONNECT may already have installed a restored
// replacement under the same ID.
//
// Unbound-only, and that is what makes THIS settlement unbound-only: claiming the gate as
// nobody for a healthy bound session would fail a concurrent SUBSCRIBE's own lockSession,
// and that packet error closes a live connection — a disruption the uncertainty pass only
// inflicts on sessions that are already unusable. So a bound session's marker simply has
// its claim refused here. It is not left unconverged: the bound session settles the marker
// on its OWN readLoop before the next packet is dispatched, claiming the gate as itself
// (mqttGateDispatchOnRejection), and this pass settles whatever survives the unbind. The
// check lives here and nowhere else: a duplicate check in the caller would mask a mutation
// of this one. See sess_rejected_write.go.
//
// Lock order is the established as.mu → sess.mu, same as lockSession.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSessionForRecordRejection(sess *mqttSession) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	fail := as.sessions[sess.id] != sess
	if !fail {
		if _, fail = as.sessLocked[sess.id]; !fail {
			sess.mu.Lock()
			fail = sess.c != nil
			sess.mu.Unlock()
		}
	}
	if fail {
		return fmt.Errorf("session %q is in use; its record-rejection probe yields", sess.id)
	}
	as.sessLocked[sess.id] = struct{}{}
	return nil
}

// lockSessionForTeardown takes the session gate on behalf of a connection that is
// about to end — a close, or a bound record-rejection settlement that has already
// decided not to serve — retrying for as long as that connection is still the
// session's owner.
//
// lockSession fails for two very different reasons: the session has been rebound
// to another connection (a takeover — nothing below is this connection's to do
// any more), or the gate is merely held for a moment by another ownership-gated
// operation, such as an uncertain-record resolution pass. Only the first is a
// reason to give up. Giving up on the second would silently drop this
// connection's whole teardown — including the Will it owes — over contention
// that was about to clear.
//
// Bounded, and mirrors the CONNECT side's own spin (mqttProcessConnect). It
// cannot deadlock: nothing that holds the gate waits on this teardown, and a
// CONNECT registers the gate before it rebinds the session, so a teardown
// waiting on a CONNECT observes the rebind on its next attempt. The settlement
// caller keeps that true by spinning here BEFORE it takes the record fence — a
// waiter holding the fence a gate holder needs would close the cycle.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) lockSessionForTeardown(sess *mqttSession, c *client) error {
	var err error
	for attempt := 0; attempt < mqttTeardownLockAttempts; attempt++ {
		if err = as.lockSession(sess, c); err == nil {
			return nil
		}
		sess.mu.Lock()
		rebound := sess.c != c
		sess.mu.Unlock()
		if rebound {
			return err
		}
		time.Sleep(mqttTeardownLockItvl)
	}
	return err
}

// beforeTeardownLock runs the teardown-ordering test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeTeardownLock(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeTeardownLock
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// beforeTakeoverDiscard runs the takeover-ordering test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeTakeoverDiscard(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeTakeoverDiscard
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// beforeResumeNaks runs the resume-ordering test seam, if one is installed.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) beforeResumeNaks(clientID string) {
	as.mu.RLock()
	hook := as.testBeforeResumeNaks
	as.mu.RUnlock()
	if hook != nil {
		hook(clientID)
	}
}

// Remove the session from the "locked" map.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) unlockSession(sess *mqttSession) {
	as.mu.Lock()
	delete(as.sessLocked, sess.id)
	as.mu.Unlock()
	as.handoffVoidVerdictsOnGateRelease(sess)
}

// handoffVoidVerdictsOnGateRelease is the ownership-gate half of the release handoff: a
// verdict staged while this gate was held could not be persisted by its stager — a
// resolver's fence and the gate itself both refuse it — and an UNBOUND session has no
// packet tail that would come back to it. See voidVerdictHandoffDueLocked.
//
// Both gate-release forms call it, and that is not redundancy: CONNECT releases through its
// own inline delete rather than through unlockSession, so naming only unlockSession would
// leave every CONNECT that ends with the session unbound uncovered.
//
// It must be called with as.mu NOT held. The attempt takes the ownership gate, and its own
// fence release re-enters this decision; doing that under as.mu would deadlock against
// lockSession.
//
// Nonresident objects are skipped, by the attempt's own gate acquisition: a session that has
// been evicted — a foreign disposition, a rollback, an expiry — has no record of ours to
// write, because that record belongs to whoever owns it now, and a verdict about a register
// of a generation nobody resumes is not ours to persist.
func (as *mqttAccountSessionManager) handoffVoidVerdictsOnGateRelease(sess *mqttSession) {
	if sess == nil {
		return
	}
	sess.mu.Lock()
	due := sess.voidVerdictHandoffDueLocked()
	sess.mu.Unlock()
	if !due {
		return
	}
	// Residency is not checked here: the attempt's gate acquisition checks it by identity,
	// in the same critical section that claims the gate, which is the only place a
	// pre-check could not go stale. See lockSessionForVoidHandoff.
	sess.attemptVoidVerdictHandoff(nil)
}

// Simply adds the session to the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to adding to the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) addSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	as.sessions[sess.id] = sess
	as.sessByHash[sess.idHash] = sess
	if lock {
		as.mu.Unlock()
	}
}

// Simply removes the session from the various sessions maps.
// The boolean `lock` indicates if this function should acquire the lock
// prior to removing from the maps.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) removeSession(sess *mqttSession, lock bool) {
	if lock {
		as.mu.Lock()
	}
	delete(as.sessions, sess.id)
	delete(as.sessByHash, sess.idHash)
	// The session is gone, so an entry kept alive only because it was resident has
	// nothing left to be about. Pruning HERE is what bounds the map: an undecided
	// trigger now outlives its worker, so the worker's own cleanup can no longer be
	// the last word. See pruneReconcileEntryLocked, which refuses when a worker is
	// still in flight or when a replacement installed its own entry.
	as.pruneReconcileEntryLocked(sess.idHash, as.reconcileWM[sess.idHash])
	if lock {
		as.mu.Unlock()
	}
}

// removeSessionIfCurrent removes the session from the maps ONLY while they still
// point at this object.
//
// removeSession deletes by client ID and hash unconditionally, which is wrong for
// any late or racing disposition: a CONNECT that has already evicted this object and
// installed a restored replacement would have that replacement deleted instead,
// leaving a live connection with a session no map knows about. Identity is the only
// safe predicate.
//
// No lock held on entry.
// It also cancels the session's expiry timer, in the SAME critical section: the
// timer is keyed by client hash, so cancelling it before knowing whether this object
// is still the resident one would silently disarm a replacement session's
// finite-expiry deadline — which the periodic sweep would eventually re-arm, but not
// before that session had outlived the interval it was promised. Removal and
// cancellation are one decision about one identity, so they belong under one lock.
func (as *mqttAccountSessionManager) removeSessionIfCurrent(sess *mqttSession) bool {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.sessions[sess.id] != sess {
		return false
	}
	delete(as.sessions, sess.id)
	if as.sessByHash[sess.idHash] == sess {
		delete(as.sessByHash, sess.idHash)
	}
	as.cancelSessionExpiryLocked(sess.idHash)
	// Same reason as removeSession's prune, and the helper's own residency check is
	// what keeps this correct when the hash map still points at a replacement.
	as.pruneReconcileEntryLocked(sess.idHash, as.reconcileWM[sess.idHash])
	return true
}

// Helper to set the sub's mqtt fields and possibly serialize (pre-loaded)
// retained messages.
//
// Session lock held on entry. Acquires the subs lock and holds it for
// the duration. Non-MQTT messages coming into mqttDeliverMsgCbQoS0 will be
// waiting.
func (sess *mqttSession) processQOS12Sub(
	c *client, // subscribing client.
	subject, sid, queue []byte, isReserved bool, qos byte, noLocal, retainAsPublished, shared bool, subID int, jsDurName string, h msgHandler, // subscription parameters.
) (*subscription, error) {
	return sess.processSub(c, subject, sid, queue, isReserved, qos, noLocal, retainAsPublished, shared, subID, jsDurName, h, false, nil, false, nil)
}

func (sess *mqttSession) processSub(
	c *client, // subscribing client.
	subject, sid, queue []byte, isReserved bool, qos byte, noLocal, retainAsPublished, shared bool, subID int, jsDurName string, h msgHandler, // subscription parameters.
	initShadow bool, // do we need to scan for shadow subscriptions? (not for QOS1+)
	rms map[string]*mqttRetainedMsg, // preloaded rms (can be empty, or missing items if errors)
	trace bool, // trace serialized retained messages in the log?
	as *mqttAccountSessionManager, // needed only for rms serialization.
) (*subscription, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		if elapsed > mqttProcessSubTooLong {
			c.Warnf("Took too long to process subscription for %q: %v", subject, elapsed)
		}
	}()

	// Hold subsMu to prevent QOS0 messages callback from doing anything until
	// the (MQTT) sub is initialized.
	sess.subsMu.Lock()
	defer sess.subsMu.Unlock()

	sub, err := c.processSub(subject, queue, sid, h, false)
	if err != nil {
		// c.processSub already called c.Errorf(), so no need here.
		return nil, err
	}
	subs := []*subscription{sub}
	if initShadow {
		subs = append(subs, sub.shadow...)
	}
	for _, ss := range subs {
		if ss.mqtt == nil {
			// reserved is set only once and once the subscription has been
			// created it can be considered immutable.
			ss.mqtt = &mqttSub{
				reserved: isReserved,
			}
		}
		// QOS and jsDurName can be changed on an existing subscription, so
		// accessing it later requires a lock.
		ss.mqtt.qos = qos
		ss.mqtt.noLocal = noLocal
		ss.mqtt.retainAsPublished = retainAsPublished
		ss.mqtt.subID = subID
		ss.mqtt.jsDur = jsDurName
		ss.mqtt.shared = shared
		// A (re)configured subscription is live; clear any prior teardown mark.
		ss.mqtt.closed = false
	}

	if len(rms) > 0 {
		// Only deal with retained messages for the normal subscription,
		// not the shadow one (which is for a different account and subject).
		as.serializeRetainedMsgsForSub(rms, sess, c, sub, trace)
	}

	return sub, nil
}

// mqttUpdateFilterOptSet adds or removes filter from a per-option filter set to
// match want, allocating the set on first add. Returns the (possibly new) set
// and whether it changed, so the caller can flag the session for persistence.
func mqttUpdateFilterOptSet(set map[string]struct{}, filter string, want bool) (map[string]struct{}, bool) {
	if _, has := set[filter]; want && !has {
		if set == nil {
			set = make(map[string]struct{})
		}
		set[filter] = struct{}{}
		return set, true
	} else if !want && has {
		delete(set, filter)
		return set, true
	}
	return set, false
}

// mqttUpdateFilterSubID records (or, when id is 0, removes) the v5 Subscription
// Identifier for filter, allocating the map on first add. Returns the (possibly
// new) map and whether it changed, so the caller can flag the session for
// persistence.
func mqttUpdateFilterSubID(m map[string]int, filter string, id int) (map[string]int, bool) {
	if id != 0 {
		if cur, has := m[filter]; !has || cur != id {
			if m == nil {
				m = make(map[string]int)
			}
			m[filter] = id
			return m, true
		}
	} else if _, has := m[filter]; has {
		delete(m, filter)
		return m, true
	}
	return m, false
}

// mqttFilterSetToList returns a subscription-filter set as a slice for
// persistence (nil when empty, to omit the JSON field). Used for the per-option
// filter sets (No Local, Retain As Published).
func mqttFilterSetToList(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	l := make([]string, 0, len(set))
	for f := range set {
		l = append(l, f)
	}
	return l
}

// mqttFilterSetFromList rebuilds a subscription-filter set from a persisted
// slice (nil when empty).
func mqttFilterSetFromList(l []string) map[string]struct{} {
	if len(l) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(l))
	for _, f := range l {
		set[f] = struct{}{}
	}
	return set
}

// mqttShouldSendRetained reports whether retained messages must be replayed for
// a new subscription with the given v5 Retain Handling option. Spec5 [3.8.3.1]:
// 0 always sends, 1 sends only when the filter has no current subscription, 2
// never sends. subs is the session's pre-SUBSCRIBE filter set.
func mqttShouldSendRetained(retainHandling byte, subs map[string]byte, filter string) bool {
	switch retainHandling {
	case 2:
		return false
	case 1:
		_, existed := subs[filter]
		return !existed
	default:
		return true
	}
}

// Process subscriptions for the given session/client.
//
// When `fromSubProto` is false, it means that this is invoked from the CONNECT
// protocol, when restoring subscriptions that were saved for this session.
// In that case, there is no need to update the session record.
//
// When `fromSubProto` is true, it means that this call is invoked from the
// processing of the SUBSCRIBE protocol, which means that the session needs to
// be updated. It also means that if a subscription on same subject with same
// QoS already exist, we should not be recreating the subscription/JS durable,
// since it was already done when processing the CONNECT protocol.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
// tok identifies the record transaction this whole packet runs in: membership
// changes, the affine provisioning save, the per-filter rollbacks and the final
// update are one transaction, so no other writer can persist a half-applied
// SUBSCRIBE. The CONNECT restore path passes its own transaction's token down.
// See sess_recordtx.go.
func (as *mqttAccountSessionManager) processSubs(sess *mqttSession, c *client, tok recordTx,
	filters []*mqttFilter, fromSubProto, trace bool) ([]*subscription, error) {

	// Hold the subs barrier for the whole SUBSCRIBE (or CONNECT session
	// restore) processing: concurrent QoS 1/2 stores wait in
	// mqttInitiateMsgDeliveryJSA until the JetStream consumers created here
	// have registered interest with the messages stream, so a racing publish
	// is never discarded by interest-based retention. Deadlock-free: nothing
	// reachable from here stores to $MQTT_msgs, and a publisher blocked in
	// wait() holds no locks.
	as.jsa.subsBarrier.enter()
	defer as.jsa.subsBarrier.leave()

	// Helper to determine if we need to create a separate top-level
	// subscription for a wildcard. sid is derived from the passed-in sid (which
	// for a shared subscription embeds the ShareName) rather than from the
	// subject, so the level-up sid stays in the same namespace as its parent.
	fwc := func(subject, sid string) (bool, string, string) {
		if !mqttNeedSubForLevelUp(subject) {
			return false, _EMPTY_, _EMPTY_
		}
		// Say subject is "foo.>", remove the ".>" so that it becomes "foo"
		fwcsubject := subject[:len(subject)-2]
		// Change the sid to "foo fwc" (for a shared sub, "$share/g/foo fwc")
		fwcsid := sid[:len(sid)-2] + mqttMultiLevelSidSuffix

		return true, fwcsubject, fwcsid
	}

	rmSubjects := map[string]uint64{}
	// Whether this session already had a session-affine QoS 2 delivery path, taken
	// BEFORE the loop below can provision one. It is captured here rather than
	// alongside the subscription snapshot further down because the loop provisions
	// it, and a rollback must be able to tell "this packet built it" from "it was
	// already there". Spec5 [4.8.2].
	var q2AffPrePacket bool
	if fromSubProto {
		sess.mu.Lock()
		q2AffPrePacket = sess.q2AffGen != _EMPTY_
		sess.mu.Unlock()
	}
	// Preload retained messages for all requested subscriptions.  Also, since
	// it's the first iteration over the filter list, do some cleanup.
	for _, f := range filters {
		// A filter flagged with a failure reason at parse time (e.g. a shared
		// subscription) is not processed; the code becomes the SUBACK byte.
		if f.reason >= mqttSubAckFailure {
			f.qos = f.reason
			continue
		}
		if f.qos > 2 {
			f.qos = 2
		}
		if c.mqtt.downgradeQoS2Sub && f.qos == 2 {
			c.Warnf("Downgrading subscription QoS2 to QoS1 for %q, as configured", f.filter)
			f.qos = 1
		}

		// Deny the whole reserved "$MQTT.>" namespace, not just the delivery
		// subjects. Upstream rejected only $MQTT.sub., $MQTT.deliver.pubrel. and
		// $MQTT.qsub., with a TODO asking why it bothered when "one can subscribe
		// to `#` and it'll include everything". The answer: a wildcard filter
		// cannot reach a $-topic ([MQTT-4.7.2-1], enforced at delivery by
		// mqttMustIgnoreForReservedSub), but an *explicit* "$MQTT/..." filter is
		// not "reserved" by that rule and is delivered normally.
		//
		// That matters here because the broker's own state moves as ordinary NATS
		// publishes on these subjects: every QoS 1/2 PUBLISH is stored to
		// $MQTT.msgs.<subject> and every retained message to $MQTT.rmsgs.<key>,
		// alongside $MQTT.sess.*, $MQTT.JSA.*, $MQTT.willq.*, $MQTT.willcancel.*,
		// $MQTT.qos2.in.* and $MQTT.out.*. So a client subscribing to
		// "$MQTT/msgs/#" would read every QoS 1/2 message in the account
		// regardless of its own per-topic NATS permissions -- a read-side bypass
		// of its subscribe ACL. Deny the prefix outright; the three delivery
		// subjects are covered by it.
		//
		// Defense in depth: the NATS account should ALSO deny subscribe on
		// "$MQTT.>" (the broker's internal subscriptions live on the service
		// connection, so that deny is safe -- see processSub). The broker must not
		// depend on that config being correct.
		//
		// The restore path cannot reach this: a record carrying such a filter is
		// refused before it becomes a session (mqttReservedMembershipInRecord), so
		// this denial only ever answers a live SUBSCRIBE.
		if strings.HasPrefix(f.filter, mqttPrefix) {
			f.qos = mqttSubAckFailure
			f.reasonStr = "cannot subscribe to internal server subject"
			continue
		}

		if f.qos == 2 {
			if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
				c.Errorf("failed to initialize PUBREL processing: %v", err)
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				continue
			}
			// A SHARED QoS 2 subscription additionally needs this session's affine
			// delivery path, because a started delivery is moved off the group
			// durable onto it. Provisioned only for shared filters: a plain QoS 2
			// subscription never moves anything and must not be charged an extra
			// consumer. Unlike the PUBREL consumer above, it participates in this
			// packet's rollback (q2AffPrePacket): it costs a consumer, a
			// MaxAckPending charge and a persisted generation, none of which a
			// SUBSCRIBE that never took effect should leave behind. Spec5 [4.8.2];
			// see qos2_shared_affine.go.
			if f.shared {
				if err := sess.ensureQ2AffineConsumerSubscription(c, tok); err != nil {
					c.Errorf("failed to initialize session-affine QoS 2 delivery: %v", err)
					f.qos = mqttSubAckFailure
					f.reasonStr = err.Error()
					continue
				}
			}
		}

		// Shared subscriptions never receive retained messages. Spec5 [4.8.2].
		if f.shared {
			continue
		}
		// Find retained messages. The v5 Retain Handling option controls whether
		// they are replayed at subscribe time. sess.subs still holds the
		// pre-SUBSCRIBE state here (sess.update runs at the end), so it is the
		// right source for the "subscription already exists" test. Spec5 [3.8.3.1].
		if fromSubProto && mqttShouldSendRetained(f.retainHandling, sess.subs, f.filter) {
			as.addRetainedSubjectsForSubject(rmSubjects, f.filter)
			if need, subject, _ := fwc(f.filter, f.filter); need {
				as.addRetainedSubjectsForSubject(rmSubjects, subject)
			}
			// A literal (non-wildcard) filter names one exact subject: force it
			// into rmSubjects even if the in-memory retained-subjects index
			// (as.retmsgs, populated asynchronously by processRetainedMsg
			// reacting to the JetStream store) doesn't have it yet, so
			// loadRetainedMessages' authoritative "last message for subject"
			// stream query below still runs for it. Without this, a retained
			// message published shortly before this SUBSCRIBE can race ahead of
			// that index and be missed entirely. Wildcard filters still rely on
			// the index — there is no cheap "list subjects" stream query to
			// enumerate matches against. Spec5 [3.8.3.1].
			if _, ok := rmSubjects[f.filter]; !ok && !strings.ContainsAny(f.filter, "*>") {
				rmSubjects[f.filter] = 0
			}
		}
	}

	var rms map[string]*mqttRetainedMsg
	if len(rmSubjects) > 0 {
		// Make the best effort to load retained messages.
		rms = as.loadRetainedMessages(rmSubjects, c)
	}

	// Small helper to add the consumer config to the session.
	//
	// Under sess.mu, like every other writer of cons: the map is read under that
	// lock by paths that run concurrently with a SUBSCRIBE or a CONNECT restore
	// — the orphan sweep, the record builder, the resume reconciliation phase —
	// so an unlocked write here is a data race on session state, not merely a
	// stale read. Never called with the lock already held (the two call sites sit
	// between the per-filter processSub calls, which take it internally).
	addJSConsToSess := func(sid string, cc *ConsumerConfig) {
		if cc == nil {
			return
		}
		sess.mu.Lock()
		if sess.cons == nil {
			sess.cons = make(map[string]*ConsumerConfig)
		}
		sess.cons[sid] = cc
		sess.mu.Unlock()
	}

	// Snapshot pre-SUBSCRIBE membership AND consumer entries so a final-save
	// failure can restore both exactly (only meaningful when persisting, i.e.
	// fromSubProto). Spec5 [4.8.2].
	var snap *mqttSubSnapshot
	var consSnap map[string]*ConsumerConfig
	if fromSubProto {
		snap = sess.snapshotSubs()
		sess.mu.Lock()
		consSnap = make(map[string]*ConsumerConfig, len(sess.cons))
		for k, v := range sess.cons {
			consSnap[k] = v
		}
		sess.mu.Unlock()
	}
	// Irreversible teardown of consumers removed by a QoS downgrade in this
	// packet is deferred until the final session save is confirmed, so a failed
	// save can restore the pre-packet consumer instead of having lost its
	// durable/cursor. Spec5 [4.8.2].
	type deferredDel struct {
		sid string
		cc  *ConsumerConfig
	}
	var deferredDels []deferredDel
	// Registrations this packet withdraws, cancelled inside its own transaction, and the
	// delivery subscriptions closed so nothing can allocate behind that snapshot.
	var stagedCancels []mqttRegCancellation
	var closedDeliveries []*subscription
	// Verdicts on TRANSMITTED registers, withdrawn if this packet (or this filter) rolls
	// back: those deliveries stay live and still owe their identifiers.
	var voidedSent []string

	var err error
	subs := make([]*subscription, 0, len(filters))
	for _, f := range filters {
		// Skip what's already been identified as a failure (0x80 or any v5
		// error reason code).
		if f.qos >= mqttSubAckFailure {
			continue
		}
		subject := f.filter
		bsubject := []byte(subject)
		sid := f.sessionKey()
		bsid := []byte(sid)
		isReserved := isMQTTReservedSubscription(subject)

		// For a shared subscription the raw QoS0 subscription is a NATS queue
		// subscription so a QoS0-published message reaches exactly one group
		// member. The queue name is derived (with the level-up subject handled
		// below) and doubles as the JS consumer DeliverGroup. Spec5 [4.8.2].
		var queue []byte
		if f.shared {
			q, _, _ := mqttSharedNames(f.group, subject)
			queue = []byte(q)
		}

		// Retain Handling is per-subscription, but the preloaded rms is shared
		// across all filters in this SUBSCRIBE and matched against each sub's
		// subject at serialization time. Gating only the preload would still leak
		// a retained message into an RH=1/2 filter when an overlapping filter
		// (e.g. a wildcard with RH=0) caused that subject to be loaded. So pass
		// rms only to filters that must replay; nil otherwise. sess.subs is
		// unchanged until sess.update at the end, so this matches the preload
		// decision. Shared subscriptions never replay retained. Spec5 [3.8.3.1], [4.8.2].
		subRMS := rms
		if f.shared || !mqttShouldSendRetained(f.retainHandling, sess.subs, f.filter) {
			subRMS = nil
		}

		// For a shared subscription persist membership BEFORE creating the group
		// consumer, so a concurrent leaver's last-holder scan (possibly on another
		// server) sees this joiner. Snapshot the prior state so a later failure of
		// this filter restores it rather than leaving a phantom member. Spec5 [4.8.2].
		var priorQoS byte
		var priorExisted, priorRap bool
		var priorSubID int
		if fromSubProto && f.shared {
			priorQoS, priorExisted = sess.sharedSubs[sid]
			if priorExisted {
				_, priorRap = sess.rapSubs[sid]
				priorSubID = sess.subIDs[sid]
			}
			wasUncertain := sess.isSeqUncertain()
			if err := sess.update(tok, []*mqttFilter{f}, true); err != nil {
				if sess.classifySave(wasUncertain, err) != saveDefinitelyFailed {
					// POSSIBLY LANDED. Continuing would be worse than failing: the final
					// update() skips filters marked failed, so the client would get a
					// failure code for a membership that is durably stored, and the rest
					// of the packet would run on a session whose record state is unknown
					// (every later save is refused while it is unresolved). Abandon the
					// packet with the attempted state intact — that is what the resolver
					// re-attempts and proves durable — and let the connection close. The
					// membership then simply stands, SUBACK or not, which is the outcome a
					// landed original would have produced anyway; nothing was deferred for
					// want of proof, so there is no landed continuation to register.
					// Spec5 [4.8.2]. See sess_uncertain.go.
					c.Errorf("MQTT: shared membership write for %q is unresolved; abandoning the SUBSCRIBE with its state intact: %v", sess.id, err)
					return nil, err
				}
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				// update() mutated the in-memory maps before its save failed; revert
				// them so a later save in this packet cannot persist a phantom
				// membership. Spec5 [4.8.2].
				sess.restoreSharedKeyInMemory(sid, priorExisted, priorQoS, priorRap, priorSubID)
				continue
			}
		}

		var jscons *ConsumerConfig
		var jssub *subscription

		// Note that if a subscription already exists on this subject, the
		// existing sub is returned. Need to update the qos.
		var sub *subscription
		var err error

		// Deferred consumer deletes recorded by THIS filter, so a later per-filter
		// failure can cancel them (restore the sess.cons entry, keep the durable).
		filterDefStart := len(deferredDels)
		// And the same boundary for what the deferral staged: the delivery barriers it
		// lowered, and the registrations it withdrew. A filter that FAILS keeps its
		// original QoS 1/2 membership — the final update simply skips it — so a barrier
		// left down would silently stop that subscription delivering until the client
		// reconnects.
		filterClosedStart := len(closedDeliveries)
		filterVoidedStart := len(voidedSent)
		cancelFilterDeferred := func() {
			// The sess.cons entries were never removed (removal happens at
			// execution time, after the confirmed save), so cancelling is just
			// dropping this filter's descriptors.
			deferredDels = deferredDels[:filterDefStart]
			sess.reopenTeardownDelivery(closedDeliveries[filterClosedStart:])
			closedDeliveries = closedDeliveries[:filterClosedStart]
			sess.unvoidRegNonces(voidedSent[filterVoidedStart:])
			voidedSent = voidedSent[:filterVoidedStart]
		}

		const processShadowSubs = true

		as.mu.Lock()
		sess.mu.Lock()
		sub, err = sess.processSub(c,
			bsubject, bsid, queue, isReserved, f.qos, f.noLocal, f.retainAsPublished, f.shared, f.subID, // main subject
			_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
			processShadowSubs,
			subRMS, trace, as)
		sess.mu.Unlock()
		as.mu.Unlock()

		if err != nil {
			f.qos = mqttSubAckFailure
			f.reasonStr = err.Error()
			cancelFilterDeferred()
			doCleanup, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorQoS, priorRap, priorSubID)
			if rerr != nil {
				// Rollback persistence not confirmed: hard-error WITHOUT releasing
				// resources, so we never delete a group durable the record may still
				// claim membership in. Spec5 [4.8.2].
				return subs, rerr
			}
			if doCleanup {
				sess.cleanupFailedSub(c, tok, sub, jscons, jssub)
			}
			continue
		}

		// This will create (if not already exist) a JS consumer for
		// subscriptions of QoS >= 1. But if a JS consumer already exists and
		// the subscription for same subject is now a QoS==0, then the JS
		// consumer will be deleted. Shared subscriptions always join the group
		// consumer (even at QoS0, so stored messages are delivered at QoS0).
		if f.shared {
			jscons, jssub, err = sess.processSharedJSConsumer(c, f.group, subject, sid, f.qos, f.retainAsPublished, f.subID, fromSubProto)
		} else {
			var dd *ConsumerConfig
			jscons, jssub, dd, err = sess.processJSConsumer(c, tok, subject, sid, f.qos, f.noLocal, f.retainAsPublished, f.subID, fromSubProto)
			if dd != nil {
				deferredDels = append(deferredDels, deferredDel{sid: sid, cc: dd})
				// The teardown itself is deferred to the confirmed save, but the
				// CANCELLATION is not: it belongs in the write that downgrades this
				// filter, or the identifier it withdraws has nothing to recover it.
				// The delivery barrier comes first, so nothing allocates a further
				// identifier behind the snapshot; a rollback reopens it, because the
				// QoS 1/2 subscription is then still the client's.
				sess.mu.Lock()
				ddCancels, ddSub, ddSent := sess.stageDeferredDeleteLocked(c, dd)
				sess.mu.Unlock()
				stagedCancels = append(stagedCancels, ddCancels...)
				voidedSent = append(voidedSent, ddSent...)
				if ddSub != nil {
					closedDeliveries = append(closedDeliveries, ddSub)
				}
			}
		}
		if err != nil {
			f.qos = mqttSubAckFailure
			f.reasonStr = err.Error()
			cancelFilterDeferred()
			doCleanup, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorQoS, priorRap, priorSubID)
			if rerr != nil {
				// Rollback persistence not confirmed: hard-error WITHOUT releasing
				// resources, so we never delete a group durable the record may still
				// claim membership in. Spec5 [4.8.2].
				return subs, rerr
			}
			if doCleanup {
				sess.cleanupFailedSub(c, tok, sub, jscons, jssub)
			}
			continue
		}

		// Process the wildcard subject if needed.
		if need, fwcsubject, fwcsid := fwc(subject, sid); need {
			var fwjscons *ConsumerConfig
			var fwjssub *subscription
			var fwcsub *subscription
			var fwcqueue []byte
			if f.shared {
				q, _, _ := mqttSharedNames(f.group, fwcsubject)
				fwcqueue = []byte(q)
			}

			// See note above about existing subscription.
			as.mu.Lock()
			sess.mu.Lock()
			fwcsub, err = sess.processSub(c,
				[]byte(fwcsubject), []byte(fwcsid), fwcqueue, isReserved, f.qos, f.noLocal, f.retainAsPublished, f.shared, f.subID, // FWC (top-level wildcard) subject
				_EMPTY_, mqttDeliverMsgCbQoS0, // no jsDur for QOS0
				processShadowSubs,
				subRMS, trace, as)
			sess.mu.Unlock()
			as.mu.Unlock()
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				cancelFilterDeferred()
				doCleanup, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorQoS, priorRap, priorSubID)
				if rerr != nil {
					return subs, rerr
				}
				if doCleanup {
					sess.cleanupFailedSub(c, tok, sub, jscons, jssub)
				}
				continue
			}

			if f.shared {
				fwjscons, fwjssub, err = sess.processSharedJSConsumer(c, f.group, fwcsubject, fwcsid, f.qos, f.retainAsPublished, f.subID, fromSubProto)
			} else {
				var fwdd *ConsumerConfig
				fwjscons, fwjssub, fwdd, err = sess.processJSConsumer(c, tok, fwcsubject, fwcsid, f.qos, f.noLocal, f.retainAsPublished, f.subID, fromSubProto)
				if fwdd != nil {
					// The same staging as the main half, through the same helper: the
					// companion is a consumer in its own right and owes exactly what it
					// does.
					deferredDels = append(deferredDels, deferredDel{sid: fwcsid, cc: fwdd})
					sess.mu.Lock()
					fwCancels, fwSub, fwSent := sess.stageDeferredDeleteLocked(c, fwdd)
					sess.mu.Unlock()
					stagedCancels = append(stagedCancels, fwCancels...)
					voidedSent = append(voidedSent, fwSent...)
					if fwSub != nil {
						closedDeliveries = append(closedDeliveries, fwSub)
					}
				}
			}
			if err != nil {
				// c.processSub already called c.Errorf(), so no need here.
				f.qos = mqttSubAckFailure
				f.reasonStr = err.Error()
				cancelFilterDeferred()
				doCleanup, rerr := sess.rollbackSharedSub(c, tok, f, fromSubProto, priorExisted, priorQoS, priorRap, priorSubID)
				if rerr != nil {
					return subs, rerr
				}
				if doCleanup {
					sess.cleanupFailedSub(c, tok, sub, jscons, jssub)
					sess.cleanupFailedSub(c, tok, fwcsub, fwjscons, fwjssub)
				}
				continue
			}

			subs = append(subs, fwcsub)
			addJSConsToSess(fwcsid, fwjscons)
		}

		subs = append(subs, sub)
		addJSConsToSess(sid, jscons)
	}

	if fromSubProto {
		wasUncertain := sess.isSeqUncertain()
		if err = sess.update(tok, filters, true); err != nil {
			// The session record could not be persisted. Rather than leave a
			// partially-applied SUBSCRIBE live with no SUBACK sent, tear down every
			// subscription and consumer newly created in this packet, restore the
			// pre-packet membership and consumer entries (deferred downgrade deletes
			// were never executed, so their durables are intact), then return the
			// error so the connection closes. Spec5 [4.8.2].
			//
			// ONLY when the write definitely did not land. An ambiguous one may have
			// stored exactly the state this packet built, so the packet's state is KEPT
			// and the resolver settles it by re-attempting those very bytes — the
			// subscriptions stand, with or without a SUBACK, exactly as they would have
			// had the original landed with its response lost. Nothing is torn down
			// meanwhile either: the consumers and subscriptions this packet created may
			// be precisely what the stored record references. The connection closes
			// regardless, and the session stays unresolved until the resolver classifies
			// it. See sess_uncertain.go.
			if sess.classifySave(wasUncertain, err) == saveMaybeLanded {
				// Not resolved here: the packet's transaction is still open and the
				// resolver must hold the fence. The readLoop resolves after it closes.
				// See mqttResolveAfterPacket.
				c.Errorf("MQTT: SUBSCRIBE record write for %q is unresolved; keeping the packet's state: %v", sess.id, err)
				return nil, err
			}
			sess.reopenTeardownDelivery(closedDeliveries)
			sess.unvoidRegNonces(voidedSent)
			sess.rollbackSubscribePacket(c, tok, subs, consSnap, snap, q2AffPrePacket)
			sess.secureRegCancellations(c, tok, stagedCancels)
		} else {
			// Save confirmed: now run the irreversible teardown of consumers that
			// this packet downgraded to QoS0.
			// Durable in the packet's record already; this lets the registers go now
			// rather than at the next resume.
			sess.secureRegCancellations(c, tok, stagedCancels)
			for _, dd := range deferredDels {
				sess.execDeferredConsumerDelete(c, tok, dd.sid, dd.cc)
			}
			// The affine path is provisioned in the first loop, before any filter is
			// known to succeed, so a packet whose shared QoS 2 filters all failed
			// per-filter (a failed membership persist, a failed group-consumer
			// create, a failed subscription) would otherwise keep a consumer, a
			// generation and a MaxAckPending charge for a subscription that does not
			// exist. Only ever unwinds a path THIS packet built. Spec5 [4.8.2].
			//
			// Not while the session is UNRESOLVED. The confirming save below is
			// refused at entry then, so the unwind would terminate the generation's
			// assignments — which nothing undoes — and then restore a path it never
			// managed to remove. The write that left the session unresolved is the
			// provisioning save itself, and it registered a continuation that owns
			// this reclaim and runs it under the resolver's token once the record is
			// proven. See unwindProvisionedQ2AffineOnLanded.
			if !q2AffPrePacket && !sess.isSeqUncertain() && !sess.hasSharedQoS2Sub() {
				// Same commit order as the rollback path: detach, persist a record
				// that no longer names it, and only then delete.
				// disposeNow: this site's failure semantics are unchanged — a failed
				// claim cleanup aborts the unwind and leaves the path usable. Moving it
				// to dispose-after-confirm is filed as its own §0 item, because the
				// rollback around it has distinct failure semantics of its own.
				if u := sess.prepareQ2AffineUnwind(c, true); u.prepared {
					outcome, serr := sess.saveClassified(tok)
					switch {
					case serr == nil:
						sess.finishQ2AffineUnwind(c, u)
					case outcome == saveDefinitelyFailed:
						// Nothing stored, so the session must go back to describing the
						// resources that are still live.
						sess.restoreQ2AffineUnwind(u)
						c.Errorf("MQTT: unable to persist the removal of an unused affine QoS 2 path for %q: %v", sess.id, serr)
					default:
						// Ambiguous: the record may already omit the path. Keep the
						// detached view (it matches what may have landed) but delete
						// nothing — the resources stay live until the resolver proves the
						// record no longer names them, which is what authorizes the
						// delete.
						sess.retainOnLanded(func(recordTx) { sess.finishQ2AffineUnwind(c, u) })
						c.Errorf("MQTT: the removal of an unused affine QoS 2 path for %q is unresolved: %v", sess.id, serr)
						sess.resolveUncertainNow(c)
					}
				}
			}
		}
	}

	return subs, err
}

// rollbackSharedSub undoes the early membership persistence done for a shared
// filter whose subscribe subsequently failed. A re-SUBSCRIBE of a filter that
// existed before this packet is restored to its prior QoS/RAP/subID and its
// membership kept (returns run=false, so the caller does NOT release the shared
// consumer); a newly-added filter is removed and the caller must release its
// resources (run=true). For non-shared filters or the CONNECT restore path this
// is a no-op that just asks the caller to run its normal cleanup. If the rollback
// persist fails it returns a non-nil error so the caller escalates to a hard
// processing error rather than leaving a phantom persisted membership. Spec5 [4.8.2].
//
// This is itself a mutate-before-save caller — update() changes the membership maps
// and then persists them — so it owes the same outcome discipline as every other
// one. A DEFINITE failure means the record still holds the previously persisted
// membership, so the compensating mutation must be undone or memory and record
// diverge and a later save writes the wrong one. An AMBIGUOUS failure keeps it: the
// record may already carry the rolled-back state, and the readback recognises this
// session by a digest of exactly those bytes. See sess_uncertain.go.
func (sess *mqttSession) rollbackSharedSub(c *client, tok recordTx, f *mqttFilter, fromSubProto, priorExisted bool, priorQoS byte, priorRap bool, priorSubID int) (run bool, err error) {
	if !fromSubProto || !f.shared {
		return true, nil
	}
	// The state as it stands NOW — what this packet attempted and what the record
	// currently holds — so a definite failure can put it back exactly.
	sid := f.sessionKey()
	sess.mu.Lock()
	curQoS, curExisted := sess.sharedSubs[sid]
	_, curRap := sess.rapSubs[sid]
	curSubID := sess.subIDs[sid]
	sess.mu.Unlock()

	// Sampled BEFORE the write. It matters here more than anywhere: this function
	// runs on a failure path, so the session may ALREADY be unresolved from an
	// earlier ambiguous write in the same packet — in which case update()'s save is
	// refused at entry, which is a definite failure, and the compensating mutation
	// must be undone to leave the earlier attempt's bytes in place for its readback.
	// Without the sample, that refusal would read as "this write may have landed".
	wasUncertain := sess.isSeqUncertain()
	// The no-prior branch below REMOVES this membership, and its group durables are
	// released once that write is known durable — so their registers are voided in the same
	// write, with the delivery barrier down first. The prior-existing branch restores an
	// earlier membership instead: nothing is abandoned there, so nothing is voided.
	var voidedSent []string
	var closed []*subscription
	var sharedCancels []mqttRegCancellation
	if !priorExisted {
		sess.mu.Lock()
		for _, dur := range mqttSharedDurablesForKey(sid) {
			for _, cc := range sess.cons {
				if cc != nil && cc.Durable == dur {
					if sub := sess.closeTeardownDeliveryLocked(c, cc); sub != nil {
						closed = append(closed, sub)
					}
				}
			}
			cancels, sent := sess.voidRegistrationsLocked(dur)
			sharedCancels = append(sharedCancels, cancels...)
			voidedSent = append(voidedSent, sent...)
		}
		sess.mu.Unlock()
	}
	restoreCur := func() {
		sess.restoreSharedKeyInMemory(sid, curExisted, curQoS, curRap, curSubID)
		// The membership is back, so the durables are not abandoned after all.
		sess.reopenTeardownDelivery(closed)
		sess.unvoidRegNonces(voidedSent)
	}
	undo := func(e error) {
		switch sess.classifySave(wasUncertain, e) {
		case saveDefinitelyFailed:
			restoreCur()
		case saveMaybeLanded:
			// Once the compensating write is PROVEN durable the record no longer names
			// this filter, but the caller has already returned without releasing its
			// resources — so the release is owed here, from the resolver. Nothing else
			// would do it, and the durable plus its MaxAckPending charge would outlive
			// the membership.
			sess.retainOnLanded(func(rtok recordTx) { sess.releaseFailedSharedFilter(c, rtok, f) })
		}
	}

	if priorExisted {
		restore := &mqttFilter{shared: true, group: f.group, filter: f.filter, qos: priorQoS, retainAsPublished: priorRap, subID: priorSubID}
		if e := sess.update(tok, []*mqttFilter{restore}, true); e != nil {
			undo(e)
			return false, e
		}
		// A failed re-SUBSCRIBE must keep delivering with the OLD state: the
		// processSub/processSharedJSConsumer refresh already overwrote the live
		// subscription's QoS/RAP/subID with the attempted values, so restore them.
		// Spec5 [4.8.2].
		sess.restoreSharedLiveMetadata(c, f, priorQoS, priorRap, priorSubID)
		return false, nil
	}
	if e := sess.update(tok, []*mqttFilter{f}, false); e != nil {
		undo(e)
		return true, e
	}
	// The removal is durable, so a tombstone can release the cancelled registers now
	// instead of leaving them to the next resume.
	sess.secureRegCancellations(c, tok, sharedCancels)
	return true, nil
}

// addedCons is one consumer a SUBSCRIBE packet created, paired with its sid.
type addedCons struct {
	sid string
	cc  *ConsumerConfig
}

// releaseFailedSharedFilter releases the group durables of a shared filter whose
// subscribe failed and whose membership removal is now known to be durable.
//
// It is the cleanup rollbackSharedSub's callers perform when they can see the
// removal persisted; when that write was ambiguous they return without doing it, so
// this is what a landed readback resumes. A durable another member still holds is
// left alone.
//
// No lock held on entry.
func (sess *mqttSession) releaseFailedSharedFilter(c *client, tok recordTx, f *mqttFilter) {
	durs := mqttSharedDurablesForKey(f.sessionKey())
	if len(durs) == 0 {
		return
	}
	held := sess.sharedDurablesHeldByOthers(durs)
	for _, dur := range durs {
		if held[dur] {
			continue
		}
		sess.mu.Lock()
		var cc *ConsumerConfig
		for sid, cfg := range sess.cons {
			if cfg != nil && cfg.Durable == dur {
				cc = cfg
				delete(sess.cons, sid)
				break
			}
		}
		sess.mu.Unlock()
		if cc != nil {
			sess.releaseSharedConsumer(c, tok, cc, nil)
			continue
		}
		if _, err := sess.jsa.deleteConsumer(mqttStreamName, dur, true); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			c.Errorf("MQTT: unable to release shared consumer %q after a failed subscribe: %v", dur, err)
		}
	}
}

// restoreSharedLiveMetadata resets the live QoS/RAP/subID on a shared filter's
// raw-subject subscription, its level-up subscription (if any), and its group
// deliver subscription to the given prior values, after a failed re-SUBSCRIBE
// refreshed them to the attempted values. Spec5 [4.8.2].
func (sess *mqttSession) restoreSharedLiveMetadata(c *client, f *mqttFilter, qos byte, rap bool, subID int) {
	sid := f.sessionKey()
	_, deliver, _ := mqttSharedNames(f.group, f.filter)
	setFields := func(csid string) {
		c.mu.Lock()
		sub := c.subs[csid]
		c.mu.Unlock()
		if sub == nil {
			return
		}
		sess.mu.Lock()
		sess.subsMu.Lock()
		if sub.mqtt != nil {
			sub.mqtt.qos = qos
			sub.mqtt.retainAsPublished = rap
			sub.mqtt.subID = subID
		}
		sess.subsMu.Unlock()
		sess.mu.Unlock()
	}
	setFields(sid)     // raw-subject sub
	setFields(deliver) // group deliver sub
	if mqttNeedSubForLevelUp(f.filter) {
		setFields(sid[:len(sid)-2] + mqttMultiLevelSidSuffix)
		_, ldeliver, _ := mqttSharedNames(f.group, f.filter[:len(f.filter)-2])
		setFields(ldeliver)
	}
}

// mqttSubSnapshot captures the pre-SUBSCRIBE state of the session's subscription
// maps so a failed packet can be reverted exactly. Spec5 [4.8.2].
type mqttSubSnapshot struct {
	subs    map[string]byte
	shared  map[string]byte
	rap     map[string]struct{}
	noLocal map[string]struct{}
	subIDs  map[string]int
	// drains and drainByDur, because a re-SUBSCRIBE ADOPTS a draining consumer:
	// it removes the drain and moves the durable into cons. The consumer snapshot
	// alone cannot undo that — restoring cons without the drain leaves a session
	// holding neither, while the record still carries the drain, and the next save
	// then erases a durable that is still owed started deliveries.
	drains     map[string]*mqttDrain
	drainByDur map[string]*mqttDrain
}

// snapshotSubs shallow-copies every subscription map under the session lock.
func (sess *mqttSession) snapshotSubs() *mqttSubSnapshot {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	s := &mqttSubSnapshot{
		subs:    make(map[string]byte, len(sess.subs)),
		shared:  make(map[string]byte, len(sess.sharedSubs)),
		rap:     make(map[string]struct{}, len(sess.rapSubs)),
		noLocal: make(map[string]struct{}, len(sess.noLocalSubs)),
		subIDs:  make(map[string]int, len(sess.subIDs)),
	}
	for k, v := range sess.subs {
		s.subs[k] = v
	}
	for k, v := range sess.sharedSubs {
		s.shared[k] = v
	}
	for k := range sess.rapSubs {
		s.rap[k] = struct{}{}
	}
	for k := range sess.noLocalSubs {
		s.noLocal[k] = struct{}{}
	}
	for k, v := range sess.subIDs {
		s.subIDs[k] = v
	}
	if len(sess.drains) > 0 {
		s.drains = make(map[string]*mqttDrain, len(sess.drains))
		for k, v := range sess.drains {
			s.drains[k] = v
		}
	}
	if len(sess.drainByDur) > 0 {
		s.drainByDur = make(map[string]*mqttDrain, len(sess.drainByDur))
		for k, v := range sess.drainByDur {
			s.drainByDur[k] = v
		}
	}
	return s
}

// restoreSharedKeyInMemory reverts the in-memory shared-subscription maps for one
// session key to the given prior state, without persisting. Used when an early
// membership persist failed (update mutates the maps before it saves). Spec5 [4.8.2].
func (sess *mqttSession) restoreSharedKeyInMemory(sid string, existed bool, qos byte, rap bool, subID int) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if existed {
		if sess.sharedSubs == nil {
			sess.sharedSubs = make(map[string]byte)
		}
		sess.sharedSubs[sid] = qos
	} else {
		delete(sess.sharedSubs, sid)
	}
	sess.rapSubs, _ = mqttUpdateFilterOptSet(sess.rapSubs, sid, existed && rap)
	sess.subIDs, _ = mqttUpdateFilterSubID(sess.subIDs, sid, mqttZeroIf(!existed, subID))
}

// mqttZeroIf returns 0 when cond is true, else v. Helper for map-key clears.
func mqttZeroIf(cond bool, v int) int {
	if cond {
		return 0
	}
	return v
}

// rollbackSubscribePacket undoes every runtime and persisted change made by a
// SUBSCRIBE whose final session-record save failed. It restores the pre-packet
// subscription maps and re-persists them; only if that restore save is confirmed
// does it tear down the subscriptions and consumers that were NEWLY created in
// this packet — so a group durable is never deleted while the persisted record
// might still claim membership. A re-SUBSCRIBE of a pre-existing filter keeps its
// membership and runtime state. The connection is closing regardless. Spec5 [4.8.2].
func (sess *mqttSession) rollbackSubscribePacket(c *client, tok recordTx, subs []*subscription, consSnap map[string]*ConsumerConfig, snap *mqttSubSnapshot, q2AffPrePacket bool) {
	// Restore EVERYTHING the packet mutated — the membership maps AND sess.cons —
	// before the confirm-save, so the record is written from a fully rolled-back
	// view: a QoS0->QoS1 upgrade's new consumer must not be persisted under the
	// restored QoS0 subs key, and a QoS1->QoS0 downgrade's restored subscription
	// must be persisted WITH its pre-existing consumer config (its durable is
	// intact because downgrade deletes are deferred until a confirmed save, which
	// did not happen). The pre-packet consumer snapshot is authoritative for these
	// QoS transitions, which the membership maps alone cannot express. Spec5 [4.8.2].
	var added []addedCons
	sess.mu.Lock()
	for sid, cc := range sess.cons {
		if _, ok := consSnap[sid]; ok {
			continue
		}
		if cc != nil && snap.drainByDur[cc.Durable] != nil {
			// ADOPTED, not created. Its durable predates this packet and is being
			// handed back to the drain below, so it must not be counted among the
			// packet's own consumers — those get torn down once the rolled-back
			// record is confirmed, which here would delete a durable that still owes
			// started deliveries, taking them with it.
			continue
		}
		added = append(added, addedCons{sid, cc})
	}
	// The consumers this packet created are about to be released, and the restored record is
	// written just below — so their registers are voided HERE, in that same write. Afterwards
	// the membership is the pre-packet one, which never named these durables, so nothing
	// would be left to explain a register that outlives them. The delivery barrier goes down
	// first, for the reason given in closeTeardownDeliveryLocked: a callback still live
	// during this snapshot would allocate behind it.
	var rollbackCancels []mqttRegCancellation
	var rollbackClosed []*subscription
	var rollbackVoided []string
	for _, a := range added {
		if a.cc == nil {
			continue
		}
		if sub := sess.closeTeardownDeliveryLocked(c, a.cc); sub != nil {
			rollbackClosed = append(rollbackClosed, sub)
		}
		cancels, sent := sess.voidRegistrationsLocked(a.cc.Durable)
		rollbackCancels = append(rollbackCancels, cancels...)
		rollbackVoided = append(rollbackVoided, sent...)
	}
	sess.subs = snap.subs
	sess.sharedSubs = snap.shared
	sess.rapSubs = snap.rap
	sess.noLocalSubs = snap.noLocal
	sess.subIDs = snap.subIDs
	sess.cons = consSnap
	// A re-SUBSCRIBE that ADOPTED a drain moved its durable out of drains and into
	// cons. Restoring cons alone would leave the session holding neither, while the
	// record still carries the drain — and the next save would then erase a durable
	// that still owes started deliveries. Restore both maps together.
	sess.drains, sess.drainByDur = snap.drains, snap.drainByDur
	sess.mu.Unlock()
	// Detach a session-affine QoS 2 path this packet provisioned, BEFORE the
	// confirm-save: the restored record must not name a consumer that is about to
	// be deleted. The deletion itself waits for that save to be confirmed, exactly
	// as the packet's other consumers do. A path that predates the packet is left
	// alone. Spec5 [4.8.2].
	var q2AffUnwind mqttQ2AffUnwind
	if !q2AffPrePacket {
		// disposeNow: unchanged here too; see the note at the unused-path unwind.
		q2AffUnwind = sess.prepareQ2AffineUnwind(c, true)
	}
	outcome, err := sess.saveClassified(tok)
	if err != nil {
		// Could not confirm the rolled-back record. Do not release/delete anything
		// (a durable could be orphaned while the record still claims membership);
		// put the packet's consumer entries back so the in-memory view matches the
		// still-live resources, and let a reconnect reconcile from the record.
		//
		// An AMBIGUOUS write is different: the rolled-back record may be exactly
		// what is stored. Re-adding the packet's consumers would then diverge from what
		// landed, so the rolled-back view stays and only the still-live resources are
		// spared; the resolver settles it by re-attempting those bytes, and the release
		// runs when it does.
		readd := func() {
			sess.mu.Lock()
			if sess.cons == nil {
				sess.cons = make(map[string]*ConsumerConfig)
			}
			for _, a := range added {
				sess.cons[a.sid] = a.cc
			}
			sess.mu.Unlock()
			// These consumers are still live, so they must deliver again. (The voided
			// registers stay voided: a withdrawn allocation is never reinstated — the
			// source is simply re-offered under a fresh one — and a transmitted register
			// source is simply re-offered under a fresh one.)
			sess.reopenTeardownDelivery(rollbackClosed)
			sess.unvoidRegNonces(rollbackVoided)
			// Same reasoning for the affine path: nothing was deleted, so the session
			// must go back to describing the resources that are still live.
			sess.restoreQ2AffineUnwind(q2AffUnwind)
		}
		switch outcome {
		case saveDefinitelyFailed:
			readd()
		case saveMaybeLanded:
			// Once this rollback's own write is proven durable the record already
			// describes the rolled-back session and the releases below are owed —
			// nothing else resumes them, so the packet's consumers and their charges
			// would be leaked.
			sess.retainOnLanded(func(rtok recordTx) {
				sess.finishSubscribeRollback(c, rtok, subs, added, rollbackCancels, snap, q2AffUnwind)
			})
		}
		c.Errorf("MQTT: unable to persist rolled-back session after SUBSCRIBE failure: %v", err)
		return
	}
	sess.finishSubscribeRollback(c, tok, subs, added, rollbackCancels, snap, q2AffUnwind)
}

// finishSubscribeRollback releases what a SUBSCRIBE packet created, once the
// rolled-back record is CONFIRMED. Split out so a readback that later proves an
// ambiguous rollback write landed can resume it: otherwise the record describes the
// rolled-back session while the packet's consumers, their MaxAckPending charges and
// its raw subscriptions stay behind, referenced by nothing.
//
// No lock held on entry.
func (sess *mqttSession) finishSubscribeRollback(c *client, tok recordTx, subs []*subscription, added []addedCons,
	cancels []mqttRegCancellation, snap *mqttSubSnapshot, q2AffUnwind mqttQ2AffUnwind) {
	// Durable in the rolled-back record already; the tombstone is cleanup.
	sess.secureRegCancellations(c, tok, cancels)
	sess.finishQ2AffineUnwind(c, q2AffUnwind)

	// A raw subscription "existed before" this packet if the restored membership
	// still holds its filter. The level-up sid ("<mainsid without .>> fwc") maps
	// back to the main membership key ("<...>.>").
	existedBefore := func(sid string) bool {
		key := sid
		if strings.HasSuffix(key, mqttMultiLevelSidSuffix) {
			key = strings.TrimSuffix(key, mqttMultiLevelSidSuffix) + ".>"
		}
		if strings.HasPrefix(key, string(mqttSharedSubPrefix)) {
			_, ok := snap.shared[key]
			return ok
		}
		_, ok := snap.subs[key]
		return ok
	}
	for _, sub := range subs {
		if sub != nil && !existedBefore(string(sub.sid)) {
			c.processUnsub(sub.sid)
		}
	}
	for _, a := range added {
		if a.cc.DeliverGroup != _EMPTY_ {
			sess.releaseSharedConsumer(c, tok, a.cc, nil)
		} else {
			sess.execDeferredConsumerDelete(c, tok, a.sid, a.cc)
		}
	}
}

// Retained publish messages matching this subscription are serialized in the
// subscription's `prm` mqtt writer. This buffer will be queued for outbound
// after the subscription is processed and SUBACK is sent or possibly when
// server processes an incoming published message matching the newly
// registered subscription.
//
// Runs from the client's readLoop.
// Account session manager lock held on entry.
// Session lock held on entry.
func (as *mqttAccountSessionManager) serializeRetainedMsgsForSub(rms map[string]*mqttRetainedMsg, sess *mqttSession, c *client, sub *subscription, trace bool) {
	if as.retmsgs.Size() == 0 || len(rms) == 0 {
		return
	}
	toTrace := []mqttPublish{}
	as.retmsgs.Match(sub.subject, func(subj []byte, _ *mqttRetainedMsgRef) {
		rm := rms[string(subj)]
		if rm == nil {
			// This should not happen since we pre-load messages into rms before
			// calling serialize.
			return
		}
		// MQTT 5.0 Message Expiry Interval: an expired retained message must not be
		// delivered. The stored copy is reaped by its per-message TTL; this guards
		// the window where the in-memory ref/cache still points at it. Spec5
		// [3.3.2.3.3].
		if !rm.expires.IsZero() && !time.Now().Before(rm.expires) {
			return
		}
		// No Local: do not replay a retained message this same session published.
		// The stored SessionHash is the authenticated origin marker; recompute it
		// for this session+subject+payload and suppress only on an authentic
		// match. Spec5 [3.8.3.1].
		if sub.mqtt.noLocal {
			expected := mqttOriginMarker(c.srv.mqtt.originKey, sess.idHash, string(subj), rm.Msg)
			if mqttNoLocalSuppress(true, rm.SessionHash, expected) {
				return
			}
		}
		// A broad wildcard subscription can overlap a subscribe deny clause.
		c.mu.Lock()
		denied := c.mperms != nil && c.checkDenySub(string(subj))
		c.mu.Unlock()
		if denied {
			return
		}
		var pi uint16
		qos := min(mqttGetQoS(rm.Flags), sub.mqtt.qos)
		if c.mqtt.rejectQoS2Pub && qos == 2 {
			c.Warnf("Rejecting retained message with QoS2 for subscription %q, as configured", sub.subject)
			return
		}
		if qos > 0 {
			pi = sess.trackPublishRetained()

			// If we failed to get a PI for this message, send it as a QoS0, the
			// best we can do?
			if pi == 0 {
				qos = 0
			}
		}

		// MQTT 5.0 Message Expiry Interval: a v5 subscriber must receive the
		// interval reduced by the time the retained message has been waiting in the
		// server. Spec5 [3.3.2.3.3]. 3.1.1 subscribers get no properties section.
		v5 := c.mqtt.proto == mqttProtoLevel5
		props := rm.Props
		if v5 && !rm.expires.IsZero() {
			rem := uint32(time.Until(rm.expires).Seconds())
			if rem < 1 {
				rem = 1
			}
			props = mqttSetMessageExpiry(rm.Props, rem)
		}
		// MQTT 5.0 Subscription Identifier: inject the subscription's identifier
		// (this path builds the header directly, bypassing mqttEnqueuePublishMsgTo).
		// After the expiry rewrite and before the size check below. Spec5 [3.8.2.1.2].
		if v5 && sub.mqtt.subID > 0 {
			props = mqttInjectSubID(props, sub.mqtt.subID)
		}

		// Need to use the subject for the retained message, not the `sub` subject.
		// We can find the published retained message in rm.sub.subject.
		// Set the RETAIN flag: [MQTT-3.3.1-8].
		// Retained frames are intentionally not aliased: they are buffered into
		// sub.mqtt.prm here (under the session lock) and flushed later under cc.mu,
		// so binding an alias here could order the binding after live frames already
		// queued. A full topic is always legal; the first live message binds instead.
		flags, headerBytes := mqttMakePublishHeader(pi, qos, false, true, v5, []byte(rm.Topic), props, len(rm.Msg))

		// MQTT 5.0 Maximum Packet Size: skip a retained message that would exceed
		// the client's limit rather than send an oversized packet (which a strict
		// client rejects as a protocol error). Discarding is spec-correct: the
		// message is treated as delivered. Release the packet identifier if one
		// was allocated (the session lock is held on entry). Spec5 [3.1.2.11.4].
		if mp := c.mqtt.maxPacketSize; mp != 0 && len(headerBytes)+len(rm.Msg) > int(mp) {
			if pi != 0 {
				sess.untrackPublish(pi)
			}
			return
		}
		c.mu.Lock()
		sub.mqtt.prm = append(sub.mqtt.prm, headerBytes, rm.Msg)
		c.mu.Unlock()
		if trace {
			toTrace = append(toTrace, mqttPublish{
				topic: []byte(rm.Topic),
				flags: flags,
				pi:    pi,
				sz:    len(rm.Msg),
			})
		}
	})
	for _, pp := range toTrace {
		c.traceOutOp("PUBLISH", []byte(mqttPubTrace(&pp)))
	}
}

// Appends the stored message subjects for all retained message records that
// match the given subscription's `subject` (which could have wildcards).
//
// Account session manager NOT lock held on entry.
func (as *mqttAccountSessionManager) addRetainedSubjectsForSubject(list map[string]uint64, topSubject string) {
	as.mu.RLock()
	defer as.mu.RUnlock()

	if as.retmsgs.Size() == 0 {
		return
	}

	as.retmsgs.Match(stringToBytes(topSubject), func(subj []byte, ret *mqttRetainedMsgRef) {
		subject := string(subj)
		if _, ok := list[subject]; ok {
			return
		}
		if seq := ret.sseq; seq > 0 {
			list[subject] = seq
		}
	})
}

type warner interface {
	Warnf(format string, v ...any)
}

// Loads a list of retained messages given a list of stored message subjects.
func (as *mqttAccountSessionManager) loadRetainedMessages(subjects map[string]uint64, w warner) map[string]*mqttRetainedMsg {
	rms := make(map[string]*mqttRetainedMsg, len(subjects))
	ss := []string{}
	for s := range subjects {
		if rm := as.getCachedRetainedMsg(s); rm != nil {
			rms[s] = rm
		} else {
			ss = append(ss, mqttRetainedMsgsStreamSubject+s)
		}
	}

	if len(ss) == 0 {
		return rms
	}

	// Although we have the stream sequence for a given subject, we still use
	// the load with "last for subject" because it will cover the cases where a
	// new retained message has arrived since we collected the subject/seq pair.
	// If we were doing a load "by seq" and the message is not found, we would
	// incorrectly remove the retained message from our map.
	results, err := as.jsa.loadLastMsgForMulti(mqttRetainedMsgsStreamName, ss)
	// If an error occurred, warn, but then proceed with what we got.
	if err != nil {
		w.Warnf("error loading retained messages: %v", err)
	}
	for i, result := range results {
		if result == nil {
			continue // skip requests that timed out
		}
		if err := result.ToError(); err != nil {
			// Skip the "$MQTT.rmsgs." prefix...
			subj := ss[i][len(mqttRetainedMsgsStreamSubject):]
			if IsNatsErr(err, JSNoMessageFoundErr) {
				// If there is no message for that subject, delete from our map.
				// The good thing here is that we handle the race where a retained
				// message may just arrive and be replacing it in the map. The
				// removeRetainedMsg() function below will not remove if the sequence
				// does not match.
				seq := subjects[subj]
				as.removeRetainedMsg(subj, seq)
				// Expected when a per-message TTL reaped an expired retained
				// message; the stale index entry is cleaned up, no warning.
				continue
			}
			w.Warnf("failed to load retained message for subject %q: %v", subj, err)
			continue
		}
		rm, err := mqttDecodeRetainedMessage(result.Message.Subject, result.Message.Header, result.Message.Data)
		if err != nil {
			// Unlikely that we can recover from that, so remove the message.
			// (see comment above if failing to load the message).
			subj := ss[i][len(mqttRetainedMsgsStreamSubject):]
			seq := subjects[subj]
			as.removeRetainedMsg(subj, seq)
			w.Warnf("failed to decode retained message for subject %q: %v", subj, err)
			continue
		}
		if len(rm.Msg) == 0 {
			// An empty body is a delete tombstone (see mqttHandlePubRetain): the
			// "last message for subject" query can return one directly on a cold
			// load (no live server has processed it via processRetainedMsg yet to
			// evict it from the index), so it must be treated the same as "no
			// retained message" here, not delivered as an empty PUBLISH.
			subj := ss[i][len(mqttRetainedMsgsStreamSubject):]
			as.removeRetainedMsg(subj, 0)
			continue
		}

		// Add the loaded retained message to the cache, and to the results map.
		// We don't need setCachedRetainedMsg() to clone the `rm.Msg` bytes slice
		// since we own it.
		as.setCachedRetainedMsg(rm.Subject, rm, false, false)
		rms[rm.Subject] = rm
	}
	return rms
}

// Composes a NATS message for a storeable mqttRetainedMsg.
// An empty body is itself the deletion tombstone [MQTT-3.3.1-11]; the flags are
// encoded plainly either way, and every reader is expected to decode both.
func mqttEncodeRetainedMessage(rm *mqttRetainedMsg) (natsMsg []byte, headerLen int) {
	delRM := len(rm.Msg) == 0

	// MQTT 5.0 Message Expiry Interval: for a live retained message with an
	// expiry, carry the absolute deadline (so the remaining interval can be
	// recomputed on replay) plus a JetStream per-message TTL for the remaining
	// lifetime (so the stored copy is dropped once it expires). Tombstones
	// (empty payload) never carry expiry. Spec5 [3.3.2.3.3].
	var expStr, ttlStr string
	if !delRM && !rm.expires.IsZero() {
		expStr = strconv.FormatInt(rm.expires.UnixNano(), 10)
		// Round the remaining lifetime up so JetStream never deletes the retained
		// copy before its deadline (truncating a 1.99s remainder to 1s would drop
		// it almost a second early). At least 1s, JetStream's minimum.
		secs := int64((time.Until(rm.expires) + time.Second - 1) / time.Second)
		if secs < 1 {
			secs = 1
		}
		ttlStr = strconv.FormatInt(secs, 10)
	}

	// No need to encode the subject, we can restore it from topic.
	l := len(hdrLine)
	l += len(mqttNatsRetainedMessageTopic) + 1 + len(rm.Topic) + 2 // 1 byte for ':', 2 bytes for CRLF
	if rm.Origin != _EMPTY_ {
		l += len(mqttNatsRetainedMessageOrigin) + 1 + len(rm.Origin) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if rm.Source != _EMPTY_ {
		l += len(mqttNatsRetainedMessageSource) + 1 + len(rm.Source) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if rm.SessionHash != _EMPTY_ {
		l += len(mqttNatsRetainedMessageSessionHash) + 1 + len(rm.SessionHash) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if len(rm.Props) > 0 {
		l += len(mqttNatsRetainedMessageProps) + 1 + base64.StdEncoding.EncodedLen(len(rm.Props)) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if expStr != _EMPTY_ {
		l += len(mqttNatsRetainedMessageExpiry) + 1 + len(expStr) + 2 // 1 byte for ':', 2 bytes for CRLF
	}
	if ttlStr != _EMPTY_ {
		l += len(JSMessageTTL) + 1 + len(ttlStr) + 1 + 2 // 1 byte for ':', 1 for 's', 2 bytes for CRLF
	}
	l += len(mqttNatsRetainedMessageFlags) + 1 + 2 + 2 // 1 byte for ':', 2 bytes for the flags, 2 bytes for CRLF
	l += 2                                             // 2 bytes for the extra CRLF after the header
	if !delRM {
		l += len(rm.Msg)
	}

	buf := bytes.NewBuffer(make([]byte, 0, l))

	buf.WriteString(hdrLine)

	buf.WriteString(mqttNatsRetainedMessageTopic)
	buf.WriteByte(':')
	buf.WriteString(rm.Topic)
	buf.WriteString(_CRLF_)

	buf.WriteString(mqttNatsRetainedMessageFlags)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatUint(uint64(rm.Flags), 16))
	buf.WriteString(_CRLF_)

	if rm.Origin != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageOrigin)
		buf.WriteByte(':')
		buf.WriteString(rm.Origin)
		buf.WriteString(_CRLF_)
	}
	if rm.Source != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageSource)
		buf.WriteByte(':')
		buf.WriteString(rm.Source)
		buf.WriteString(_CRLF_)
	}
	if rm.SessionHash != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageSessionHash)
		buf.WriteByte(':')
		buf.WriteString(rm.SessionHash)
		buf.WriteString(_CRLF_)
	}
	if len(rm.Props) > 0 {
		buf.WriteString(mqttNatsRetainedMessageProps)
		buf.WriteByte(':')
		buf.WriteString(base64.StdEncoding.EncodeToString(rm.Props))
		buf.WriteString(_CRLF_)
	}

	if expStr != _EMPTY_ {
		buf.WriteString(mqttNatsRetainedMessageExpiry)
		buf.WriteByte(':')
		buf.WriteString(expStr)
		buf.WriteString(_CRLF_)
	}
	if ttlStr != _EMPTY_ {
		buf.WriteString(JSMessageTTL)
		buf.WriteByte(':')
		buf.WriteString(ttlStr)
		buf.WriteByte('s')
		buf.WriteString(_CRLF_)
	}

	// End of header, finalize
	buf.WriteString(_CRLF_)
	headerLen = buf.Len()
	buf.Write(rm.Msg)
	return buf.Bytes(), headerLen
}

func mqttSliceHeaders(headers map[string][]byte, hdr []byte) {
	// Skip the hdrLine
	if !bytes.HasPrefix(hdr, stringToBytes(hdrLine)) {
		return
	}
	crLFAsBytes := stringToBytes(CR_LF)
	for i := len(hdrLine); i < len(hdr); {
		// Search for key/val delimiter. `del` is an offset INTO hdr[i:], not an
		// absolute index — comparing it against `i` (as this once did) aborts the
		// scan whenever a key's length happens to equal the current position, and
		// silently discards every header from there on. The first header sits at
		// i == len(hdrLine) == 10, so a ten-character key such as "Nmqtt-RExp"
		// triggered it; NATS header order is map order, so it bit roughly one
		// retained message in seven that carried an MQTT 5.0 expiry. A zero-length
		// key is `del == 0`, and it is caught by the empty-key check below in any
		// case.
		del := bytes.IndexByte(hdr[i:], ':')
		// Not found or key is length 0, we stop.
		if del < 0 || del == 0 {
			break
		}
		keyStart := i
		// Walk back to remove spaces between the key and ':' if applicable.
		index := keyStart + del - 1
		for index > keyStart && hdr[index] == ' ' {
			index--
		}
		key := hdr[keyStart : index+1]
		// If what we had is only spaces, we stop.
		if len(key) == 0 {
			break
		}
		i += del + 1
		valStart := i
		// Search for `\r\n`.
		nl := bytes.Index(hdr[valStart:], crLFAsBytes)
		// If we don't find, we stop.
		if nl < 0 {
			break
		}
		// Look if the caller is interested in this key.
		if _, ok := headers[bytesToString(key)]; ok {
			index := valStart
			// Remove possible spaces between the ':' and the value.
			for index < valStart+nl && hdr[index] == ' ' {
				index++
			}
			// Create a slice and limit capacity to the value range.
			val := hdr[index : valStart+nl : valStart+nl]
			// Record in the caller's map the value for this key.
			headers[bytesToString(key)] = val
		}
		// Reposition to past the `\r\n`.
		i += nl + 2
	}
}

// Decodes a retained message based on the content of the header `h`.
// The returned `*mqttRetainedMsg` object will hold a reference to `m`.
// If the buffer `m` is not owned by the caller, it is the caller
// responsibility to make a copy of the byte slice.
func mqttDecodeRetainedMessage(subject string, h, m []byte) (*mqttRetainedMsg, error) {
	headers := map[string][]byte{
		mqttNatsRetainedMessageOrigin:      nil,
		mqttNatsRetainedMessageFlags:       nil,
		mqttNatsRetainedMessageSource:      nil,
		mqttNatsRetainedMessageProps:       nil,
		mqttNatsRetainedMessageExpiry:      nil,
		mqttNatsRetainedMessageSessionHash: nil,
	}
	// Retrieve the values for the above headers.
	mqttSliceHeaders(headers, h)
	// Get the flag header. It is the one header that identifies the record as
	// one this broker wrote: without it the stored bytes are in no format this
	// broker produces, and are rejected rather than interpreted as another one.
	fHeader := headers[mqttNatsRetainedMessageFlags]
	if len(fHeader) == 0 {
		return nil, errMQTTRetainedMsgUnsupportedFormat
	}
	flagsUint, err := strconv.ParseUint(bytesToString(fHeader), 16, 8)
	if err != nil {
		// Since the error is currently not reported in the server, we
		// will simply replace with this one.
		return nil, errMQTTInvalidRetainFlags
	}
	rm := &mqttRetainedMsg{
		Flags:       byte(flagsUint),
		Origin:      string(headers[mqttNatsRetainedMessageOrigin]),
		Source:      string(headers[mqttNatsRetainedMessageSource]),
		SessionHash: string(headers[mqttNatsRetainedMessageSessionHash]),
		Msg:         m,
	}
	// MQTT 5.0 properties block (base64). Absent for messages published without
	// properties. Validate before keeping it: the retained stream could contain a
	// crafted Nmqtt-RProps from a non-MQTT publisher, and rm.Props is later
	// written verbatim into outbound PUBLISH packets.
	if enc := headers[mqttNatsRetainedMessageProps]; len(enc) > 0 {
		if raw, err := base64.StdEncoding.DecodeString(bytesToString(enc)); err == nil {
			rm.Props = mqttValidateForwardProps(raw)
		}
	}
	// MQTT 5.0 Message Expiry Interval: absolute deadline (Unix nanoseconds).
	if exp := headers[mqttNatsRetainedMessageExpiry]; len(exp) > 0 {
		if ns, err := strconv.ParseInt(bytesToString(exp), 10, 64); err == nil && ns > 0 {
			rm.expires = time.Unix(0, ns)
		}
	}
	// Now check that the values are correct.
	//
	// For "Flags", anything at or above binary (1111) is too big.
	if rm.Flags >= mqttPacketFlagMask {
		return nil, errMQTTInvalidRetainFlags
	}
	if qos := mqttGetQoS(rm.Flags); qos > 2 {
		return nil, errMQTTInvalidRetainFlags
	}
	// We store `Topic` in the retained message because we used to store
	// all retained messages under the same subject `$MQTT_rmsgs` in
	// the retained messages stream. That is no longer the case, and to
	// cover setups where the retained message stream is sourced from another
	// account and has some subject transforms, simply reconstruct the
	// topic/subject based on the `subject` passed to this function.
	rm.Subject = strings.TrimPrefix(subject, mqttRetainedMsgsStreamSubject)
	rm.Topic = bytesToString(natsSubjectStrToMQTTTopic(rm.Subject))
	return rm, nil
}

// mqttReservedMembershipInRecord returns the first subscription key in the
// record that lies in the reserved "$MQTT.>" namespace, looking in Subs,
// SharedSubs, Cons and Drains. Such a key can only have been written by a
// development broker predating the namespace deny (processSubs has rejected
// those filters ever since, and buildPersistedSessionLocked persists only what
// the session holds), so the record's schema is UNSUPPORTED and restoration
// fails closed before any name is derived from it — no migration, no
// half-honored record. CONTRIBUTING.md forbids migration code before the first
// release; the operator recreates the bridge-owned state instead.
//
// Such a record is almost certainly also generationless, since the deny predates
// generation-scoped naming — which is why the caller runs this check BEFORE the
// generation one. A reserved key alongside a VALID generation is a shape no writer
// could produce (a broker new enough to mint generations already denied the filter);
// it is refused all the same, because a fail-closed schema boundary is not owed a
// provenance story.
//
// Read-only, and deliberately narrow: it checks the key SHAPES a pre-deny
// writer could emit, not the record's schema in general. A shared key that does
// not parse is skipped at restore anyway, and mqttPersistedDrain.Subject is not
// independently checked — no writer, pre- or post-deny, can produce a drain
// whose key is harmless while its subject is reserved (drains postdate the deny
// entirely), and defending against a hand-tampered record is general schema
// validation rather than this boundary.
func mqttReservedMembershipInRecord(ps *mqttPersistedSession) (string, bool) {
	// A plain filter and a level-up sid both start with the prefix; a shared key
	// ("$share/g/$MQTT.msgs.>") never does, so its underlying subject is tested.
	isReserved := func(key string) bool {
		if strings.HasPrefix(key, mqttPrefix) {
			return true
		}
		if _, subject, ok := mqttParseSharedKey(key); ok {
			return strings.HasPrefix(subject, mqttPrefix)
		}
		return false
	}
	// Cons and Drains are keyed by sid, so a record can carry a reserved consumer
	// with no membership left to name it: the old in-place migration was driven by
	// the memberships alone and would leave those behind.
	for _, m := range []map[string]byte{ps.Subs, ps.SharedSubs} {
		for key := range m {
			if isReserved(key) {
				return key, true
			}
		}
	}
	for key := range ps.Cons {
		if isReserved(key) {
			return key, true
		}
	}
	for key := range ps.Drains {
		if isReserved(key) {
			return key, true
		}
	}
	return _EMPTY_, false
}

// Creates the session stream (limit msgs of 1) for this client ID if it does
// not already exist. If it exists, recover the single record to rebuild the
// state of the session. If there is a session record but this session is not
// registered in the runtime of this server, then a request is made to the
// owner to close the client associated with this session since specification
// [MQTT-3.1.4-2] specifies that if the ClientId represents a Client already
// connected to the Server then the Server MUST disconnect the existing client.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (as *mqttAccountSessionManager) createOrRestoreSession(clientID string, opts *Options) (*mqttSession, bool, error) {
	jsa := &as.jsa

	hash := getHash(clientID)
	smsg, err := jsa.loadSessionMsg(as.domainTk, hash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return nil, false, fmt.Errorf("loading session record: %w", err)
		}
		// Message not found, so reate the session...
		// Create a session and indicate that this session did not exist.
		sess := mqttSessionCreate(jsa, clientID, hash, mqttNewSessGen(), 0, _EMPTY_, _EMPTY_, opts)
		sess.asm = as
		sess.domainTk = as.domainTk
		return sess, false, nil
	}
	// We need to recover the existing record now.
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		return nil, false, fmt.Errorf("unmarshal of session record at sequence %v: %w", smsg.Sequence, err)
	}
	if ps.ID != clientID {
		return nil, false, errMQTTSessionCollision
	}

	// A record carrying a subscription in the reserved "$MQTT.>" namespace has an
	// unsupported schema and is REFUSED here, ahead of every other branch: the
	// teardown adoption below copies durable names straight out of Cons / Drains and
	// derives shared durables from every SharedSubs key (mqttTeardownTargetFromRecord,
	// which this path does not validate), and the overdue discard runs clear(), which
	// does the same while deleting. Refusing first is what keeps the CONNECT path from
	// ever deriving a name from such a record or migrating it in place.
	//
	// It also precedes the GENERATION check below, which is what makes the diagnostic
	// reach the record shape that actually exists: the namespace deny (3610eb3)
	// predates generation-scoped naming, so a broker old enough to have written a
	// reserved membership wrote no `gen` either, and the generation refusal would
	// otherwise answer every real case with the less specific message. Both refusals
	// are read-only and both prescribe recreating the streams, so the order decides
	// only which one an operator is told about — and naming the offending key is the
	// more actionable of the two. Nothing here reads ps.Gen, and the generation is
	// still validated before anything derives a name from it.
	// See mqttReservedMembershipInRecord.
	if key, found := mqttReservedMembershipInRecord(ps); found {
		return nil, false, fmt.Errorf("restoring session %q: the record contains a "+
			"subscription in the reserved %q namespace (%q), written by a pre-release "+
			"development broker; this format is not supported: stop this account's "+
			"brokers and recreate the bridge-owned JetStream state (the $MQTT_* "+
			"streams)", clientID, mqttPrefix, key)
	}

	// A CLAIMED teardown must be FINISHED, never resumed and never overwritten: some
	// broker is (or was) part way through deleting this incarnation's resources, so
	// the record's consumers may already be gone. Finish the deletions, release the
	// record, and start fresh — with a fresh generation, so nothing this session
	// creates can collide with what the previous owner may still be deleting — and
	// Session Present false. See sess_teardown.go.
	// The generation is validated BEFORE anything derives a name from it — the
	// adopted teardown below deletes by generation-scoped name and purges by
	// generation-scoped FILTER, so a malformed stored value could widen that purge
	// across other incarnations. Fails closed. See mqttSessGenFromRecord.
	recGen, err := mqttSessGenFromRecord(ps.Gen)
	if err != nil {
		return nil, false, fmt.Errorf("restoring session %q: %w", clientID, err)
	}
	ps.Gen = recGen

	if ps.Teardown != nil {
		if err := as.finishAdoptedTeardown(ps, hash, smsg.Sequence, opts); err != nil {
			return nil, false, fmt.Errorf("finishing the claimed teardown of session %q: %w", clientID, err)
		}
		sess := mqttSessionCreate(jsa, clientID, hash, mqttNewSessGen(), 0, _EMPTY_, _EMPTY_, opts)
		sess.asm = as
		sess.domainTk = as.domainTk
		return sess, false, nil
	}

	// An overdue session must not be resumed, even if this reconnect beats the
	// startup sweep to it: discard its state and start fresh. Spec5 [3.1.2.11.2].
	if ps.DisconnectedAt > 0 && ps.ExpiryInterval > 0 && ps.ExpiryInterval != mqttSessionNeverExpire &&
		time.Now().Unix() >= ps.DisconnectedAt+int64(ps.ExpiryInterval) {
		sess := mqttSessionCreate(jsa, clientID, hash, ps.Gen, smsg.Sequence, ps.WID, ps.PWID, opts)
		sess.asm = as
		sess.domainTk = as.domainTk
		sess.cons = ps.Cons
		// Already durable: they came out of the record. See voidedRegsRev.
		sess.cancelledRegNonces = mqttNonceSet(ps.CancelledRegs)
		sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
		sess.restoreDrainsFromRecord(ps)
		sess.sharedSubs = ps.SharedSubs
		sess.pubRelConsumer = ps.PubRel
		sess.q2AffConsumer = ps.Q2Aff
		sess.q2AffGen = ps.Q2AffGen
		// Wait for the deletes (like the clean-start discard): a queued no-wait
		// delete of the deterministic PUBREL durable could otherwise race the new
		// session recreating it.
		// As in expireSessionFromRecord, the late-uncertainty refusal is UNREACHABLE on
		// this object and gets no recovery: it was just built from the record, this
		// function never returns it resident (only the non-overdue branch below calls
		// addSession) and nothing writes through it before this call, so no save can have
		// armed uncertainty. The refusal is retained as a fail-closed invariant — the
		// CONNECT is refused rather than the session destroyed unclassified.
		if err := sess.clear(false); err != nil {
			return nil, false, fmt.Errorf("clearing expired session: %w", err)
		}
		// clear() reset the state (seq included), so sess is now a fresh session.
		return sess, false, nil
	}

	// Restore this session (even if we don't own it), the caller will do the right
	// thing. The incarnation generation comes from the record and is PRESERVED: a
	// resumed session must still find its own PUBRELs and QoS 2 holds, which are
	// named after it. Only a discard re-mints (clear()).
	sess := mqttSessionCreate(jsa, clientID, hash, ps.Gen, smsg.Sequence, ps.WID, ps.PWID, opts)
	sess.asm = as
	sess.domainTk = as.domainTk
	sess.clean = ps.Clean
	sess.subs = ps.Subs
	sess.noLocalSubs = mqttFilterSetFromList(ps.NoLocal)
	sess.rapSubs = mqttFilterSetFromList(ps.RetainAsPublished)
	sess.subIDs = ps.SubIDs
	sess.sharedSubs = ps.SharedSubs
	sess.cons = ps.Cons
	// The restored lists came OUT of the record, so they are already durable there; only
	// a later change reopens the gap.
	sess.cancelledRegNonces = mqttNonceSet(ps.CancelledRegs)
	sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
	sess.restoreDrainsFromRecord(ps)
	sess.pubRelConsumer = ps.PubRel
	// A persisted affine consumer proves only that one once existed: it may have
	// been deleted, may have drifted, or may never have been honored by an older
	// broker. q2AffReady therefore stays FALSE here and is set only by
	// ensureQ2AffineConsumerSubscription, after the delivery subscription is
	// installed and the real consumer is create-or-update verified.
	sess.q2AffConsumer = ps.Q2Aff
	sess.q2AffGen = ps.Q2AffGen
	sess.expiryInterval = ps.ExpiryInterval
	sess.disconnectedAt = ps.DisconnectedAt
	as.addSession(sess, true)
	return sess, true, nil
}

func (as *mqttAccountSessionManager) getCachedRetainedMsg(subject string) *mqttRetainedMsg {
	v, ok := as.rmsCache.Load(subject)
	if !ok {
		return nil
	}
	rm := v.(*mqttRetainedMsg)
	if rm.expiresFromCache.Before(time.Now()) {
		as.rmsCache.Delete(subject)
		return nil
	}
	// MQTT 5.0 Message Expiry Interval: drop an expired retained message rather
	// than serve a stale copy from the cache. Spec5 [3.3.2.3.3].
	if !rm.expires.IsZero() && !time.Now().Before(rm.expires) {
		as.rmsCache.Delete(subject)
		return nil
	}
	return rm
}

// If cache is enabled, the expiration for the `rm` is bumped by
// `mqttRetainedCacheTTL` seconds.
// If `onlyReplace` is true, then the `rm` object is stored in the cache using
// the `subject` key only if there was already an object stored under that key.
// If `copyMsgBytes` is true, then the `rm.Msg` bytes are copied (because it
// references some buffer that is not owned by the caller).
//
// Note: currently `onlyReplace` and `cloneMsgBytes` always have the same
// value (all `true` or all `false`) however we use different booleans to
// better express the intent.
func (as *mqttAccountSessionManager) setCachedRetainedMsg(subject string, rm *mqttRetainedMsg, onlyReplace, copyMsgBytes bool) {
	if rm == nil {
		return
	}
	rm.expiresFromCache = time.Now().Add(mqttRetainedCacheTTL)
	if onlyReplace {
		if _, ok := as.rmsCache.Load(subject); !ok {
			return
		}
	}
	if copyMsgBytes {
		rm.Msg = copyBytes(rm.Msg)
	}
	as.rmsCache.Store(subject, rm)
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT session related functions
//
//////////////////////////////////////////////////////////////////////////////

// mqttTopicAliasMax returns the effective MQTT 5.0 Topic Alias Maximum from
// Options: 0 (unset) => mqttDefaultTopicAliasMax, negative (explicitly
// disabled) => 0, otherwise the configured value. Spec5 [3.2.2.3.2].
func mqttTopicAliasMax(opts *Options) uint16 {
	tam := opts.MQTT.TopicAliasMaximum
	switch {
	case tam == 0:
		return mqttDefaultTopicAliasMax
	case tam < 0:
		return 0
	default:
		return uint16(tam)
	}
}

// mqttReceiveMax returns the effective MQTT 5.0 Receive Maximum from Options: 0
// (unset) => mqttDefaultReceiveMax, negative (explicitly disabled) => 0,
// otherwise the configured value. Spec5 [3.2.2.3.3].
func mqttReceiveMax(opts *Options) uint16 {
	rm := opts.MQTT.ReceiveMaximum
	switch {
	case rm == 0:
		return mqttDefaultReceiveMax
	case rm < 0:
		return 0
	default:
		return uint16(rm)
	}
}

// Returns a new mqttSession object with max ack pending set based on
// option or use mqttDefaultMaxAckPending if no option set.
//
// gen is the session incarnation generation every hash-derived name is scoped by:
// a freshly minted one for a session that did not exist, and the record's own for a
// restore. It is never empty — a record carrying no generation is rejected by
// mqttSessGenFromRecord rather than resumed on hash-only names. See
// sess_incarnation.go.
//
// wid / pwid are the restored record's lineage, adopted ALONGSIDE seq for the same
// reason seq is: the first save after this restore parents itself to the record it
// was derived from, and a restore that dropped the pair would stamp an empty parent
// — making every sibling of the restored base unprovable, so detection would be inert
// after any restart. Both empty for a session that did not exist, and for a record
// written before this field. See sess_fork_lineage.go.
func mqttSessionCreate(jsa *mqttJSA, id, idHash, gen string, seq uint64, wid, pwid string, opts *Options) *mqttSession {
	maxp := opts.MQTT.MaxAckPending
	if maxp == 0 {
		maxp = mqttDefaultMaxAckPending
	}

	sess := &mqttSession{
		jsa:      jsa,
		id:       id,
		idHash:   idHash,
		seq:      seq,
		wid:      wid,
		pwid:     pwid,
		maxp:     maxp,
		regClaim: mqttNewInflightNonce(),
	}
	sess.setIncarnationLocked(gen)
	return sess
}

// setIncarnationLocked adopts gen as this session's incarnation and derives every
// name scoped by it. Called at creation, on restore, and by clear() when it
// re-mints, so the derived names can never drift from the generation the record
// carries.
//
// sess.mu must be held, or the session must not yet be reachable.
func (sess *mqttSession) setIncarnationLocked(gen string) {
	sess.gen = gen
	subject, deliver, _ := mqttPubRelNames(sess.idHash, gen)
	sess.pubRelSubject = subject
	sess.pubRelDeliverySubject = deliver
	sess.pubRelDeliverySubjectB = []byte(deliver)
}

// buildPersistedSessionLocked builds the on-stream persisted form of the session.
// sess.mu must be held. Shared by save() and the CONNECT-accept watermark-failure
// rollback, so a rejected session is re-persisted (as disconnected) with its full
// resumable state rather than a partial record.
//
// Persist only consumer configs that still correspond to a live subscription:
//   - Shared (DeliverGroup) consumers are group-owned and reconstructed from
//     SharedSubs on restore; never persist them in Cons (an older server would
//     delete the group durable in clear()).
//   - A plain consumer is persisted only while its subscription exists in Subs,
//     so a consumer removed from Subs but not yet torn down (e.g. UNSUBSCRIBE
//     persisting before teardown, or a rolled-back failed SUBSCRIBE) is not
//     written with no matching Subs entry. The level-up sid "<X> fwc" maps back
//     to the main membership key "<X>.>". Spec5 [4.8.2].
func (sess *mqttSession) buildPersistedSessionLocked() mqttPersistedSession {
	var cons map[string]*ConsumerConfig
	if len(sess.cons) > 0 {
		cons = make(map[string]*ConsumerConfig, len(sess.cons))
		for sid, cc := range sess.cons {
			if cc.DeliverGroup != _EMPTY_ {
				continue
			}
			key := sid
			if strings.HasSuffix(key, mqttMultiLevelSidSuffix) {
				key = strings.TrimSuffix(key, mqttMultiLevelSidSuffix) + ".>"
			}
			if _, ok := sess.subs[key]; !ok {
				continue
			}
			cons[sid] = cc
		}
		if len(cons) == 0 {
			cons = nil
		}
	}
	// Every drain is persisted, staged ones included: the UNSUBSCRIBE's save is
	// exactly what COMMITS a staged drain, and it must carry the membership removal
	// and the drain that replaces it in the same record — either the filter is gone
	// and the drain exists, or neither is persisted (I4). A failed save rolls both
	// back in memory, so nothing intermediate is ever visible.
	//
	// A drain that claims NOTHING is deliberately omitted, which is what makes
	// finishDrainIfEmpty safe: the save that clears the last claim is the same save
	// that removes the entry, so the durable is unreferenced BEFORE it is deleted
	// (I3). Persisting it instead would leave the record naming a consumer that
	// finishDrainIfEmpty then deletes with no further write, so a crash in between
	// would strand a reference to a durable that no longer exists.
	var drains map[string]*mqttPersistedDrain
	if len(sess.drains) > 0 {
		for sid, d := range sess.drains {
			if d.claimedCount() == 0 {
				continue
			}
			if drains == nil {
				drains = make(map[string]*mqttPersistedDrain, len(sess.drains))
			}
			drains[sid] = d.persisted()
		}
	}
	return mqttPersistedSession{
		Origin:            sess.jsa.id,
		ID:                sess.id,
		Clean:             sess.clean,
		Subs:              sess.subs,
		NoLocal:           mqttFilterSetToList(sess.noLocalSubs),
		RetainAsPublished: mqttFilterSetToList(sess.rapSubs),
		SubIDs:            sess.subIDs,
		SharedSubs:        sess.sharedSubs,
		Cons:              cons,
		PubRel:            sess.pubRelConsumer,
		Q2Aff:             sess.q2AffConsumer,
		Q2AffGen:          sess.q2AffGen,
		Gen:               sess.gen,
		CancelledRegs:     mqttSortedNonces(sess.cancelledRegNonces),
		Drains:            drains,
		Will:              sess.will,
		ExpiryInterval:    sess.expiryInterval,
		DisconnectedAt:    sess.disconnectedAt,
	}
}

// Persists a session, under an OPEN record transaction that tok must identify.
//
// The token is required, not optional: a transaction that starts after the
// mutation it is meant to protect cannot make that mutation invisible to another
// writer, so every writer opens its transaction before its first mutation and
// threads the token down. A stale or copied token is refused even within the same
// generation — see sess_recordtx.go.
//
// Lock not held on entry.
func (sess *mqttSession) save(tok recordTx) error {
	_, err := sess.saveWithDetail(tok)
	return err
}

// mqttSessSaveDetail is the raw identity of one attempted record write: the CAS
// base sequence, the exact bytes (body for the digest a readback compares,
// framed for a byte-identical retry), and the store-boundary error with its
// TYPE intact. It exists because save() re-formats the store error with %v,
// destroying the CAS kind — and 10071 (taken over) versus 10164 (clustered
// contention) demand opposite dispositions, so a caller that must turn the
// rejection into a DECISION captures it here, at the boundary, rather than
// parsing it back out of a string. See sess_resume_reconcile.go.
type mqttSessSaveDetail struct {
	baseSeq  uint64
	body     []byte // the record bytes a readback returns (digest identity)
	framed   []byte // CAS header + body, exactly as sent (for an identical retry)
	hdr      int
	domainTk string
	cidHash  string
	rawErr   error // the store-boundary error, type preserved; nil on success
	// wid / pwid are the LINEAGE these bytes carry: this write's own minted identity
	// and the record it was derived from. Carried on the detail because everything
	// that later proves or classifies this attempt holds only the detail — the three
	// markWriteUncertainLocked calls and resolveContendedRecordSave's landed arms —
	// so without it there is nothing for them to adopt, and the next save would parent
	// itself to whatever memory holds instead of to the record that actually landed.
	// See sess_fork_lineage.go.
	wid, pwid string
	// voidedRegsRev is the void-register revision these bytes MARSHALLED, so a
	// caller that proves them durable outside saveWithDetail's own success arm —
	// resolveContendedRecordSave classifies a 10164-rejected write landed by an
	// exact-byte retry or a digest match — credits exactly what they carried. See
	// mqttPendingWrite.
	voidedRegsRev uint64
}

// mqttFrameRecordWrite frames a record body for the store: the CAS header requiring
// the subject's last sequence to be seq, followed by the body. It returns the framed
// bytes and the header length — the two arguments storeSessionMsg takes, and the two
// an ambiguous write keeps so it can be re-attempted verbatim.
//
// A first-ever write (seq 0) carries no header and is therefore UNCONDITIONAL: there
// is no sequence to expect. That is what keeps such a write out of the resolver's
// re-attempt arm — an unconditional create can never be classified at-base, since a
// found record always has a sequence of at least 1. See sess_uncertain.go.
func mqttFrameRecordWrite(seq uint64, body []byte) ([]byte, int) {
	if seq == 0 {
		return body, 0
	}
	bb := bytes.Buffer{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":")
	bb.WriteString(strconv.FormatInt(int64(seq), 10))
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	bb.Write(body)
	return bb.Bytes(), hdr
}

// saveWithDetail is save()'s body; it additionally returns the attempted
// write's raw identity so a caller can classify a definite CAS rejection by
// kind and retry (or read back) the exact bytes it sent. The returned error is
// unchanged from save()'s.
//
// Lock not held on entry.
func (sess *mqttSession) saveWithDetail(tok recordTx) (mqttSessSaveDetail, error) {
	var detail mqttSessSaveDetail
	if h := mqttTestSessSaveHookV.Load(); h != nil {
		if err := (*h)(sess); err != nil {
			return detail, err
		}
	}
	sess.mu.Lock()
	if err := sess.validateRecordTxLocked(tok); err != nil {
		sess.mu.Unlock()
		return detail, fmt.Errorf("refusing to persist session %q: %w", sess.id, err)
	}
	// An unresolved earlier write means sess.seq may be stale, so this CAS would be
	// meaningless: it could fail against a record that is in fact ours, or — worse,
	// once resolution adopts a sequence — overwrite one that is not. Refuse until a
	// readback has classified it. See sess_uncertain.go.
	if sess.seqUncertain {
		sess.mu.Unlock()
		return detail, fmt.Errorf("refusing to persist session %q: an earlier record write is unresolved", sess.id)
	}
	ps := sess.buildPersistedSessionLocked()
	// Stamp the lineage HERE, at the store boundary, never in the builder: the builder
	// is also clear()'s pre-teardown snapshot, so minting there would write a parent
	// pointer naming a record that was never stored. This write's parent is the record
	// we were last acknowledged for, which sess.mu makes stable across the two
	// statements. See sess_fork_lineage.go.
	ps.WID, ps.PWID = mqttNewRecordWID(), sess.wid
	// body is what a readback returns (the CAS header below is a message header,
	// not part of the stored data), so it is what an ambiguous write is identified
	// by.
	body, _ := json.Marshal(&ps)
	b := body

	domainTk, cidHash := sess.domainTk, sess.idHash
	seq := sess.seq
	wid, pwid := ps.WID, ps.PWID
	// The void-register revision this write MARSHALLS, sampled in the same critical
	// section as the marshal, which is what makes it exact: the record above carries
	// every nonce recorded at or before it and none recorded after, so whoever later
	// proves these bytes durable can credit precisely this much. EVERY landed save
	// credits it — the accounting is a property of the durability boundary, not of the
	// path that happened to be persisting. See mqttPendingWrite.
	voidedRev := sess.voidedRegsRev
	sess.mu.Unlock()

	b, hdr := mqttFrameRecordWrite(seq, b)
	detail = mqttSessSaveDetail{
		baseSeq: seq, body: body, framed: b, hdr: hdr,
		domainTk: domainTk, cidHash: cidHash, voidedRegsRev: voidedRev,
		wid: wid, pwid: pwid,
	}

	resp, err := sess.jsa.storeSessionMsg(domainTk, cidHash, hdr, b)
	if h := mqttTestSessStoreResultHookV.Load(); h != nil {
		resp, err = (*h)(sess, resp, err)
	}
	if err != nil {
		detail.rawErr = err
		// A rejected CAS is CERTAIN: nothing was stored, so sess.seq is still right
		// and the session stays usable. Anything else (a timeout, a lost response)
		// leaves an outcome nobody can name, and a stale sess.seq would otherwise
		// reject the client's own next reconnect. Record the attempted write's
		// identity so a readback can classify it.
		sess.mu.Lock()
		if mqttIsRecordCASRejection(err) {
			// Certain about the WRITE, and silent about the other fact the rejection
			// carries: the subject moved. Armed HERE, at the one boundary every record
			// write passes through, so a rejection site nobody enumerated is covered
			// anyway — the enumeration in this feature's plan was itself incomplete
			// twice over. See sess_rejected_write.go.
			sess.markWriteRejectedLocked(seq)
		} else {
			// The framed bytes travel with the identity, because settling this
			// ambiguity means RE-SENDING them: a re-marshal of whatever memory holds
			// later could not carry the digest a readback compares against. See
			// mqttPendingWrite.
			sess.markWriteUncertainLocked(seq, body, b, hdr, voidedRev, wid, pwid)
		}
		sess.mu.Unlock()
		return detail, fmt.Errorf("unable to persist session %q (seq=%v): %v", ps.ID, seq, err)
	}
	sess.mu.Lock()
	sess.seq = resp.Sequence
	// The lineage is adopted with the sequence, in the same critical section, because a
	// pair that lagged the sequence would parent the NEXT write to a record that is no
	// longer the base — and every sibling of the real base would then be unprovable.
	sess.wid, sess.pwid = wid, pwid
	// A CONFIRMED write is the durability boundary for everything it marshalled, so the
	// void-register verdicts it carried are credited HERE rather than by whichever caller
	// happened to want them recorded. Crediting only at the paths that set out to persist
	// a verdict left the other half open: a save that CARRIES a nonce without crediting it
	// leaves a settled reservation — a packet identifier and a Receive Maximum slot —
	// parked in memory although the fact is durable, and on an unbound session nothing
	// need ever look again. A reconnect's accept save is the case that proves the rule:
	// it inherits the verdicts and is not a verdict path at all.
	sess.creditVoidedRegsLocked(voidedRev)
	sess.mu.Unlock()
	return detail, nil
}

// errMQTTClearRefusedUnresolved is the sentinel clear returns when it refused BEFORE its
// first deletion because the record state is unresolved. What it encodes is
// RECOVERABILITY, not timing: nothing was deleted, claimed or reset, so a caller holding an
// obligation can put it back untouched and a caller holding a client can simply reject.
//
// It deliberately says nothing about WHEN the write became unresolved, because both are
// real. The in-fence refusal is genuinely late — an off-readLoop writer went ambiguous
// while this call waited for the fence. The entry refusal is not necessarily late at all:
// expireSession has no early uncertainty check, so it can arrive with the write already
// unresolved before its timer ever fired.
//
// It is a distinct error precisely so that recovery is never inferred from an arbitrary
// clear() failure — a lost teardown claim or a JetStream error means the teardown is
// another owner's or partly done, and re-parking there would resurrect an obligation
// nobody owes.
var errMQTTClearRefusedUnresolved = errors.New("clear refused before deletion: the record state is unresolved")

// mqttTestClearWindowHookV fires at each of clear's two decision points, named by the
// window: BEFORE its entry check, and after that check but before it takes the record
// fence. Both are windows in which another writer's ambiguity is still invisible to the
// check that follows, and a test arms one positively there instead of sleeping.
var mqttTestClearWindowHookV atomic.Pointer[func(sess *mqttSession, window string)]

const (
	mqttClearWindowPreEntry = "pre-entry"
	mqttClearWindowPreFence = "pre-fence"
)

func setMQTTTestClearWindowHook(h func(sess *mqttSession, window string)) func() {
	prev := mqttTestClearWindowHookV.Load()
	if h == nil {
		mqttTestClearWindowHookV.Store(nil)
	} else {
		mqttTestClearWindowHookV.Store(&h)
	}
	return func() { mqttTestClearWindowHookV.Store(prev) }
}

func (sess *mqttSession) clearWindowHook(window string) {
	if h := mqttTestClearWindowHookV.Load(); h != nil {
		(*h)(sess, window)
	}
}

// Clear the session: discard everything one session incarnation owns and remove
// its record.
//
// The destructive half runs under a DURABLE TEARDOWN CLAIM, not under the local
// locks: lockSession and the record transaction fence are in-process, so they
// arbitrate nothing against a broker that has taken the session over. A lost claim
// means a newer owner exists and NOTHING is deleted. A release that removes no
// record is classified by re-reading rather than assumed to be ours. And every
// resource named here is generation-scoped, so even a delete that lands after a
// peer finished the whole teardown can only name something that is already gone.
// See sess_teardown.go.
//
// It runs in a record transaction of the OLD generation — its record removal must
// validate against the epoch it opened with — and ends that generation as it
// releases the fence, so every token minted before it is dead. That covers the two
// paths that hand this very object back to be reused as a fresh session: they
// write normally at the new epoch while any pre-clear() writer is refused. See
// sess_recordtx.go.
//
// BOTH uncertainty refusals are UNCONDITIONAL, and both carry
// errMQTTClearRefusedUnresolved. The in-fence one re-checks with the fence HELD, which is
// the sound check — the entry check below is a check-then-act, because the fence is
// what serializes writers and an off-readLoop one (a register store settling) can go
// ambiguous while this call is still waiting for it. It was once opt-in, for the
// deferred discharge alone, because no other caller could act on the sentinel; each
// reachable caller now acts on the sentinel, though the shared recovery may decline, so
// the flag was a mechanism that said "true" at every
// site it could reach. The two callers built from a record just read
// (expireSessionFromRecord, createOrRestoreSession's overdue discard) cannot arm
// uncertainty at all — see the proofs beside them — and refuse rather than destroy if
// that proof were ever wrong.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) clear(noWait bool) error {
	// REFUSE AT ENTRY while an earlier write is unresolved, BEFORE any consumer or
	// record deletion. sess.seq may be stale, and this path deletes the shared
	// durables and the record before a stale CAS could fail — so it must not run on
	// a snapshot nobody has classified. The caller defers the discard instead; see
	// sess_uncertain.go.
	sess.clearWindowHook(mqttClearWindowPreEntry)
	if sess.isSeqUncertain() {
		// The SENTINEL belongs here too, not only on the in-fence refusal, because what it
		// encodes is that nothing has been deleted — which is equally true here. Two states
		// reach this check: a caller whose own uncertainty check ran before this call cannot
		// cover the interval since (the two are not under one fence), so an ambiguity armed
		// in between arrives HERE; and expireSession has no early check at all, so it
		// arrives with the write already unresolved. Returning a plain error would have
		// either caller treat it as a partly-done teardown and drop an obligation it could
		// have put back.
		return fmt.Errorf("refusing to discard session %q: %w", sess.id, errMQTTClearRefusedUnresolved)
	}
	sess.clearWindowHook(mqttClearWindowPreFence)

	tok := sess.beginRecordTx()
	// Nothing has been mutated yet, so the fence is released WITHOUT ending the
	// generation: this session is not being discarded after all, and the next resolution
	// decides again. endRecordTxNewGeneration would bump recordEpoch and kill every token
	// minted before it, for a discard that is not happening.
	if sess.isSeqUncertain() {
		if terr := sess.endRecordTx(tok); terr != nil {
			sess.jsa.c.Errorf("MQTT: %v", terr)
		}
		return fmt.Errorf("refusing to discard session %q: %w", sess.id, errMQTTClearRefusedUnresolved)
	}
	// Released on EVERY exit path, including the error returns below, after which
	// the in-memory state is already reset and old writers must be dead anyway.
	defer func() {
		if err := sess.endRecordTxNewGeneration(tok); err != nil {
			sess.jsa.c.Errorf("MQTT: %v", err)
		}
	}()

	var durs []string
	var sharedDurs []string
	var pubRelDur string

	// Close the session-affine QoS 2 fence FIRST and wait for every in-flight move
	// to finish. A move captures readiness under the lock and then performs
	// JetStream round trips, so without this a delayed claim CAS could land after
	// the scan below and leave a `claimed` record naming a generation that no
	// longer exists — which contenders would honor forever, stalling a live
	// message on a dead session. See qos2_shared_affine.go.
	q2AffGen, q2AffDur := sess.closeQ2AffAndDrain()

	// Same fence for the INBOUND QoS 2 release, and for the same reason: a PUBREL
	// admitted before this point performs JetStream round trips between its claim and
	// its fan-out, so without this it could deliver a message out of this session
	// after the discard completed. Its durable claim arbitrates that against a peer
	// broker; here it can simply be prevented. See qos2_release_claim.go.
	//
	// Reopened on EVERY exit path, because two callers reuse this object as a fresh
	// session; the re-mint below is what makes that safe.
	sess.closeQoS2ReleaseAndDrain()
	defer sess.reopenQoS2ReleaseFence()

	sess.mu.Lock()
	id := sess.id
	idHash := sess.idHash
	// The RETIRING incarnation. Everything this teardown deletes must be named
	// after it, and the session is re-minted below so a reused object provisions
	// under a generation nothing here can name. See sess_incarnation.go.
	retiringGen := sess.gen
	seq := sess.seq
	// The identity of the record the teardown marker will be derived from, captured
	// BEFORE the reset below for the same reason retiringGen and seq are. It is
	// threaded to claimTeardown explicitly rather than read off the snapshot: the
	// snapshot comes from buildPersistedSessionLocked, which deliberately mints no
	// lineage, so deriving the parent there would stamp an empty one. See
	// sess_fork_lineage.go.
	retiringWID := sess.wid
	// The record as it stands, captured BEFORE the reset below: the teardown marker
	// preserves it so any broker can finish this teardown, and so the orphan sweep
	// keeps reading the durables it names as referenced.
	ps := sess.buildPersistedSessionLocked()
	if l := len(sess.cons); l > 0 {
		durs = make([]string, 0, l)
	}
	for sid, cc := range sess.cons {
		delete(sess.cons, sid)
		// A shared group consumer is not owned by this session alone; it is
		// released (deleted only if no other member remains) after our record is
		// gone, below. Spec5 [4.8.2].
		if cc.DeliverGroup != _EMPTY_ {
			continue
		}
		durs = append(durs, cc.Durable)
	}
	// A draining consumer is this session's too: it survived an UNSUBSCRIBE only
	// because started deliveries were still owed, and the session ending discharges
	// that obligation. See unsub_drain.go.
	durs = append(durs, sess.drainDurablesLocked()...)
	// Derive shared group durables from the persisted authority (sharedSubs),
	// which also covers transient sessions rebuilt for expiry that carry no
	// consumer configs in memory. Spec5 [4.8.2].
	for key := range sess.sharedSubs {
		sharedDurs = append(sharedDurs, mqttSharedDurablesForKey(key)...)
	}
	if sess.pubRelConsumer != nil {
		pubRelDur = sess.pubRelConsumer.Durable
	}

	sess.subs = nil
	sess.noLocalSubs = nil
	sess.rapSubs = nil
	sess.subIDs = nil
	sess.sharedSubs = nil
	sess.sharedCharged = nil
	sess.consCharged = nil
	// With drains nil no new finalizer can even start, and the generation bump on
	// fence release stops any that is already in flight from writing (I9).
	sess.drains = nil
	sess.drainByDur = nil
	sess.pendingPublish = nil
	sess.pendingPubRel = nil
	sess.cpending = nil
	sess.deferred = nil
	// Retired sources belonged to the discarded incarnation's consumers, which are being
	// deleted: nothing can redeliver them, so nothing needs settling.
	sess.srcAckOwed = nil
	// The whole generation's registers are purged by the teardown, so nothing is owed.
	// The handoff watermark goes with them, and for the same reason the two paths that
	// REUSE this object as a fresh session need it: an old generation's value left
	// standing would suppress the new generation's handoffs until its revisions caught
	// up. See voidVerdictHandoffDueLocked.
	sess.cancelledRegNonces = nil
	sess.settledVoidReservations = nil
	sess.voidedRegsRev, sess.voidedRegsSavedRev, sess.voidedRegsHandoffRev = 0, 0, 0
	// A retired generation's rejected CAS is not a fresh incarnation's work to settle.
	// Cross-generation false equality is structurally excluded anyway — $MQTT_sess
	// sequences only grow, so a new incarnation's sess.seq can never equal a prior
	// generation's base — but the reset states the intent instead of leaning on stream
	// arithmetic. See sess_rejected_write.go.
	sess.recordRejectedSeq = 0
	// The identifier space of the RETIRING incarnation is gone with the maps above, so
	// the restriction that protected it must go too. Two paths REUSE the session object
	// as a fresh session, and a fresh one reports Session Present false, so it never
	// runs the resume barrier that would otherwise clear this — left set, bumpPI would
	// return 0 for the whole life of the new session and it could never deliver a
	// single outbound QoS 1/2 message. Safe because the new incarnation has a new
	// generation: nothing the old one could not account for can name a subject the new
	// one uses. See inflight_register.go (R3).
	sess.piSpaceIncomplete = false
	sess.pubRelConsumer = nil
	sess.pubRelVerified = false
	// The PUBREL delivery subject is RE-MINTED below with the new incarnation, so a
	// flag that means "already subscribed" would refer to a subscription on the
	// RETIRED subject. Left set, a reused session object skips processSub and then
	// creates a consumer delivering to a subject nothing is listening on — and
	// because $MQTT_out is interest-retained, every PUBREL it stores is dropped and
	// the client can never complete a QoS 2 exchange.
	//
	// The CONNECT reuse path happens to clear this again after calling clear(), so
	// the hazard is not reachable through it today; that is a caller compensating
	// for state clear() left inconsistent, in one of its six call sites. The
	// invariant belongs here, beside the re-mint that breaks it — exactly as the
	// affine path already does with q2AffSubscribed below.
	sess.pubRelSubscribed = false
	sess.q2AffConsumer = nil
	sess.q2AffGen = _EMPTY_
	sess.q2AffSubscribed = false
	sess.q2AffDeliverySubject = _EMPTY_
	sess.q2AffDeliverySubjectB = nil
	// Reopen the move fence. clear() is not only a teardown: the expired-session
	// path reuses this very object as a fresh session, and a permanently closed
	// fence would make every later shared QoS 2 subscribe on it fail. Safe here
	// because the fence only has to hold across the drain above — from this point
	// on there is no generation and no readiness, so beginQ2AffOp admits nothing
	// regardless.
	sess.q2AffClosing = false
	// Re-mint the incarnation generation. clear() is not only a teardown: two paths
	// REUSE this very object as a fresh session (the clean-start discard and the
	// overdue-record path), and both must provision under a generation that the
	// deletions below cannot name — including when one of them lands late, after a
	// peer has finished the teardown and a new session exists. That is what makes a
	// late delete harmless by construction rather than merely unlikely. A legacy
	// session with no generation is re-minted here too, which is the only way it
	// leaves the exposed hash-only naming. See sess_incarnation.go.
	sess.setIncarnationLocked(mqttNewSessGen())
	sess.seq = 0
	// The lineage goes with the sequence: the retiring record is being deleted, so a
	// parent pointer at it would make a reused object's first write claim descent from
	// a record that no longer exists. Zeroed AFTER retiringWID was captured above.
	sess.wid, sess.pwid = _EMPTY_, _EMPTY_
	sess.tmaxack = 0
	// Discarded session: reset the PI counter too so a reused session object
	// does not inherit the previous session's identifiers. Spec [MQTT-3.1.2-6].
	sess.last_pi = 0
	sess.mu.Unlock()

	target := &mqttTeardownTarget{
		id: id, idHash: idHash, gen: retiringGen,
		durs: durs, sharedDurs: sharedDurs, pubRelDur: pubRelDur,
		q2AffGen: q2AffGen, q2AffDur: q2AffDur,
	}

	// No persisted record means there is nothing to arbitrate and nothing to
	// release: the only resources in scope are ones this object created, under a
	// generation no other broker can name. Delete them and stop.
	if seq == 0 {
		if err := sess.deleteTeardownTargets(target, noWait); err != nil {
			return err
		}
		return sess.releaseSharedTeardownDurables(target, noWait)
	}

	// CLAIM. Losing it means a newer owner exists, so nothing is deleted — not the
	// record, not one consumer. That is a COMPLETE outcome, not a failure: the
	// session is already detached and reset in memory, which is all a superseded
	// broker is entitled to do. An indeterminate claim is different and does fail,
	// so the caller (a clean-start CONNECT, say) fails closed rather than proceeding
	// as though a teardown had run. See sess_teardown.go.
	markerSeq, nonce, claim, err := sess.claimTeardown(tok, &ps, seq, retiringWID)
	switch claim {
	case mqttTeardownClaimWon:
	case mqttTeardownClaimSuperseded:
		return nil
	default:
		return err
	}

	// DELETE this incarnation's resources. A nonce re-read before the batch would
	// only be an early-out, never what makes this safe — the read and the JetStream
	// delete cannot be atomic. Generation-scoped naming is what makes it safe, and
	// the hook here is how a test parks a broker in exactly that window.
	if h := mqttTestTeardownClaimedHookV.Load(); h != nil {
		(*h)(sess, nonce, markerSeq)
	}
	if err := sess.deleteTeardownTargets(target, noWait); err != nil {
		return err
	}

	// RELEASE. Always confirmed, even on the no-wait path — see releaseTeardown.
	// That also subsumes the older reason for waiting whenever shared subscriptions
	// were involved: the last-holder scan below must not observe this session's own
	// stale record and wrongly keep (or, via another member, delete) a group
	// durable. Spec5 [4.8.2].
	switch res, rerr := sess.releaseTeardown(tok, target, nonce, markerSeq); res {
	case mqttTeardownReleased:
		// Ours, and gone. The shared durables are now safe to consider.
	case mqttTeardownSuperseded:
		// A newer owner holds the record. Its membership counts as a holder, so the
		// shared durables are not ours to release either.
		return nil
	default:
		return rerr
	}

	// Release shared group consumers only after this session's record is gone, so
	// the last-holder scan does not count this session; delete each only when no
	// other member remains. sharedSubs was niled above, so the scan's in-memory
	// check is empty and only other sessions' records matter. Spec5 [4.8.2].
	return sess.releaseSharedTeardownDurables(target, noWait)
}

// This will update the session record for this client in the account's MQTT
// sessions stream if the session had any change in the subscriptions.
//
// tok identifies the caller's open record transaction: the membership mutations
// below and the save that persists them are one transaction, so no other writer
// can observe (or persist) a half-applied membership change. Callers that are
// themselves part of a larger packet-scoped transaction pass its token down.
//
// Runs from the client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) update(tok recordTx, filters []*mqttFilter, add bool) error {
	// Evaluate if we need to persist anything.
	var needUpdate bool
	// The membership maps are read under sess.mu by the delivery callbacks, so the
	// mutations below must hold it. Callers are serialized against each other by
	// the session's locked-map entry, but that says nothing to a concurrent READER
	// — which is a genuine unsynchronized access, not merely a test artifact. The
	// lock is released before save(), which takes it itself.
	sess.mu.Lock()
	for _, f := range filters {
		// Shared subscriptions live in a separate map keyed by session key; plain
		// subscriptions in subs keyed by their NATS subject. rapSubs/subIDs are
		// shared but keyed by session key (collision-free). Spec5 [4.8.2].
		key := f.sessionKey()
		if add {
			if f.qos >= mqttSubAckFailure {
				continue
			}
			if f.shared {
				if qos, ok := sess.sharedSubs[key]; !ok || qos != f.qos {
					if sess.sharedSubs == nil {
						sess.sharedSubs = make(map[string]byte)
					}
					sess.sharedSubs[key] = f.qos
					needUpdate = true
				}
			} else if qos, ok := sess.subs[key]; !ok || qos != f.qos {
				if sess.subs == nil {
					sess.subs = make(map[string]byte)
				}
				sess.subs[key] = f.qos
				needUpdate = true
			}
			// Track No Local and Retain As Published in separate sets so the
			// persisted QoS byte stays in the 0..2 range older servers understand.
			// No Local never applies to shared subscriptions. Spec5 [3.8.3.1].
			var changed bool
			if !f.shared {
				if sess.noLocalSubs, changed = mqttUpdateFilterOptSet(sess.noLocalSubs, key, f.noLocal); changed {
					needUpdate = true
				}
			}
			if sess.rapSubs, changed = mqttUpdateFilterOptSet(sess.rapSubs, key, f.retainAsPublished); changed {
				needUpdate = true
			}
			// f.subID == 0 clears any prior identifier: a re-SUBSCRIBE without one
			// removes it. Spec5 [3.8.2.1.2].
			if sess.subIDs, changed = mqttUpdateFilterSubID(sess.subIDs, key, f.subID); changed {
				needUpdate = true
			}
		} else {
			if f.shared {
				if _, ok := sess.sharedSubs[key]; ok {
					delete(sess.sharedSubs, key)
					needUpdate = true
				}
			} else if _, ok := sess.subs[key]; ok {
				delete(sess.subs, key)
				needUpdate = true
			}
			var changed bool
			if sess.noLocalSubs, changed = mqttUpdateFilterOptSet(sess.noLocalSubs, key, false); changed {
				needUpdate = true
			}
			if sess.rapSubs, changed = mqttUpdateFilterOptSet(sess.rapSubs, key, false); changed {
				needUpdate = true
			}
			if sess.subIDs, changed = mqttUpdateFilterSubID(sess.subIDs, key, 0); changed {
				needUpdate = true
			}
		}
	}
	sess.mu.Unlock()

	var err error
	if needUpdate {
		err = sess.save(tok)
	}
	return err
}

func (sess *mqttSession) bumpPI() uint16 {
	if sess.piSpaceIncomplete {
		// The durable identifier space could not be fully reconstructed on resume, so
		// some identifiers the client may be holding are unknown to us. Allocating one
		// could collide with a register or an unreplayed PUBREL that still owns it
		// ([MQTT-2.3.1-4]). Withhold instead: the caller treats 0 exactly as it treats
		// the in-flight cap, and AckWait re-offers the delivery. See
		// inflight_register.go (R3).
		return 0
	}
	var avail bool
	next := sess.last_pi
	for i := 0; i < 0xFFFF; i++ {
		next++
		if next == 0 {
			next = 1
		}

		_, usedInPublish := sess.pendingPublish[next]
		_, usedInPubRel := sess.pendingPubRel[next]
		if !usedInPublish && !usedInPubRel {
			sess.last_pi = next
			avail = true
			break
		}
	}
	if !avail {
		return 0
	}
	return sess.last_pi
}

// effectiveMaxAck returns the maximum number of unacknowledged QoS1/2 PUBLISH
// this session may have in flight: the configured MaxAckPending, further capped
// by the client's MQTT 5.0 Receive Maximum when one was advertised (rmax != 0).
//
// Lock held on entry.
func (sess *mqttSession) effectiveMaxAck() int {
	m := sess.maxp
	if sess.rmax != 0 && sess.rmax < m {
		m = sess.rmax
	}
	return int(m)
}

// qos12InflightForCap counts in-flight QoS 1/2 PUBLISHes against effectiveMaxAck.
// With a Receive Maximum, QoS 2 messages awaiting PUBCOMP (pendingPubRel) still
// count: Spec5 [4.9] frees the quota only on PUBACK/PUBCOMP or a failed PUBREC.
// Without one, the cap is JetStream MaxAckPending, which those were acked out of.
//
// Lock held on entry
func (sess *mqttSession) qos12InflightForCap() int {
	n := len(sess.pendingPublish)
	// Nothing is exempt. It is tempting to discount a delivery whose consumer is gone,
	// since it can never complete — but Receive Maximum bounds what the CLIENT is holding
	// unacknowledged, and a consumer disappearing does not prove the client completed
	// anything. Discounting it would let the broker send another PUBLISH while the client
	// still holds the first, breaking the negotiated limit (Spec5 [4.9]). A session stuck
	// this way is a lost message being reported honestly, not an accounting bug.
	//
	// A delivery that was provably NEVER TRANSMITTED is a different matter, and it does not
	// reach here at all: its register carries a cancellation tombstone, so recovery frees
	// the identifier outright rather than reserving it. See inflight_register.go.
	if sess.rmax != 0 {
		n += len(sess.pendingPubRel)
	}
	return n
}

// deferredNaksLocked removes and returns up to (effectiveMaxAck −
// qos12InflightForCap) deferred ack subjects in ascending stream-sequence
// order, so NAK-forced redeliveries preserve publication order
// [MQTT-4.6.0-1]. The caller must send AckNak on each subject AFTER releasing
// the session lock. A NAK'd message that arrives while the session is again at
// the cap simply re-enters deferred via trackPublish.
//
// Lock held on entry
func (sess *mqttSession) deferredNaksLocked() []string {
	if len(sess.deferred) == 0 {
		return nil
	}
	capacity := sess.effectiveMaxAck() - sess.qos12InflightForCap()
	if capacity <= 0 {
		return nil
	}
	type deferredNak struct {
		key        mqttDeferredKey
		ackSubject string
	}
	all := make([]deferredNak, 0, len(sess.deferred))
	for k, subj := range sess.deferred {
		all = append(all, deferredNak{k, subj})
	}
	slices.SortFunc(all, func(a, b deferredNak) int {
		switch {
		case a.key.sseq < b.key.sseq:
			return -1
		case a.key.sseq > b.key.sseq:
			return 1
		default:
			return 0
		}
	})
	if len(all) > capacity {
		all = all[:capacity]
	}
	subjects := make([]string, 0, len(all))
	for _, n := range all {
		delete(sess.deferred, n.key)
		subjects = append(subjects, n.ackSubject)
	}
	return subjects
}

// dropUnredeliverableRetained releases pending retained (QoS1/2) deliveries: they
// have no JetStream backing and so cannot be redelivered on resume. Keeping them
// would leak a Receive Maximum slot for the life of a persistent session.
//
// A retained send is identified by having NO ack subject, NO durable and NO in-flight
// register — trackPublishRetained records a wholly empty entry. Each exclusion is
// load-bearing, because a resumed entry can lack the first two:
//
//   - an empty ack subject alone is not the test: a delivery recovered from an
//     in-flight register has none until its first redelivery arrives;
//   - an empty durable alone is not either: an entry reserved from an UNREADABLE
//     register has no source to name, and it is precisely the identifier that must
//     not be handed out again.
//
// Dropping either would undo the resume barrier on the next reconnect. See
// inflight_register.go.
//
// Lock held on entry
func (sess *mqttSession) dropUnredeliverableRetained() {
	for pi, ack := range sess.pendingPublish {
		if ack.jsAckSubject == _EMPTY_ && ack.jsDur == _EMPTY_ && ack.regSeq == 0 {
			delete(sess.pendingPublish, pi)
		}
	}
}

// trackPublishRetained is invoked when a retained (QoS) message is published. It
// allocates a new PI and adds it to pendingPublish with an empty value (no
// JetStream backing). Spec5 [3.3.4].
//
// Lock held on entry
func (sess *mqttSession) trackPublishRetained() uint16 {
	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}

	// Honor the effective in-flight cap (MaxAckPending capped by the client's
	// Receive Maximum). At the limit, return 0 so the caller downgrades this
	// retained message to QoS0: it is still delivered, but does not count as an
	// unacknowledged QoS1/2 PUBLISH. Spec5 [3.3.4].
	if sess.qos12InflightForCap() >= sess.effectiveMaxAck() {
		return 0
	}

	pi := sess.bumpPI()
	if pi == 0 {
		return 0
	}
	sess.pendingPublish[pi] = &mqttPending{}

	return pi
}

// trackPublish is invoked when a (QoS) PUBLISH message is to be delivered. It
// detects an untracked (new) message based on its sequence extracted from its
// delivery-time jsAckSubject, and adds it to the tracking maps. Returns a PI to
// use for the message (new, or previously used), and whether this is a
// duplicate delivery attempt.
//
// Lock held on entry
// The returned started flag is true when the PI was already tracked, i.e. this
// is a redelivery of a message whose onward delivery already began.
//
// isNew is true only where this call ALLOCATED the identifier, which is exactly the
// set of deliveries owing a durable in-flight register (R1/R6). Such an entry is
// returned marked regPending and MUST NOT be transmitted until
// storeInflightRegister has confirmed it — the caller either confirms it or removes
// it. See inflight_register.go.
func (sess *mqttSession) trackPublish(jsDur, jsAckSubject string) (packetID uint16, dup, started, isNew bool) {
	var pi uint16

	if jsAckSubject == _EMPTY_ || jsDur == _EMPTY_ {
		return 0, false, false, false
	}

	// Make sure we initialize the tracking maps.
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}

	// Get the stream sequence and duplicate flag from the ack reply subject.
	sseq, _, dcount, _, _ := ackReplyInfo(jsAckSubject)
	if dcount > 1 {
		dup = true
	}

	var ack *mqttPending
	sseqToPi, ok := sess.cpending[jsDur]
	if !ok {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsDur] = sseqToPi
	} else {
		pi = sseqToPi[sseq]
	}

	if pi != 0 {
		// There is a possible race between a PUBLISH re-delivery calling us,
		// and a PUBREC received already having submitting a PUBREL into JS . If
		// so, indicate no need for (re-)delivery by returning a PI of 0.
		_, usedForPubRel := sess.pendingPubRel[pi]
		if /*dup && */ usedForPubRel {
			return 0, false, false, false
		}

		// We should have a pending JS ACK for this PI. This is a redelivery of a
		// message already counted against the in-flight cap, so it is not gated
		// again here. NOTE: if a persistent session accumulated more unacked
		// messages than a newly-lowered Receive Maximum, redeliveries of those
		// pre-existing pending messages are not additionally paced on the new
		// connection (a known limitation; new deliveries below still are, and
		// the session drains to the cap as PUBACKs arrive).
		ack = sess.pendingPublish[pi]
		// A register store is in flight for this identifier, so its FIRST
		// transmission has not happened yet (R1). Decline: acting here would either
		// put the payload on the wire before its identity is durable, or — if the
		// store then fails definitively and untracks the identifier — send under an
		// identifier no record names. AckWait re-offers the sequence, by which time
		// the owner has resolved it either way.
		if ack != nil && ack.regPending {
			return 0, false, false, false
		}
		started = true
	} else {
		// Gate new deliveries at the effective cap: MaxAckPending capped by the
		// client's MQTT 5.0 Receive Maximum. QoS 2 messages awaiting PUBCOMP are
		// counted too when a Receive Maximum applies (Spec5 [4.9]).
		if sess.qos12InflightForCap() >= sess.effectiveMaxAck() {
			// At the in-flight cap: withhold the message and remember its ack
			// subject so a freed slot can NAK it for prompt redelivery
			// (deferredNaksLocked); AckWait remains the fallback. Overwrite on
			// re-deferral: JS ignores acks for superseded delivery attempts, so
			// only the latest subject is NAK-able. Spec5 [4.9].
			if sess.deferred == nil {
				sess.deferred = make(map[mqttDeferredKey]string)
			}
			sess.deferred[mqttDeferredKey{jsDur, sseq}] = jsAckSubject
			return 0, false, false, false
		}

		if _, wasDeferred := sess.deferred[mqttDeferredKey{jsDur, sseq}]; wasDeferred {
			delete(sess.deferred, mqttDeferredKey{jsDur, sseq})
			// A withheld message was never transmitted, so this is its first
			// send even though the JS delivery count is > 1 [MQTT-3.3.1-1].
			dup = false
		}

		pi = sess.bumpPI()
		if pi == 0 {
			return 0, false, false, false
		}

		sseqToPi[sseq] = pi
		isNew = true
	}

	if ack == nil {
		entry := &mqttPending{
			jsDur:        jsDur,
			sseq:         sseq,
			jsAckSubject: jsAckSubject,
			// This allocation owes a register, and nothing may send under the
			// identifier until it is confirmed (R1).
			regPending: isNew,
		}
		if isNew {
			// The register's identity is minted WITH the allocation, not with the store,
			// so a cancellation that happens while the store is in flight has something
			// immutable to name it by. Neither the identifier nor the source works there:
			// the cancellation frees the one and the other is re-offered.
			entry.regNonce = mqttNewInflightNonce()
		}
		sess.pendingPublish[pi] = entry
	} else {
		ack.jsAckSubject = jsAckSubject
		ack.sseq = sseq
		ack.jsDur = jsDur
	}

	return pi, dup, started, isNew
}

// Stops a PI from being tracked as a PUBLISH. It can still be in use for a
// pending PUBREL.
//
// Also returns the affine assignment claim this delivery came from, when it came
// from one: every terminal path for a session-affine shared QoS 2 delivery
// (PUBREC, expiry, Maximum Packet Size discard) has to record the affine side as
// complete on that claim, and the claim identity is only reachable through the
// pending entry being removed here. Empty for every ordinary delivery. See
// qos2_shared_affine.go.
//
// Lock held on entry
func (sess *mqttSession) untrackPublish(pi uint16) (jsAckSubject, q2AffClaim string) {
	ack := sess.untrackPublishEntry(pi)
	if ack == nil {
		return _EMPTY_, _EMPTY_
	}
	return ack.jsAckSubject, ack.q2AffClaim
}

// untrackPublishEntry is untrackPublish returning the whole removed entry, so a
// caller that may have to PUT IT BACK can. The PUBREC path needs that: it
// untracks before storing the PUBREL, and a failed store must leave the session
// exactly as it was rather than with the identifier wedged and its delivery
// tracking gone. It is also how a caller learns the delivery's (durable,
// sequence), which nothing else exposes once the entry is removed.
//
// Returns nil when the identifier was not tracked as a PUBLISH.
//
// Lock held on entry
func (sess *mqttSession) untrackPublishEntry(pi uint16) *mqttPending {
	ack, ok := sess.pendingPublish[pi]
	if !ok {
		return nil
	}

	delete(sess.pendingPublish, pi)
	// Do NOT reset last_pi here: it is a monotonic rolling counter (see bumpPI) so
	// a just-freed id is not reused within a delivery burst, which a client would
	// read as a duplicate. Spec [MQTT-2.3.1-4].

	if len(sess.cpending) != 0 && ack.jsDur != _EMPTY_ {
		if sseqToPi := sess.cpending[ack.jsDur]; sseqToPi != nil {
			delete(sseqToPi, ack.sseq)
		}
	}

	return ack
}

// mqttRetiredSource is one QoS 2 source that is past PUBREC while its acknowledgement is
// unproven: its payload must never be sent again, and its next delivery attempt is to be
// acknowledged rather than delivered.
//
// It carries what is needed to make the fact DURABLE later as well as to act on it now.
// durable says whether the register already records the retirement; while it is false the
// exchange's PUBREL record is the only cross-restart evidence, so that PUBREL must not be
// acknowledged away (see mqttProcessPubComp).
type mqttRetiredSource struct {
	pi         uint16 // the identifier whose exchange retired this source
	regSeq     uint64 // the register's current sequence: CAS base while unmarked, delete target once settled
	nonce      string // the register's identity, so a later CAS can prove the record is ours
	q2AffClaim string // preserved across the rewrite; empty for an ordinary delivery
	durable    bool
}

// retireSourceLocked records that a QoS 2 source delivery is past PUBREC while its
// acknowledgement is NOT proven: its payload must never be sent again, and the next
// delivery attempt is to be acknowledged rather than delivered.
//
// Keyed by the SOURCE, deliberately, and not by the packet identifier. The obvious
// alternative — restoring the source's cpending link so the usedForPubRel guard in
// trackPublish suppresses the redelivery — works only until PUBCOMP: untrackPubRel
// removes the pendingPubRel entry the guard tests, and the very next redelivery then
// re-sends the payload of an exchange the client has already COMPLETED. A fact about
// the source has to outlive the identifier's bookkeeping, which is the same shape the
// affine path already uses (its claim's AffineDone flag) and for the same reason.
//
// The set is drained by mqttSourceRetired, which is why it cannot grow with traffic: an
// entry costs one map slot until the first delivery attempt whose acknowledgement is
// CONFIRMED, and it is only ever created when an acknowledgement could not be proven.
//
// Idempotent: a same-broker reconnect re-runs the resume barrier while an existing
// retirement is still in place, and re-recording one must not undo what is already known
// about it (in particular that it is already durable).
//
// Lock held on entry.
func (sess *mqttSession) retireSourceLocked(jsDur string, sseq uint64, rs *mqttRetiredSource) {
	if jsDur == _EMPTY_ || sseq == 0 || rs == nil {
		return
	}
	if sess.srcAckOwed == nil {
		sess.srcAckOwed = make(map[mqttDeferredKey]*mqttRetiredSource)
	}
	key := mqttDeferredKey{jsDur, sseq}
	if cur := sess.srcAckOwed[key]; cur != nil {
		if rs.durable {
			cur.durable = true
			cur.regSeq = rs.regSeq
		}
		return
	}
	sess.srcAckOwed[key] = rs
}

// retiredSourceForLocked finds the retirement an exchange's packet identifier created, so
// PUBCOMP can secure it before destroying the PUBREL that also evidences it. The map holds
// one entry per unproven acknowledgement, so the scan is over a handful of entries at most.
//
// Lock held on entry.
func (sess *mqttSession) retiredSourceForLocked(pi uint16) (mqttDeferredKey, bool) {
	for key, rs := range sess.srcAckOwed {
		if rs != nil && rs.pi == pi && !rs.durable {
			return key, true
		}
	}
	return mqttDeferredKey{}, false
}

// sourceRetiredLocked reports whether a delivery attempt belongs to a source that is
// already past PUBREC with an unproven acknowledgement. Such an attempt must not be
// tracked and must not be delivered; the caller settles it with settleRetiredSource
// once it has released the lock.
//
// Lock held on entry.
func (sess *mqttSession) sourceRetiredLocked(jsDur string, sseq uint64) bool {
	if len(sess.srcAckOwed) == 0 {
		return false
	}
	_, owed := sess.srcAckOwed[mqttDeferredKey{jsDur, sseq}]
	return owed
}

// settleRetiredSource acknowledges a retired source's delivery attempt with
// CONFIRMATION, using THIS attempt's reply subject — never the stale one whose
// acknowledgement could not be proven — and releases the entry only once that lands. A
// failure leaves the entry in place so the next redelivery retries; nothing is
// delivered either way, which is the whole point.
//
// Lock NOT held on entry: this issues a JetStream request.
func (sess *mqttSession) settleRetiredSource(c *client, jsDur string, sseq uint64, reply string) {
	if err := sess.jsa.confirmAck(reply); err != nil {
		c.Warnf("MQTT: discarded a redelivery of retired QoS 2 source %s/%d for client %q, but its acknowledgement is still unproven: %v",
			jsDur, sseq, sess.id, err)
		return
	}
	key := mqttDeferredKey{jsDur, sseq}
	sess.mu.Lock()
	var regSeq uint64
	if rs := sess.srcAckOwed[key]; rs != nil {
		regSeq = rs.regSeq
	}
	delete(sess.srcAckOwed, key)
	sess.mu.Unlock()
	// The source is provably gone, so the retired register has nothing left to protect
	// and is removed at its exact sequence — otherwise every retirement would leave a
	// record behind for the orphan sweep to collect much later.
	if regSeq != 0 && sess.asm != nil {
		sess.asm.deleteInflightRegister(regSeq)
	}
	c.Debugf("MQTT: settled retired QoS 2 source %s/%d for client %q", jsDur, sseq, sess.id)
}

// retrackPublishLocked reinstates an entry removed by untrackPublishEntry, under
// the SAME packet identifier, restoring both maps. Used to roll back a PUBREC
// whose PUBREL store failed: without it the identifier stays wedged in
// pendingPubRel with no ack subject anything will ever fill, and the delivery's
// tracking is gone, so it can neither be redelivered nor acknowledged.
//
// Lock held on entry.
func (sess *mqttSession) retrackPublishLocked(pi uint16, ack *mqttPending) {
	if ack == nil {
		return
	}
	if sess.pendingPublish == nil {
		sess.pendingPublish = make(map[uint16]*mqttPending)
	}
	sess.pendingPublish[pi] = ack
	if ack.jsDur == _EMPTY_ {
		return
	}
	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	sseqToPi := sess.cpending[ack.jsDur]
	if sseqToPi == nil {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[ack.jsDur] = sseqToPi
	}
	sseqToPi[ack.sseq] = pi
}

// resendPendingRedelivery re-sends the session's unacknowledged QoS 1/2 PUBLISH
// and PUBREL packets by NAK'ing each on its JS ack subject, forcing immediate
// redelivery (with the DUP flag set) on session resume as required by spec
// [MQTT-4.4.0-1], instead of waiting for the JS consumer's AckWait interval.
// NAKs are ordered by stream sequence so redeliveries preserve original order
// [MQTT-4.6.0-1], and re-use the original packet identifiers.
//
// The NAK — the PROMPTNESS of the redelivery — is a same-server-reconnect
// optimization: it needs a live per-attempt JetStream ack subject, and an entry
// rebuilt by the resume barrier from its durable in-flight register has none until
// its first redelivery arrives, so after a restart or a failover redelivery falls
// back to the AckWait interval. The packet IDENTIFIER no longer depends on any of
// this: it is restored from the register itself, before any source interest exists.
// See inflight_register.go.
//
// resumePubs/resumeRels are the pending identities captured when the session was
// attached to this connection, BEFORE any subscription was restored. Only those
// are eligible: restoring subscriptions re-establishes interest, so by the time
// this runs the consumer may already have pushed brand new deliveries into the
// pending maps, and NAK'ing one of those redelivers a message that is on its way
// to the client for the first time — a duplicate the client never asked for.
//
// If the disconnect lasted longer than AckWait, the consumer's own pending
// check may already redeliver a message as interest is re-established; the NAK
// then produces one extra copy. That is harmless (same packet identifier, DUP
// set, so the client dedups) and does not happen on a timely reconnect.
func (sess *mqttSession) resendPendingRedelivery(c *client, resumePubs, resumeRels map[mqttPendingRef]struct{}) {
	type pendingNak struct {
		pi         uint16
		sseq       uint64
		ackSubject string
		// jsDur is which consumer the sequence belongs to. Required, not
		// decorative: the reap reconciliation below checks sequences against
		// $MQTT_msgs, and a session-affine shared QoS 2 entry's sequence is a
		// $MQTT_out sequence in a completely different sequence space. Checking one
		// against the other would misread a live copy as reaped, release its packet
		// identifier and skip its redelivery. See qos2_shared_affine.go.
		jsDur string
	}
	bySeq := func(a, b pendingNak) int {
		switch {
		case a.sseq < b.sseq:
			return -1
		case a.sseq > b.sseq:
			return 1
		default:
			return 0
		}
	}
	// Keep only what was pending when the session was resumed. processSubs has
	// already run, so the maps below can also hold deliveries this connection
	// started moments ago; those are being transmitted for the first time and a
	// NAK would duplicate them. The resume snapshot is the discriminator — the
	// maps themselves cannot tell the two apart. PUBLISH and PUBREL live on
	// different streams (distinct sequence spaces) and are paced differently, so
	// collect them separately.
	sess.mu.Lock()
	jsa := sess.jsa
	maxAck := sess.effectiveMaxAck()
	hasReceiveMax := sess.rmax != 0
	pubNaks := make([]pendingNak, 0, len(sess.pendingPublish))
	for pi, ack := range sess.pendingPublish {
		if _, atResume := resumePubs[mqttPendingRef{pi, ack.sseq, ack.jsDur}]; !atResume {
			continue
		}
		if ack.regPending {
			// Allocated but not yet durable, so never transmitted (R1). Forcing a
			// redelivery would achieve nothing — trackPublish declines a regPending
			// entry — and the identifier may still be released if the store fails.
			continue
		}
		if ack.jsAckSubject != _EMPTY_ {
			pubNaks = append(pubNaks, pendingNak{pi, ack.sseq, ack.jsAckSubject, ack.jsDur})
		}
	}
	// Everything in flight that is NOT being NAK'd still occupies a slot at the
	// client (a fresh delivery, or a pre-resume entry with no ack subject to NAK),
	// so it has to come off the redelivery budget computed below.
	notNakedPubs := len(sess.pendingPublish) - len(pubNaks)
	relNaks := make([]pendingNak, 0, len(sess.pendingPubRel))
	for pi, ack := range sess.pendingPubRel {
		if _, atResume := resumeRels[mqttPendingRef{pi, ack.sseq, ack.jsDur}]; !atResume {
			continue
		}
		if ack.jsAckSubject != _EMPTY_ {
			relNaks = append(relNaks, pendingNak{pi, ack.sseq, ack.jsAckSubject, _EMPTY_})
		}
	}
	// The affine QoS 2 delivery path has to be re-established on resume for the
	// same reason the PUBREL one does: its consumer may hold a started delivery
	// this session owns, and nothing else recreates the delivery subscription when
	// the resumed session restores no shared QoS 2 subscription.
	q2AffGen := sess.q2AffGen
	q2AffDur := _EMPTY_
	if sess.q2AffConsumer != nil {
		q2AffDur = sess.q2AffConsumer.Durable
	}
	// Any pending PUBREL (including a placeholder created at PUBREC before its
	// delivery recorded an ack subject) needs the PUBREL delivery subscription.
	hasPendingPubRel := len(sess.pendingPubRel) > 0
	inflightPubRel := len(sess.pendingPubRel)
	// Deferred (withheld, never transmitted) deliveries from before the
	// disconnect should not wait an AckWait either; drain them here too.
	//
	// The entry is NAK'd but KEPT: it is the only record that this message has
	// never been on the wire, and the NAK's own redelivery arrives with a
	// JetStream delivery count above one, so dropping it here would mark that
	// delivery DUP on the client's FIRST sight of it — the very property this
	// mechanism is documented to preserve ([MQTT-3.3.1-1]; see the prompt-release
	// note in docs/CONFORMANCE.md). trackPublish removes the entry when the
	// message actually goes out, which is the point at which it stops being
	// never-transmitted; a re-send after that is a genuine DUP.
	deferredNaks := make([]pendingNak, 0, len(sess.deferred))
	for k, subj := range sess.deferred {
		deferredNaks = append(deferredNaks, pendingNak{0, k.sseq, subj, k.jsDur})
	}
	sess.mu.Unlock()

	// Reconcile against the messages stream: a QoS1/2 message whose Message Expiry
	// TTL reaped it while it was delivered-but-unacked is gone from the stream, so
	// it can neither be redelivered nor PUBACK'd. Such an entry would otherwise
	// leak a Receive Maximum slot for the life of the session. Drop any snapshot
	// entry whose message no longer exists — below the stream's first sequence, or
	// (per-message expiry can leave holes above it) confirmed absent by an exact
	// lookup — releasing its packet identifier and skipping its NAK. The lookups
	// run outside the session lock. Spec5 [3.3.2.3.3].
	if si, err := jsa.lookupStream(mqttStreamName); err == nil && si != nil {
		firstSeq := si.State.FirstSeq
		kept := make([]pendingNak, 0, len(pubNaks))
		var gone []uint16
		for _, n := range pubNaks {
			if n.jsDur == q2AffDur && q2AffDur != _EMPTY_ {
				// A session-affine QoS 2 copy: its sequence is a $MQTT_out sequence,
				// so the $MQTT_msgs checks below say nothing about it. NAK it directly
				// (JetStream ignores an ack for a delivery it does not recognize).
				kept = append(kept, n)
				continue
			}
			reaped := n.sseq < firstSeq
			if !reaped {
				if _, err := jsa.loadMsg(mqttStreamName, n.sseq); err != nil && !isErrorOtherThan(err, JSNoMessageFoundErr) {
					reaped = true
				}
			}
			if reaped {
				gone = append(gone, n.pi)
			} else {
				kept = append(kept, n)
			}
		}
		if len(gone) > 0 {
			sess.mu.Lock()
			for _, pi := range gone {
				sess.untrackPublish(pi)
			}
			sess.mu.Unlock()
		}
		pubNaks = kept
	}

	// Replay pending PUBRELs: they complete already-started QoS2 handshakes (the
	// client answers PUBCOMP, freeing its Receive Maximum slot) rather than
	// starting new deliveries. The PUBREL delivery subscription is only (re)created
	// by the QoS2 subscribe path, so ensure it exists here whenever any PUBREL is
	// pending (a resume may restore no QoS2 subscription, and a placeholder created
	// at PUBREC may not yet carry an ack subject). Spec5 [MQTT-4.4.0-1].
	if q2AffGen != _EMPTY_ {
		// Standalone caller, so it opens its own record transaction and passes the
		// token down. See sess_recordtx.go.
		tok := sess.beginRecordTx()
		err := sess.ensureQ2AffineConsumerSubscription(c, tok)
		if terr := sess.endRecordTx(tok); terr != nil {
			c.Errorf("MQTT: %v", terr)
		}
		if err != nil {
			c.Errorf("Unable to re-establish session-affine QoS 2 delivery on resume: %v", err)
		} else if sess.asm != nil {
			// Reconcile the assignments this generation still owns: complete the ones
			// whose copy is already gone, and reap the ones whose source message no
			// longer exists. Both would otherwise sit forever, and a stalled claim
			// makes every contender wait on a winner with nothing left to deliver.
			sess.asm.recoverQ2AffClaims(sess.idHash, q2AffGen)
		}
	}

	if hasPendingPubRel {
		if err := sess.ensurePubRelConsumerSubscription(c); err != nil {
			c.Errorf("Unable to re-establish PUBREL delivery on resume: %v", err)
		}
		slices.SortFunc(relNaks, bySeq)
		for _, n := range relNaks {
			jsa.sendMsg(n.ackSubject, AckNak)
		}
	}

	// PUBLISH redeliveries reuse their packet identifiers and bypass the in-flight
	// gate, so respect the client's Receive Maximum: NAK at most the effective cap,
	// in stream order; the JS consumer re-offers the rest via AckWait. Deliveries
	// already in flight and not part of this NAK set hold slots of their own, so
	// they come off the budget first. When a Receive Maximum applies, in-flight
	// PUBRELs count against it too, so subtract them; without one the cap is the JS
	// MaxAckPending, which PUBRELs have already acked out of. Spec5 [MQTT-3.3.4-9].
	capacity := maxAck - notNakedPubs
	if hasReceiveMax {
		capacity -= inflightPubRel
	}
	if capacity < 0 {
		capacity = 0
	}
	slices.SortFunc(pubNaks, bySeq)
	if len(pubNaks) > capacity {
		pubNaks = pubNaks[:capacity]
	}
	for _, n := range pubNaks {
		jsa.sendMsg(n.ackSubject, AckNak)
	}

	// Deferred (withheld, never transmitted) deliveries fill the capacity left
	// after pending redeliveries; the rest re-offer via AckWait and re-enter
	// deferred through trackPublish.
	if rem := capacity - len(pubNaks); rem > 0 {
		slices.SortFunc(deferredNaks, bySeq)
		if len(deferredNaks) > rem {
			deferredNaks = deferredNaks[:rem]
		}
		for _, n := range deferredNaks {
			jsa.sendMsg(n.ackSubject, AckNak)
		}
	}
}

// trackAsPubRel is invoked in 2 cases: (a) when we receive a PUBREC and we need
// to change from tracking the PI as a PUBLISH to a PUBREL; and (b) when we
// attempt to deliver the PUBREL to record the JS ack subject for it.
//
// Lock held on entry
func (sess *mqttSession) trackAsPubRel(pi uint16, jsAckSubject string) {
	if sess.pubRelConsumer == nil {
		// The cosumer MUST be set up already.
		return
	}
	jsDur := sess.pubRelConsumer.Durable

	if sess.pendingPubRel == nil {
		sess.pendingPubRel = make(map[uint16]*mqttPending)
	}

	if jsAckSubject == _EMPTY_ {
		sess.pendingPubRel[pi] = &mqttPending{
			jsDur: jsDur,
		}
		return
	}

	sseq, _, _, _, _ := ackReplyInfo(jsAckSubject)

	if sess.cpending == nil {
		sess.cpending = make(map[string]map[uint64]uint16)
	}
	sseqToPi := sess.cpending[jsDur]
	if sseqToPi == nil {
		sseqToPi = make(map[uint64]uint16)
		sess.cpending[jsDur] = sseqToPi
	}
	sseqToPi[sseq] = pi
	sess.pendingPubRel[pi] = &mqttPending{
		jsDur:        sess.pubRelConsumer.Durable,
		sseq:         sseq,
		jsAckSubject: jsAckSubject,
	}
}

// Stops a PI from being tracked as a PUBREL.
//
// Lock held on entry
func (sess *mqttSession) untrackPubRel(pi uint16) (jsAckSubject string) {
	ack, ok := sess.pendingPubRel[pi]
	if !ok {
		return _EMPTY_
	}

	delete(sess.pendingPubRel, pi)

	if sess.pubRelConsumer != nil && len(sess.cpending) > 0 {
		if sseqToPi := sess.cpending[ack.jsDur]; sseqToPi != nil {
			delete(sseqToPi, ack.sseq)
		}
	}

	return ack.jsAckSubject
}

// Sends a consumer delete request, but does not wait for response.
//
// The tmaxack refund is what this session was actually CHARGED, not the config's
// MaxAckPending: a consumer restored from the session record is never charged
// (processJSConsumer charges only in its !exists branch), so refunding it drove
// tmaxack negative after a resume-then-teardown cycle and inflated the account's
// remaining budget. Same rule, and same reason, as sharedCharged. Spec5 [4.8.2].
//
// Lock not held on entry.
func (sess *mqttSession) deleteConsumer(cc *ConsumerConfig) {
	sess.mu.Lock()
	sess.refundConsumerChargeLocked(cc.Durable)
	sess.jsa.deleteConsumer(mqttStreamName, cc.Durable, true)
	sess.mu.Unlock()
}

// chargeConsumerLocked records that this process added amt to tmaxack for durable,
// so exactly that much is refunded exactly once when it goes away.
//
// Lock held on entry.
func (sess *mqttSession) chargeConsumerLocked(durable string, amt int) {
	if amt == 0 {
		return
	}
	if sess.consCharged == nil {
		sess.consCharged = make(map[string]int)
	}
	sess.consCharged[durable] += amt
	sess.tmaxack += amt
}

// refundConsumerChargeLocked subtracts what was charged for durable, once. A
// durable with no entry was never charged by this process (it was restored from
// the record), so nothing is refunded.
//
// Lock held on entry.
func (sess *mqttSession) refundConsumerChargeLocked(durable string) {
	amt, ok := sess.consCharged[durable]
	if !ok {
		return
	}
	delete(sess.consCharged, durable)
	sess.tmaxack -= amt
	if sess.tmaxack < 0 {
		sess.tmaxack = 0
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// CONNECT protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

// mqttKeepAliveRD returns the read deadline for a keep alive: one and a half
// times the keep alive interval. Computed in milliseconds to avoid truncating
// the half second for odd keep alives (e.g. 1s => 1.5s). Spec [MQTT-3.1.2-24].
func mqttKeepAliveRD(ka uint16) time.Duration {
	return time.Duration(ka) * 1500 * time.Millisecond
}

// Parse the MQTT connect protocol
func (c *client) mqttParseConnect(r *mqttReader, hasMappings bool) (byte, *mqttConnectProto, error) {
	// Protocol name
	proto, err := r.readBytes("protocol name", false)
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-1]
	if !bytes.Equal(proto, mqttProtoName) {
		// Check proto name against v3.1 to report better error
		if bytes.Equal(proto, mqttOldProtoName) {
			return 0, nil, fmt.Errorf("older protocol %q not supported", proto)
		}
		return 0, nil, fmt.Errorf("expected connect packet with protocol name %q, got %q", mqttProtoName, proto)
	}

	// Protocol level
	level, err := r.readByte("protocol level")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.2-2], spec5 [3.1.2.2]. We support 3.1.1 (0x4) and 5.0 (0x5).
	switch level {
	case mqttProtoLevel, mqttProtoLevel5:
	default:
		return mqttConnAckRCUnacceptableProtocolVersion, nil, fmt.Errorf("unacceptable protocol version of %v", level)
	}
	// Enforce the maximum accepted protocol version. MQTT 5.0 support is still
	// being completed (e.g. session expiry is not yet honored), so it is opt-in:
	// unless the operator sets MaxProtocolVersion to 5, the server accepts only
	// 3.1.1. When rejecting, reply using the framing
	// the client asked for (set c.mqtt.proto) so it can read the reason code and
	// fall back.
	if c.srv != nil {
		maxv := c.srv.getOpts().MQTT.MaxProtocolVersion
		if maxv == 0 {
			// MQTT 5.0 is always enabled in mqtt-nats-bridge.
			maxv = mqttProtoLevel5
		}
		if level > maxv {
			c.mqtt.proto = level
			return mqttConnAckRCUnacceptableProtocolVersion, nil, fmt.Errorf("MQTT protocol version %v not accepted (maximum is %v); set mqtt.max_protocol_version to 5 to enable MQTT 5.0", level, maxv)
		}
	}
	// Record the negotiated version; it drives wire framing for the rest of the
	// connection's lifetime (CONNACK, ACKs, DISCONNECT).
	c.mqtt.proto = level

	cp := &mqttConnectProto{}
	// Connect flags
	cp.flags, err = r.readByte("flags")
	if err != nil {
		return 0, nil, err
	}

	// Spec [MQTT-3.1.2-3]
	if cp.flags&mqttConnFlagReserved != 0 {
		return 0, nil, errMQTTConnFlagReserved
	}

	var hasWill bool
	wqos := (cp.flags & mqttConnFlagWillQoS) >> 3
	wretain := cp.flags&mqttConnFlagWillRetain != 0
	// Spec [MQTT-3.1.2-11]
	if cp.flags&mqttConnFlagWillFlag == 0 {
		// Spec [MQTT-3.1.2-13]
		if wqos != 0 {
			return 0, nil, fmt.Errorf("if Will flag is set to 0, Will QoS must be 0 too, got %v", wqos)
		}
		// Spec [MQTT-3.1.2-15]
		if wretain {
			return 0, nil, errMQTTWillAndRetainFlag
		}
	} else {
		// Spec [MQTT-3.1.2-14]
		if wqos == 3 {
			return 0, nil, fmt.Errorf("if Will flag is set to 1, Will QoS can be 0, 1 or 2, got %v", wqos)
		}
		hasWill = true
	}

	if c.mqtt.rejectQoS2Pub && hasWill && wqos == 2 {
		return mqttConnAckRCQoS2WillRejected, nil, fmt.Errorf("server does not accept QoS2 for Will messages")
	}

	// Spec [MQTT-3.1.2-19]
	hasUser := cp.flags&mqttConnFlagUsernameFlag != 0
	// Spec [MQTT-3.1.2-21]
	hasPassword := cp.flags&mqttConnFlagPasswordFlag != 0
	// Spec [MQTT-3.1.2-22]
	if !hasUser && hasPassword {
		return 0, nil, errMQTTPasswordFlagAndNoUser
	}

	// Keep alive
	var ka uint16
	ka, err = r.readUint16("keep alive")
	if err != nil {
		return 0, nil, err
	}
	cp.ka = ka
	// Spec [MQTT-3.1.2-24]
	if ka > 0 {
		cp.rd = mqttKeepAliveRD(ka)
	}

	// MQTT 5.0: the CONNECT variable header ends with a properties block, just
	// before the payload. Spec5 [3.1.2.11].
	if level == mqttProtoLevel5 {
		cp.props, err = r.readProperties(mqttPacketConnect)
		if err != nil {
			return 0, nil, err
		}
		// Capture at parse time the two properties that gate optional outbound
		// content, so even a CONNACK that rejects this CONNECT honors them:
		// Maximum Packet Size bounds every server-generated packet (an optional
		// Reason String is dropped, and a packet that still overflows is not sent),
		// and Request Problem Information 0 forbids Reason String/User Property on
		// packets other than CONNACK/DISCONNECT. If readProperties itself failed
		// above, cp.props is nil and the client communicated no limit. Spec5
		// [3.1.2.11.4], [3.1.2.11.7].
		if cp.props != nil {
			c.mqtt.maxPacketSize = cp.props.maxPacketSize
			c.mqtt.noProblemInfo = cp.props.present[mqttPropRequestProblemInfo] && cp.props.requestProblem == 0
		}
		// Enhanced authentication (AUTH exchange) is not implemented. If the
		// client requests an authentication method, reject the connection with
		// the v5 "bad authentication method" reason. Spec5 [4.12].
		if cp.props != nil && cp.props.authMethod != _EMPTY_ {
			return mqttReasonBadAuthMethod, nil, errMQTTAuthMethodNotSupported
		}
		// Spec5 [3.1.2.11.10]: Authentication Data without an Authentication
		// Method is a Protocol Error.
		if cp.props != nil && cp.props.authData != nil && cp.props.authMethod == _EMPTY_ {
			return mqttReasonProtocolError, nil, fmt.Errorf("%w: authentication data without authentication method", errMQTTProtocolError)
		}
	}

	// Payload starts here and order is mandated by:
	// Spec [MQTT-3.1.3-1]: client ID, will topic, will message, username, password

	// Client ID
	c.mqtt.cid, err = r.readString("client ID")
	if err != nil {
		return 0, nil, err
	}
	// Spec [MQTT-3.1.3-7]
	if c.mqtt.cid == _EMPTY_ {
		if cp.flags&mqttConnFlagCleanSession == 0 {
			return mqttConnAckRCIdentifierRejected, nil, errMQTTCIDEmptyNeedsCleanFlag
		}
		// Spec [MQTT-3.1.3-6]
		c.mqtt.cid = nuid.Next()
		c.mqtt.cidGenerated = true
	}
	// Spec [MQTT-3.1.3-4] and [MQTT-3.1.3-9]
	//
	// mqtt-nats-bridge divergence from upstream NATS: we accept any valid-UTF-8
	// client ID (MQTT 5.0 [3.1.3.1] permits it, and mainstream brokers do too),
	// rather than requiring a NATS-safe name. The client ID is only used as a
	// NATS subject token via getHash() (sessions, QoS2 dedup — see
	// mqttQoS2InternalSubject), so arbitrary characters (spaces, '.', wildcards)
	// are safe. mqttValidateString still enforces UTF-8 and rejects NUL.
	if err := mqttValidateString(c.mqtt.cid, "client ID"); err != nil {
		return mqttConnAckRCIdentifierRejected, nil, err
	}

	if hasWill {
		cp.will = &mqttWill{
			qos:    wqos,
			retain: wretain,
		}
		// MQTT 5.0: Will properties precede the Will topic. Spec5 [3.1.3.2].
		if level == mqttProtoLevel5 {
			cp.will.props, err = r.readProperties(mqttPropsContextWill)
			if err != nil {
				return 0, nil, err
			}
		}
		var topic []byte
		// Need to make a copy since we need to hold to this topic after the
		// parsing of this protocol.
		topic, err = r.readBytes("Will topic", true)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, errMQTTEmptyWillTopic
		}
		if err := mqttValidateTopic(topic, "Will topic"); err != nil {
			return 0, nil, err
		}
		// Convert MQTT topic to NATS subject
		cp.will.subject, err = mqttTopicToNATSPubSubject(topic)
		if err != nil {
			return 0, nil, err
		}
		// Check for subject mapping.
		if hasMappings {
			// For selectMappedSubject to work, we need to have c.pa.subject set.
			// If there is a change, c.pa.mapped will be set after the call.
			c.pa.subject = cp.will.subject
			if changed := c.selectMappedSubject(); changed {
				// We need to keep track of the NATS subject/mapped in the `cp` structure.
				cp.will.subject = c.pa.subject
				cp.will.mapped = c.pa.mapped
				// We also now need to map the original MQTT topic to the new topic
				// based on the new subject.
				topic = natsSubjectToMQTTTopic(cp.will.subject)
			}
			// Reset those now.
			c.pa.subject, c.pa.mapped = nil, nil
		}
		// A Will may not target the broker's reserved namespace. This is the same
		// rule mqttProcessPub enforces for a client PUBLISH, applied here because
		// a Will never goes through that path: the immediate Will is delivered by
		// mqttInitiateMsgDelivery, and a delayed Will is replayed from the outbox
		// over the *service* connection, where the client's own NATS permissions
		// no longer apply. Without this check a Will topic is the one remaining
		// way for a client to publish into $MQTT.>.
		//
		// Checked after subject mapping so a mapping cannot land in the namespace
		// either. Rejecting the CONNECT (rather than dropping the Will silently)
		// tells the client its Will will not be honoured; 0x05 in 3.1.1, mapped to
		// 0x87 Not authorized for v5 by mqttConnAckReasonFromRC.
		if bytes.HasPrefix(cp.will.subject, []byte(mqttPrefix)) {
			return mqttConnAckRCNotAuthorized, nil, fmt.Errorf("Will topic %q targets the reserved %s namespace", topic, mqttPrefix)
		}
		cp.will.topic = topic
		// Now "will" message.
		// Ask for a copy since we need to hold to this after parsing of this protocol.
		cp.will.message, err = r.readBytes("Will message", true)
		if err != nil {
			return 0, nil, err
		}
		// MQTT 5.0 Payload Format Indicator on the Will: if the client declares the
		// Will payload is UTF-8 (PFI=1) but it is not well-formed, reject the CONNECT
		// with CONNACK 0x99. Spec5 [3.1.3.2.2], [3.3.2.3.2].
		if level == mqttProtoLevel5 && cp.will.props != nil &&
			cp.will.props.present[mqttPropPayloadFormat] && cp.will.props.payloadFormat == 1 &&
			!utf8.Valid(cp.will.message) {
			return 0, nil, fmt.Errorf("%w: Will payload is not valid UTF-8 but the Payload Format Indicator is 1", errMQTTPayloadFormatInvalid)
		}
	}

	if hasUser {
		c.opts.Username, err = r.readString("user name")
		if err != nil {
			return 0, nil, err
		}
		if c.opts.Username == _EMPTY_ {
			return mqttConnAckRCBadUserOrPassword, nil, errMQTTEmptyUsername
		}
		// Spec [MQTT-3.1.3-11]
		if err := mqttValidateString(c.opts.Username, "user name"); err != nil {
			return mqttConnAckRCBadUserOrPassword, nil, err
		}
	}

	if hasPassword {
		c.opts.Password, err = r.readString("password")
		if err != nil {
			return 0, nil, err
		}
		c.opts.Token = c.opts.Password
	}
	return 0, cp, nil
}

func (c *client) mqttConnectTrace(cp *mqttConnectProto) string {
	trace := fmt.Sprintf("clientID=%s", c.mqtt.cid)
	if cp.rd > 0 {
		trace += fmt.Sprintf(" keepAlive=%v", cp.rd)
	}
	if cp.will != nil {
		trace += fmt.Sprintf(" will=(topic=%s QoS=%v retain=%v)",
			cp.will.topic, cp.will.qos, cp.will.retain)
	}
	if cp.flags&mqttConnFlagCleanSession != 0 {
		trace += " clean"
	}
	if c.opts.Username != _EMPTY_ {
		trace += fmt.Sprintf(" username=%s", c.opts.Username)
	}
	if c.opts.Password != _EMPTY_ {
		trace += " password=****"
	}
	return trace
}

// Process the CONNECT packet.
//
// For the first session on the account, an account session manager will be created,
// along with the JetStream streams/consumer necessary for the working of MQTT.
//
// The session, identified by a client ID, will be registered, or if already existing,
// will be resumed. If the session exists but is associated with an existing client,
// the old client is evicted, as per the specifications.
//
// Due to specific locking requirements around JS API requests, we cannot hold some
// locks for the entire duration of processing of some protocols, therefore, we use
// a map that registers the client ID in a "locked" state. If a different client tries
// to connect and the server detects that the client ID is in that map, it will try
// a little bit until it is not, or fail the new client, since we can't protect
// processing of protocols in the original client. This is not expected to happen often.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessConnect(c *client, cp *mqttConnectProto, trace bool) error {
	sendConnAck := func(rc byte, sessp bool, reasonStr string) bool {
		// A delivered success CONNACK is recorded by mqttEnqueueConnAck itself,
		// under the same c.mu hold as the write, so a concurrent takeover
		// notification cannot read the gate in between (see
		// mqttRecordConnAckWrittenLocked).
		sent, werr := c.mqttEnqueueConnAck(rc, sessp, reasonStr)
		if sent && trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", sessp, rc)))
		}
		// The write error is part of the verdict: "a success CONNACK was written"
		// must be a checked fact, because every post-bind failure path decides its
		// abort discipline by exactly that. A failed write closed the socket, so
		// nothing was acknowledged.
		return sent && werr == nil
	}

	c.mu.Lock()
	cid := c.mqtt.cid
	c.clearAuthTimer()
	c.mu.Unlock()
	if !s.isClientAuthorized(c) {
		// The CONNACK is enqueued inside authViolation; trace it only if sent.
		sent := c.authViolation()
		if sent && trace {
			c.traceOutOp("CONNACK", []byte(fmt.Sprintf("sp=%v rc=%v", false, mqttConnAckRCNotAuthorized)))
		}
		return ErrAuthentication
	}
	// Now that we are authenticated, we have the client bound to the account.
	// Get the account's level MQTT sessions manager. If it does not exists yet,
	// this will create it along with the streams where sessions and messages
	// are stored.
	asm, err := s.getOrCreateMQTTAccountSessionManager(c)
	if err != nil {
		return err
	}

	// Resolve every input the success CONNACK's size depends on, then verify the
	// frame fits the client's Maximum Packet Size BEFORE touching any session
	// state. If it cannot fit, the server MUST NOT send it [MQTT-3.1.2-24]; there
	// is no useful connection to establish, so fail now — before any session
	// lookup, clear, rebind, live-client eviction or Will handling — so nothing
	// needs rolling back and no side effects leak. v5 only. Spec5 [3.2.2.3.6].
	if c.mqtt.proto == mqttProtoLevel5 {
		// Server Keep Alive: enforce the configured maximum when the client asked
		// for none (0) or more. Reads only Options and cp.ka. Spec5 [3.1.2.10],
		// [3.2.2.3.14].
		if kam := s.getOpts().MQTT.KeepAliveMaximum; kam > 0 && (cp.ka == 0 || cp.ka > uint16(kam)) {
			c.mqtt.serverKeepAlive = uint16(kam)
			cp.rd = mqttKeepAliveRD(uint16(kam))
		}
		// Response Information: when the client sets Request Response Information = 1
		// and the server is configured with a base response topic, return it in the
		// CONNACK so the client can build a request/response topic. Spec5
		// [3.1.2.11.6], [3.2.2.3.15].
		if ri := s.getOpts().MQTT.ResponseInformation; ri != _EMPTY_ &&
			cp.props != nil && cp.props.present[mqttPropRequestResponseInfo] && cp.props.requestResponse == 1 {
			c.mqtt.responseInfo = ri
		}
		// Topic Alias Maximum, both directions. Inbound: the server-side cap for
		// aliases the client may send us, advertised in the CONNACK. Outbound: the
		// client's CONNECT Topic Alias Maximum (absent => 0) caps aliases we may
		// send it, bounded by the same server-side cap to limit per-connection
		// memory. Spec5 [3.2.2.3.2], [3.1.2.11.2].
		c.mqtt.topicAliasMax = mqttTopicAliasMax(s.getOpts())
		if cp.props != nil {
			c.mqtt.outTopicAliasMax = min(cp.props.topicAliasMax, c.mqtt.topicAliasMax)
		}
		// Maximum Packet Size we advertise and enforce on inbound; captured once so
		// the encoded and enforced values are identical even across a reload.
		c.mqtt.maxInPacketSize = c.mqttServerMaxPacketSize()
		// Receive Maximum we advertise and enforce on inbound QoS 1/2 PUBLISH;
		// captured before the fit check below so its property bytes count toward
		// the CONNACK size. Spec5 [3.2.2.3.3], [4.9].
		c.mqtt.recvMaxIn = mqttReceiveMax(s.getOpts())
		// Frame size is independent of the session present flag, so a placeholder
		// (0) is fine for the fit check.
		if frame := c.mqttMakeConnAckV5Frame(mqttConnAckRCConnectionAccepted, 0, _EMPTY_); !c.mqttFitsClientMax(len(frame)) {
			c.Errorf("Success CONNACK of %d bytes exceeds client Maximum Packet Size %d; closing", len(frame), c.mqtt.maxPacketSize)
			return fmt.Errorf("CONNACK exceeds client maximum packet size")
		}
	}

	// Most of the session state is altered only in the readLoop so does not
	// need locking. For things that can be access in the readLoop and in
	// callbacks, we will use explicit locking.
	// To prevent other clients to connect with the same client ID, we will
	// add the client ID to a "locked" map so that the connect somewhere else
	// is put on hold.
	// This keep track of how many times this client is detecting that its
	// client ID is in the locked map. After a short amount, the server will
	// fail this inbound client.
	locked := 0

CHECK:
	asm.mu.Lock()
	// Check if different applications keep trying to connect with the same
	// client ID at the same time.
	if tm, ok := asm.flappers[cid]; ok {
		// If the last time it tried to connect was more than 1 sec ago,
		// then accept and remove from flappers map.
		if time.Since(tm) > mqttSessJailDur {
			asm.removeSessFromFlappers(cid)
		} else {
			// Will hold this client for a second and then close it. We
			// do this so that if the client has a reconnect feature we
			// don't end-up with very rapid flapping between apps.
			// We need to wait in place and not schedule the connection
			// close because if this is a misbehaved client that does
			// not wait for the CONNACK and sends other protocols, the
			// server would not have a fully setup client and may panic.
			asm.mu.Unlock()
			select {
			case <-s.quitCh:
			case <-time.After(mqttSessJailDur):
			}
			c.closeConnection(DuplicateClientID)
			return ErrConnectionClosed
		}
	}
	// If an existing session is in the process of processing some packet, we can't
	// evict the old client just yet. So try again to see if the state clears, but
	// if it does not, then we have no choice but to fail the new client instead of
	// the old one.
	if _, ok := asm.sessLocked[cid]; ok {
		asm.mu.Unlock()
		if locked++; locked == 10 {
			return fmt.Errorf("other session with client ID %q is in the process of connecting", cid)
		}
		time.Sleep(100 * time.Millisecond)
		goto CHECK
	}

	// Register this client ID the "locked" map for the duration if this function.
	asm.sessLocked[cid] = struct{}{}
	// And remove it on exit, regardless of error or not.
	defer func() {
		asm.mu.Lock()
		delete(asm.sessLocked, cid)
		// The resident session, read in the same critical section as the release, for the
		// void-verdict re-check below: this is a gate release like unlockSession's and owes
		// the same handoff, and reading the map here is what keeps it to a RESIDENT object
		// however this CONNECT ended. A successful CONNECT leaves the session bound, which
		// the predicate excludes — a bound session converges through its own packet tails,
		// and firing here (right after the success CONNACK, before any tail exists) could
		// refuse the resume work behind an already-accepted connection.
		released := asm.sessions[cid]
		asm.mu.Unlock()
		asm.handoffVoidVerdictsOnGateRelease(released)
	}()

	// Is the client requesting a clean session (v5: Clean Start) or not.
	cleanSess := cp.flags&mqttConnFlagCleanSession != 0
	// MQTT 5.0 Session Expiry Interval: how long the session survives after the
	// connection closes; orthogonal to Clean Start. 3.1.1 maps clean => 0,
	// persistent => never. A session ends at connection close iff the interval is
	// 0 — exactly the legacy meaning of `clean` — so sess.clean is derived from the
	// interval and all existing clean-based branches work unchanged. Spec5 [3.1.2.11.2].
	var expiry uint32
	if !cleanSess {
		expiry = mqttSessionNeverExpire
	}
	if c.mqtt.proto == mqttProtoLevel5 {
		expiry = 0
		if cp.props != nil {
			expiry = cp.props.sessionExpiry
		}
	}
	sessClean := expiry == 0
	// Session present? Assume false, will be set to true only when applicable.
	sessp := false
	// The QoS 1/2 deliveries this session already had in flight when it was bound
	// to this connection, filled in by the resume path below and consumed by
	// resendPendingRedelivery. Empty for a session that was not resumed.
	var resumePubs, resumeRels map[mqttPendingRef]struct{}
	// Do we have an existing session for this client ID
	es, exists := asm.sessions[cid]
	asm.mu.Unlock()
	formatError := func(err error) error {
		return fmt.Errorf("%v for account %q, session %q", err, c.acc.GetName(), cid)
	}

	// The session is not in the map, but may be on disk, so try to recover
	// or create the stream if not.
	if !exists {
		es, exists, err = asm.createOrRestoreSession(cid, s.getOpts())
		if err != nil {
			if err == errMQTTSessionCollision {
				sendConnAck(mqttConnAckRCIdentifierRejected, false, err.Error())
			}
			return formatError(err)
		}
	}
	// A resident session with an UNRESOLVED record write must be resolved BEFORE
	// Session Present is computed and before the accept save, or the reconnect
	// would be decided (and CAS'd) against a sequence nobody has classified. A
	// reconnect routinely beats the periodic retry pass here, and both go through
	// resolveUncertainSession so exactly one of them acts; the client ID is already
	// in sessLocked, which is what excludes the retry pass. See sess_uncertain.go.
	if exists && es.isSeqUncertain() {
		switch s.resolveUncertainSession(asm, es) {
		case mqttUncertainForeign, mqttUncertainForked:
			// Another owner is authoritative — or, on the forked arm, a record exists
			// that nothing established an owner for — and the local copy has been
			// evicted either way. Rebuild from the record and take over normally: the
			// surviving record is what the session's durable state IS, whoever wrote
			// it, and this CONNECT is a legitimate new claim on it. Named explicitly
			// rather than left to the default, which means "ours" and would bind a
			// stale object whose CAS base is no longer the record's.
			es, exists, err = asm.createOrRestoreSession(cid, s.getOpts())
			if err != nil {
				if err == errMQTTSessionCollision {
					sendConnAck(mqttConnAckRCIdentifierRejected, false, err.Error())
				}
				return formatError(err)
			}
		case mqttUncertainUnresolved:
			// Nothing may be concluded about this session's durable state, so it must
			// not be resumed, discarded, or overwritten. Reject and let the client
			// retry. Two states arrive here and the rejection is right for both: the
			// readback failed, in which case the deferred close obligations are still
			// parked; or the resolution's own work — a discharged deferred close, a
			// continuation's re-save — left a NEW write outstanding, in which case
			// binding anyway would have the accept save refused and the session evicted
			// with that write's obligations. See sess_uncertain.go.
			return formatError(fmt.Errorf("session record state is unresolved"))
		default:
			// Resolved as ours, and the deferred close (if any) has been applied. If
			// that discharged a deferred discard — a clean disconnect, or a deadline
			// that passed while the record was unresolved — the session is gone from
			// the maps and this CONNECT must start fresh rather than resume the object
			// it left behind. clear() already reset and re-minted it, so it is reusable
			// exactly as the clean-start path reuses one.
			asm.mu.RLock()
			_, resident := asm.sessions[cid]
			asm.mu.RUnlock()
			if !resident {
				exists = false
			}
		}
	}

	if exists {
		// An overdue session must not be resumed even when this reconnect beat
		// a late expiry timer to the in-memory copy (the record-restore path
		// has the same check). Spec5 [3.1.2.11.2].
		es.mu.Lock()
		overdue := es.disconnectedAt > 0 && es.expiryInterval > 0 &&
			es.expiryInterval != mqttSessionNeverExpire &&
			time.Now().Unix() >= es.disconnectedAt+int64(es.expiryInterval)
		es.mu.Unlock()
		// Clear the session if client wants a clean session.
		// Also, Spec [MQTT-3.2.2-1]: don't report session present
		if cleanSess || es.clean || overdue {
			// Spec [MQTT-3.1.2-6]: If CleanSession is set to 1, the Client and
			// Server MUST discard any previous Session and start a new one.
			// This Session lasts as long as the Network Connection. State data
			// associated with this Session MUST NOT be reused in any subsequent
			// Session.
			if err := es.clear(false); err != nil {
				if errors.Is(err, errMQTTClearRefusedUnresolved) {
					// A LATE ambiguity — armed by an off-readLoop writer while this
					// discard waited for the fence — deleted nothing, so this case gets
					// the SAME answer as the early one above: reject without evicting,
					// and let the client retry. Evicting here would drop the retained
					// continuations, so what the resolution of that ambiguous write
					// authorizes would never be committed at all, and the
					// gate release reads asm.sessions to decide the void-verdict handoff,
					// which an evicted object never gets. Nothing is parked: this path has
					// a client waiting for a CONNACK, not an obligation, and the retry's
					// own uncertainty check resolves the session.
					return formatError(fmt.Errorf("session record state is unresolved"))
				}
				asm.removeSession(es, true)
				return err
			}
		} else {
			// Report to the client that the session was present
			sessp = true
		}
		// Spec [MQTT-3.1.4-2]. If the ClientId represents a Client already
		// connected to the Server then the Server MUST disconnect the existing
		// client.
		// Bind with the new client. This needs to be protected because can be
		// accessed outside of the readLoop.
		es.mu.Lock()
		ec := es.c
		es.c = c
		// The requested Session Expiry Interval and Clean flag are NOT applied
		// here. This CONNECT has not been granted yet — the reconciliation phase
		// below can still refuse it — and the accept save a few lines down would
		// persist them regardless, leaving the record describing a session the
		// client was never told it has: with a requested interval of 0 the close
		// path would then discard a persistent session outright (doClean). They
		// are applied, and persisted, at the grant point below instead, so a
		// refused resume changes no lifetime and has nothing to repair.
		// Spec5 [3.1.2.11.2].
		// A session that was CLEARED just above is not being resumed: it has no
		// prior lifetime worth protecting, clear() does not reset these two
		// fields, and the accept save below is about to create its record from
		// scratch. So it takes the requested terms immediately; only a genuine
		// RESUME defers them to the grant point.
		if !sessp {
			es.clean, es.expiryInterval = sessClean, expiry
		}
		es.disconnectedAt = 0
		// Clear these flags so we resubscribe to the PUBREL and session-affine QoS 2
		// delivery subjects if needed: both subscriptions belonged to the previous
		// connection and went with it, while the consumers behind them (and their
		// pending deliveries) are session state that survives.
		es.pubRelSubscribed = false
		es.q2AffSubscribed = false
		// A resumed session must re-prove its affine delivery path before any move
		// may use it, and its PUBREL path before it stores a PUBREL: in both cases
		// the persisted consumer config is not evidence that the consumer still
		// exists, and $MQTT_out's interest retention silently drops what it stores
		// when it does not. See ensureQ2AffineConsumerSubscription and
		// ensurePubRelConsumerSubscription.
		es.q2AffReady = false
		es.pubRelVerified = false
		if sessp {
			// Resumed session: release retained (QoS1/2) deliveries with no
			// JetStream backing. They cannot be redelivered, so keeping them would
			// leak a Receive Maximum slot for the life of the session.
			es.dropUnredeliverableRetained()
		}
		// What this session already had in flight, captured before any subscription
		// is restored — the only point at which "pending" unambiguously means
		// "pending from before the disconnect". resendPendingRedelivery NAKs exactly
		// these; anything the restored consumers push from here on is a first
		// transmission that must not be redelivered on top of itself.
		resumePubs, resumeRels = mqttPendingRefs(es.pendingPublish), mqttPendingRefs(es.pendingPubRel)
		es.mu.Unlock()
		if ec != nil {
			asm.beforeTakeoverDiscard(cid)
			// Remove the "will" of the existing client before closing it — unless
			// that connection already closed and owes an immediate Will, which this
			// takeover is too late to cancel. See mqttDiscardWillOnTakeover.
			ec.mqttDiscardWillOnTakeover()
			// Add to the map of the flappers
			asm.mu.Lock()
			asm.addSessToFlappers(cid)
			asm.mu.Unlock()
			c.Warnf("Replacing old client %q since both have the same client ID %q", ec, cid)
			// Tell the evicted v5 client why before the close (no-op for
			// 3.1.1), through the tail every displacement path shares: it
			// claims the connection's one DISCONNECT and stays silent for a
			// connection displaced before its own CONNACK. Spec5 [3.1.4].
			ec.mqttNotifyTakenOver()
			// Close old client in separate go routine
			go ec.closeConnection(DuplicateClientID)
		}
	} else {
		// Spec [MQTT-3.2.2-3]: if the Server does not have stored Session state,
		// it MUST set Session Present to 0 in the CONNACK packet.
		es.mu.Lock()
		es.c, es.clean = c, sessClean
		es.expiryInterval = expiry
		es.disconnectedAt = 0
		es.mu.Unlock()
		// Now add this new session into the account sessions
		asm.addSession(es, true)
	}
	// MQTT 5.0 Receive Maximum: cap the number of in-flight unacknowledged
	// QoS1/2 PUBLISH we will send to this client. Re-evaluated on every CONNECT
	// since a resumed session may be taken over by a client with a different
	// (or no) limit. 0 means no client-imposed limit.
	var rmax uint16
	if cp.props != nil {
		rmax = cp.props.receiveMax
	}
	// The accept transaction. It opens BEFORE the mutations the accept save
	// persists (the Receive Maximum and the Will drop just below), because a
	// transaction that begins after its own mutation cannot make that mutation
	// invisible to another writer. See sess_recordtx.go.
	acceptTok := es.beginRecordTx()
	acceptTokOpen := true
	endAcceptTx := func() {
		if !acceptTokOpen {
			return
		}
		acceptTokOpen = false
		if err := es.endRecordTx(acceptTok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
	}
	defer endAcceptTx()

	es.mu.Lock()
	es.rmax = rmax
	// A new connection cancels any pending delayed Will (cancelWill below);
	// drop its persisted copy with the save that follows. Spec5 [MQTT-3.1.2-8].
	es.will = nil
	es.mu.Unlock()

	// Server Keep Alive, Topic Alias Maximum and the advertised Maximum Packet
	// Size were resolved on entry (before any session state), so the CONNACK could
	// be size-checked against the client's Maximum Packet Size before committing.

	// We would need to save only if it did not exist previously, but we save
	// always in case we are running in cluster mode. This will notify other
	// running servers that this session is being used.
	if err := es.save(acceptTok); err != nil {
		asm.removeSession(es, true)
		return err
	}
	cidHash := getHash(cid)
	// Durably advance this client's delayed-Will cancellation watermark to the
	// session sequence just saved, BEFORE the success CONNACK. This is the
	// authoritative, permanent suppression of any older-generation Will: unlike the
	// best-effort cancelWillOutbox below, and unlike the session record (which a
	// finite disconnect or clean start later removes), the watermark survives and
	// keeps an obsolete Will suppressed. The CONNACK is therefore gated on this
	// write (strict: no acknowledgement unless the suppression is durable). On
	// failure the accepted session is rolled back to a well-defined, non-falsely-
	// connected state and the CONNECT is rejected; the client retries. Note that
	// c.mqtt.cp is still unset here, so the failing connection's own Will does not
	// fire (mqttHandleWill no-ops on a nil cp). See advanceWillCancelWatermark.
	es.mu.Lock()
	candidate := es.seq
	es.mu.Unlock()
	if werr := asm.advanceWillCancelWatermark(cidHash, candidate); werr != nil {
		s.Warnf("MQTT: rejecting CONNECT for client %q: could not durably advance delayed-Will cancel watermark: %v", cid, werr)
		// The rollback is its OWN record transaction, so this frame's must be
		// released first — the fence's blocking form would otherwise deadlock
		// against a transaction only this frame can close. Idempotent, so the
		// deferred release below stays a no-op.
		endAcceptTx()
		asm.rollbackAcceptedSession(s, es, cidHash, candidate, expiry)
		return werr
	}
	// The connection is now accepted (session saved). A new connection for this
	// client ID cancels any delayed Will pending from a previous connection,
	// whether or not its session still existed (a clean-start client removes its
	// session on disconnect). Done only on success so a failed CONNECT does not
	// suppress the prior Will. This is the authoritative cross-replica
	// cancellation (CAS pending->cancelled on the outbox), regardless of which
	// broker enqueued the Will. Keyed by client ID hash. Spec5 [MQTT-3.1.2-8].
	// cancelWillOutbox retries internally and logs on exhausted retries; proceed
	// with the reconnect regardless — blocking CONNECT on a JetStream outage would
	// be worse than the rare chance the prior Will still fires under sustained
	// unavailability. Spec5 [MQTT-3.1.2-8].
	_ = asm.cancelWillOutbox(s, cidHash)
	// Likewise cancel any pending session-expiry timer: this connection has resumed
	// (or replaced) the session before it expired. Spec5 [3.1.2.11.2].
	asm.cancelSessionExpiry(getHash(cid))
	c.mu.Lock()
	c.flags.set(connectReceived)
	c.mqtt.cp = cp
	c.mqtt.asm = asm
	c.mqtt.sess = es
	// This connection's own copy of the interval, set in the same critical section
	// as the Will it classifies. See mqtt.sessionExpiry.
	c.mqtt.sessionExpiry = expiry
	c.mu.Unlock()

	// Rebuild the outbound in-flight identifier space from durable state (R3,
	// inflight_register.go), BEFORE the CONNACK. Two orderings depend on it: no source
	// interest may exist yet (processSubs below installs it), and recoverDrainPubRels
	// stores PUBRELs under recorded identifiers that must already be reserved.
	//
	// Before the CONNACK rather than beside those steps, for a reason that is not
	// aesthetic: a client may publish the instant its CONNACK lands, and every
	// JetStream round trip taken after the CONNACK but before processSubs widens the
	// window in which a QoS 0 subscription does not yet exist and a message is simply
	// dropped. It also reads better as a rule — the identifier space is reconstructed
	// before the session is declared usable at all.
	//
	// A failure REFUSES the CONNECT: unlike a drain, whose barrier can withhold one
	// durable's interest, the identifier space is a single shared resource and half of
	// it cannot be used safely. Only reads JetStream and writes session memory, so it
	// takes no record transaction and is safe inside the accept transaction.
	//
	// From here to the success CONNACK, every failure exit goes through
	// abortAcceptedResume: the client is bound with connectReceived set and its
	// CONNECT (Will included) installed, so a bare error return would have the
	// readLoop send a v5 DISCONNECT as the FIRST packet ([MQTT-3.2.0-1]) and
	// close handling could publish the NEW connection's Will for a session that
	// was never granted. See sess_resume_reconcile.go.
	//
	// It also puts back the session lifetime this CONNECT overwrote. The accept
	// save has already persisted the requested interval, but the resume was
	// never acknowledged, so the client cannot know its session was touched —
	// and a requested interval of 0 would otherwise have the close path discard
	// a persistent session outright (doClean in mqttHandleClosedClient), after
	// which the client's retry gets Session Present 0 and the state is gone. The
	// close path re-persists the restored interval, so memory and record
	// converge. Spec5 [3.1.2.11.2].
	// foreign says the session has been disposed of because another owner holds
	// it: nothing about it may be restored, re-saved or re-armed here.
	// A refused resume needs no lifetime repair: the requested interval and Clean
	// flag are not applied until the grant point below, so the session still
	// holds — and its record still describes — the lifetime it had when this
	// CONNECT arrived.
	abortAcceptedResume := func(bool) {
		c.mqttAbortAcceptedConnect()
	}
	if sessp {
		if err := es.recoverInflightRegisters(c); err != nil {
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, err)
			abortAcceptedResume(false)
			return formatError(err)
		}
	}

	// Reconcile the plain QoS 1/2 durables the record names BEFORE the CONNACK:
	// a durable reaped by an inactivity threshold must be repaired (under a
	// fresh identity) or the CONNECT refused — never silently trusted into a
	// subscription that will not deliver. After register recovery, so every
	// in-flight register is classified against the OLD durable names before any
	// repair exists; before the CONNACK, so a failure refuses the CONNECT and
	// nothing has been transmitted when it fails. The repair save is, by
	// construction, the first record write after the accept save. See
	// sess_resume_reconcile.go.
	//
	// The phase's subscriptions are installed CLOSED and phase-owned;
	// restoration below adopts them in place. The sweep of whatever restoration
	// did not adopt has an owner on EVERY exit: it always runs right after
	// processSubs (before its error is even tested), and the deferred fallback
	// covers the exits that never reach it (the CONNACK failure and the 3.1.1
	// forced-leave failure returns). Idempotent, so the double run is harmless.
	var phaseSubs []string
	if sessp {
		installed, verdict, rerr := asm.reconcileResumedConsumers(es, c, acceptTok)
		if verdict != mqttReconcileProceed {
			c.Errorf("MQTT: refusing to resume session %q: %v", cid, rerr)
			abortAcceptedResume(verdict == mqttReconcileRefuseForeign)
			return formatError(rerr)
		}
		phaseSubs = installed
		if len(phaseSubs) > 0 {
			defer es.sweepUnadoptedPhaseSubs(c, phaseSubs)
		}
	}

	// GRANT the requested Session Expiry Interval and Clean flag. Everything that
	// could refuse this CONNECT has now passed, so this is the first moment the
	// session may take on what the client asked for — and it is persisted here
	// too, because the accept save deliberately carried the OLD lifetime and a
	// session whose record disagrees with its memory would be resumed, or
	// expired, on the wrong terms after a restart. Only when something actually
	// changed: an ordinary reconnect that keeps its interval writes nothing.
	//
	// A failure refuses the CONNECT and puts the two fields back. Nothing else
	// needs unwinding, because the record was never made to describe the new
	// lifetime — which is precisely what the earlier restore-after-the-fact
	// machinery existed to repair. Spec5 [3.1.2.11.2].
	if sessp {
		if err := es.grantRequestedLifetime(asm, c, acceptTok, expiry, sessClean); err != nil {
			c.Errorf("MQTT: refusing to resume session %q: could not grant the requested session lifetime: %v", cid, err)
			abortAcceptedResume(false)
			return formatError(err)
		}
	}

	// Spec [MQTT-3.2.0-1]: CONNACK must be the first protocol sent to the session.
	// The entry fit check used the identical frame builder and captured values, so
	// this send is guaranteed to fit; the guard is defensive — but the WRITE can
	// still fail, and an unacknowledged session must not proceed as accepted.
	if !sendConnAck(mqttConnAckRCConnectionAccepted, sessp, _EMPTY_) {
		abortAcceptedResume(false)
		return fmt.Errorf("unable to deliver the success CONNACK")
	}

	// A resuming MQTT 3.1.1 client cannot express, observe, or UNSUBSCRIBE a
	// "$share/..." filter (shared-subscription parsing is v5-only), so restoring
	// shared membership would create deliveries it can never manage and a phantom
	// holder no UNSUBSCRIBE could clear. Force-leave any persisted shared
	// subscriptions instead: remove them (transactionally) and release the group
	// consumers. On save failure keep membership and close the connection so a
	// later reconnect retries. Spec5 [4.8.2].
	if c.mqtt.proto != mqttProtoLevel5 && len(es.sharedSubs) > 0 {
		es.mu.Lock()
		prior := es.sharedSubs
		es.sharedSubs = nil
		// In the same breath as the membership removal, and before the save that
		// persists it: see voidRegistrationsLocked.
		var leaveCancels []mqttRegCancellation
		var leaveVoided []string
		voidLeave := func(dur string) {
			cancels, sent := es.voidRegistrationsLocked(dur)
			leaveCancels = append(leaveCancels, cancels...)
			leaveVoided = append(leaveVoided, sent...)
		}
		for _, cc := range es.cons {
			if cc.DeliverGroup != _EMPTY_ {
				voidLeave(cc.Durable)
			}
		}
		for key := range prior {
			for _, dur := range mqttSharedDurablesForKey(key) {
				voidLeave(dur)
			}
		}
		es.mu.Unlock()
		// Everything the forced leave does once its record write is CONFIRMED. A
		// closure, because that confirmation can also arrive later from the resolver:
		// skipping it then would leave the record without the shared memberships while
		// the runtime kept their cons entries, their MaxAckPending charge and their
		// group durables — stale state a v5 re-subscribe would trip over, and leaked
		// consumers nothing releases.
		//
		// The record token is a PARAMETER, never the captured acceptTok: as a landed
		// continuation this runs under the RESOLVER'S transaction, and the CONNECT's
		// own is long closed by then — every write under it would be refused at entry.
		commitLeave := func(tok recordTx) {
			// Also drop the resident session's in-memory shared consumer state
			// (cons entries, tmaxack charge, pending deliveries): a later v5
			// resubscribe of the same filter must not find a stale cons entry and
			// skip recreating a durable this forced leave may delete below.
			es.mu.Lock()
			var regs []mqttVoidedReg
			var cancels []mqttRegCancellation
			for sid, cc := range es.cons {
				if cc.DeliverGroup == _EMPTY_ {
					continue
				}
				delete(es.cons, sid)
				pregs, pcancels := es.purgeConsumerPendingLocked(cc.Durable)
				regs = append(regs, pregs...)
				cancels = append(cancels, pcancels...)
				if amt, ok := es.sharedCharged[cc.Durable]; ok {
					es.tmaxack -= amt
					if es.tmaxack < 0 {
						es.tmaxack = 0
					}
					delete(es.sharedCharged, cc.Durable)
				}
			}
			es.mu.Unlock()
			es.deleteInflightRegisters(c, tok, regs)
			// The staged cancellations are already in this CONNECT's record; anything the
			// purge just found arrived after that write, and the group durables below stay
			// while such a cancellation is unaccounted for — an unexplained register costs
			// an identifier for the life of the session, a surviving durable only costs
			// resources.
			es.secureRegCancellations(c, tok, leaveCancels)
			if !es.secureRegCancellations(c, tok, cancels) {
				c.Errorf("MQTT: the cancelled in-flight registration(s) of client %q are not "+
					"durably recorded yet on a 3.1.1 forced leave", es.id)
			}
			var durs []string
			for key := range prior {
				durs = append(durs, mqttSharedDurablesForKey(key)...)
			}
			held := es.sharedDurablesHeldByOthers(durs)
			for _, dur := range durs {
				if held[dur] {
					continue
				}
				if _, err := es.jsa.deleteConsumer(mqttStreamName, dur, true); isErrorOtherThan(err, JSConsumerNotFoundErr) {
					c.Errorf("MQTT: unable to delete shared consumer %q on 3.1.1 forced leave: %v", dur, err)
				}
			}
			c.Warnf("MQTT: dropped %d shared subscription(s) not supported for a 3.1.1 session resume", len(prior))
		}
		outcome, err := es.saveClassified(acceptTok)
		if err != nil {
			// Put the memberships back only if the removal DEFINITELY did not land.
			// An ambiguous write may already have stored the record without them, and
			// the resolver settles it forward by re-attempting exactly those bytes;
			// restoring here would leave memory holding entries the record has dropped,
			// and the next save would resurrect a shared membership the client cannot
			// see, observe or UNSUBSCRIBE. See sess_uncertain.go.
			undoLeave := func() {
				es.mu.Lock()
				es.sharedSubs = prior
				es.mu.Unlock()
				es.unvoidRegNonces(leaveVoided)
			}
			switch outcome {
			case saveDefinitelyFailed:
				undoLeave()
			case saveMaybeLanded:
				// The CONNECT is refused either way, so this connection does not stay to
				// finish the leave — which is exactly why the LANDED continuation matters:
				// nothing else would ever run it. Resolve now so the resident session is
				// usable again rather than permanently unable to persist.
				es.retainOnLanded(func(rtok recordTx) { commitLeave(rtok) })
				es.resolveUncertainNow(c)
			}
			return formatError(err)
		}
		commitLeave(acceptTok)
	}

	// Process possible saved subscriptions.
	if l := len(es.subs) + len(es.sharedSubs); l > 0 {
		filters := make([]*mqttFilter, 0, l)
		for subject, qos := range es.subs {
			_, noLocal := es.noLocalSubs[subject]
			_, rap := es.rapSubs[subject]
			filters = append(filters, &mqttFilter{filter: subject, qos: qos, noLocal: noLocal, retainAsPublished: rap, subID: es.subIDs[subject]})
		}
		// Restore shared subscriptions (v5 only; a 3.1.1 resume force-left them
		// above). The persisted key is the sole authority; parse it back into
		// group + underlying subject. Spec5 [4.8.2].
		for key, qos := range es.sharedSubs {
			if group, subject, ok := mqttParseSharedKey(key); ok {
				_, rap := es.rapSubs[key]
				filters = append(filters, &mqttFilter{shared: true, group: group, filter: subject, qos: qos, retainAsPublished: rap, subID: es.subIDs[key]})
			}
		}
		_, serr := asm.processSubs(es, c, acceptTok, filters, false, trace)
		// The sweep runs BEFORE the error is tested: a subscription restoration
		// skipped stays closed and phase-owned, holding consumer interest while
		// delivering nothing, and nothing later would remove it — the connection
		// stays live and a re-SUBSCRIBE's metadata refresh does not clear
		// `closed`. (On this restore path per-filter failures all continue rather
		// than harden the return, and the hard exits are fromSubProto-only, but
		// the sweep's promise must not be path-dependent.) The deferred fallback
		// above stays armed for the exits that never reach here.
		if len(phaseSubs) > 0 {
			es.sweepUnadoptedPhaseSubs(c, phaseSubs)
		}
		if serr != nil {
			return serr
		}
	}
	// The CONNECT's record transaction ends HERE, before the resume work below.
	// resendPendingRedelivery re-establishes the affine QoS 2 path under a
	// transaction of its own, and the fence's blocking form would deadlock against
	// one still held by this frame. Idempotent, so the deferred release is a no-op.
	endAcceptTx()

	// Restore the drains this session carries, so their started deliveries can be
	// redelivered and completed. Each of the steps below opens a record transaction
	// of its own, so they run only after the accept transaction is released, and
	// their ORDER is load-bearing (see unsub_drain.go):
	//
	//  1. make PUBREL delivery ready — needed whenever a PUBREL consumer is
	//     persisted or a QoS 2 drain was restored, and BEFORE the barrier, because
	//     $MQTT_out is interest-retained and a recreate with no consumer returns
	//     success while keeping nothing. Once per resume, not per drain: the
	//     barrier can finalize the last drain before a per-drain loop would run;
	//  2. if that fails, withhold every QoS 2 drain — without a delivery path an
	//     exchange cannot complete, so the barrier must not finalize one;
	//  3. run the past-PUBREC barrier, before any source interest exists;
	//  4. reconcile the rest against consumer state, skipping what the barrier
	//     could not resolve — a claim past PUBREC is owed a PUBREL, not a source
	//     message, and must not be pruned by the fate of its consumer;
	//  5. install source interest, then the NAKs, so a redelivery lands on a
	//     subscription that exists.
	skipDrains, pubRelReady := make(map[string]bool), true
	if es.needsPubRelDelivery() {
		if err := es.ensurePubRelConsumerSubscription(c); err != nil {
			c.Errorf("Unable to re-establish PUBREL delivery on resume: %v", err)
			pubRelReady = false
			for _, dur := range es.qos2DrainDurables() {
				skipDrains[dur] = true
			}
		}
	}
	if pubRelReady {
		skipDrains = es.recoverDrainPubRels(c)
	}
	es.recoverDrains(c, skipDrains)
	es.restoreDrains(c, skipDrains)

	asm.beforeResumeNaks(cid)
	// Re-send unacked QoS 1/2 messages (and reconcile any that expired while
	// offline) on resume, whether or not saved subscriptions were restored: a
	// session can carry unacknowledged PUBRELs with no active subscription.
	es.resendPendingRedelivery(c, resumePubs, resumeRels)
	return nil
}

// mqttConnAckReasonFromRC maps a 3.1.1 CONNACK return code to the equivalent
// MQTT 5.0 reason code. Codes already in the v5 failure range (>= 0x80) are
// passed through, which lets callers inject v5-only reasons (e.g. bad auth
// method) without a 3.1.1 equivalent. Spec5 [3.2.2.2].
func mqttConnAckReasonFromRC(rc byte) byte {
	switch rc {
	case mqttConnAckRCConnectionAccepted:
		return mqttReasonSuccess
	case mqttConnAckRCUnacceptableProtocolVersion:
		return mqttReasonUnsupportedProtocolVersion
	case mqttConnAckRCIdentifierRejected:
		return mqttReasonClientIDNotValid
	case mqttConnAckRCServerUnavailable:
		return mqttReasonServerUnavailable
	case mqttConnAckRCBadUserOrPassword:
		return mqttReasonBadUserOrPassword
	case mqttConnAckRCNotAuthorized:
		return mqttReasonNotAuthorized
	case mqttConnAckRCQoS2WillRejected:
		return mqttReasonQoSNotSupported
	}
	if rc >= 0x80 {
		return rc
	}
	return mqttReasonUnspecifiedError
}

// mqttEnqueueConnAck enqueues a CONNACK and reports whether it was sent. A v5
// CONNACK is suppressed (returns false) when it cannot fit the client's Maximum
// Packet Size even after dropping its Reason String; the 3.1.1 path always sends.
// It returns whether the CONNACK was sent (false only when a v5 frame exceeds
// the client's Maximum Packet Size even after dropping its Reason String) and
// the socket write error, so the caller can decide its abort discipline on a
// write failure.
//
// A clean success CONNACK records connAckWritten HERE, inside the same c.mu
// critical section as the write, not in the caller after the lock is released.
// The flag is the [MQTT-3.2.0-1] gate every takeover notification consults
// (mqttNotifyTakenOver reads it under c.mu): storing it after the unlock left a
// window in which a notifier could observe false for a client that had already
// received its CONNACK, and silently drop the DISCONNECT it is owed. Under the
// mutex, a notifier entering the critical section after the write completed is
// guaranteed to see the flag.
func (c *client) mqttEnqueueConnAck(rc byte, sessionPresent bool, reasonStr string) (bool, error) {
	// Spec [MQTT-3.2.2-4], spec5 [3.2.2.1.1]: when the (reason) code indicates
	// failure, the session present flag must be 0.
	sp := byte(0)
	if rc == 0 && sessionPresent {
		sp = 1
	}

	if c.mqtt.proto == mqttProtoLevel5 {
		frame := c.mqttMakeConnAckV5Frame(rc, sp, reasonStr)
		if !c.mqttFitsClientMax(len(frame)) {
			c.Debugf("Suppressing MQTT CONNACK of %d bytes exceeding client Maximum Packet Size %d", len(frame), c.mqtt.maxPacketSize)
			return false, nil
		}
		c.mu.Lock()
		err := c.enqueueProtoErr(frame)
		c.mqttRecordConnAckWrittenLocked(rc, err)
		c.mu.Unlock()
		return true, err
	}

	proto := [4]byte{mqttPacketConnectAck, 2, sp, rc}
	c.mu.Lock()
	err := c.enqueueProtoErr(proto[:])
	c.mqttRecordConnAckWrittenLocked(rc, err)
	c.mu.Unlock()
	return true, err
}

// mqttRecordConnAckWrittenLocked records that this connection has been told its
// CONNECT succeeded. Only a clean write of an accepted CONNACK counts: a failed
// write closed the socket and nothing was acknowledged, and a failure CONNACK
// never opens the connection for further packets. c.mu must be held — the flag
// gates every later takeover notification [MQTT-3.2.0-1], and holding the lock
// across write-then-store is what orders it against those readers.
func (c *client) mqttRecordConnAckWrittenLocked(rc byte, err error) {
	if err == nil && rc == mqttConnAckRCConnectionAccepted {
		c.connAckWritten.Store(true)
	}
}

// mqttMakeConnAckV5Frame builds an MQTT 5.0 CONNACK frame: a reason code, the
// session present flag, and a properties block advertising the server's
// capabilities. On a failure reason, reasonStr (when set and it fits) is added
// as a Reason String. The frame size is independent of sp (one byte either way),
// so the accept path can size it before knowing session present. Spec5 [3.2.2].
func (c *client) mqttMakeConnAckV5Frame(rc, sp byte, reasonStr string) []byte {
	reason := mqttConnAckReasonFromRC(rc)
	success := reason == mqttReasonSuccess

	props := &mqttProperties{present: map[byte]bool{}}
	// Advertise the capabilities of the current implementation. Properties left
	// absent take their spec defaults, which already describe us correctly, so
	// we MUST NOT send them with an explicit value:
	//   - Maximum QoS: absent => QoS 2 supported. (A value of 2 is illegal in
	//     this property: spec5 [3.2.2.3.4] allows only 0 or 1.)
	//   - Retain Available: absent => retained messages supported.
	//   - Wildcard Subscription Available: absent => wildcards supported.
	// We only emit the properties whose truthful value differs from the default.
	//   - Shared Subscription Available: absent => available. We implement them,
	//     so we emit nothing. Spec5 [3.2.2.3.11], [4.8.2].
	//   - Subscription Identifiers: absent => available (implemented), emit nothing.
	// Spec5 [3.2.2.3].
	// Topic Alias Maximum: advertise the (configured) number of inbound topic
	// aliases we accept. Success only (resolved on the accept path); omitted when
	// disabled: absent => 0, i.e. the client must not send any alias. Spec5
	// [3.2.2.3.2].
	if tam := c.mqtt.topicAliasMax; success && tam > 0 {
		props.topicAliasMax = tam
		props.present[mqttPropTopicAliasMax] = true
	}
	// Server Keep Alive: sent only when the server overrides the client's
	// requested keep alive (resolved on the accept path); absent => the client's
	// value applies. Spec5 [3.2.2.3.14].
	if ska := c.mqtt.serverKeepAlive; success && ska > 0 {
		props.serverKeepAlive = ska
		props.present[mqttPropServerKeepAlive] = true
	}
	// Maximum Packet Size: advertise the server's inbound limit (captured on the
	// accept path). Truthful: a packet over it gets a DISCONNECT 0x95 at the
	// framing layer, and an in-range PUBLISH whose NATS re-encoding exceeds the
	// account max payload gets a non-fatal 0x97 rather than being rejected here.
	// Omitted (absent => no limit) when there is none. Spec5 [3.2.2.3.6].
	if mps := c.mqtt.maxInPacketSize; success && mps > 0 {
		props.maxPacketSize = mps
		props.present[mqttPropMaxPacketSize] = true
	}
	// Receive Maximum: advertise how many inbound QoS 1/2 PUBLISH the server
	// processes concurrently (resolved on the accept path). Omitted when disabled:
	// absent => 65535, which the 16-bit packet identifier space can never exceed,
	// so not enforcing is truthful. Spec5 [3.2.2.3.3].
	if rm := c.mqtt.recvMaxIn; success && rm > 0 {
		props.receiveMax = rm
		props.present[mqttPropReceiveMaximum] = true
	}
	// Echo a server-assigned client identifier. Spec5 [3.2.2.3.7].
	if success && c.mqtt.cidGenerated {
		props.assignedClientID = c.mqtt.cid
	}
	// Response Information: returned only when the client requested it and the
	// server is configured with a base response topic (resolved on the accept
	// path). Spec5 [3.2.2.3.15].
	if success && c.mqtt.responseInfo != _EMPTY_ {
		props.responseInfo = c.mqtt.responseInfo
	}
	// On a failure reason, explain why with a Reason String (CONNACK is exempt
	// from Request Problem Information). Spec5 [3.2.2.3.9].
	if reason >= 0x80 {
		props.reasonString = c.mqttReasonString(mqttPacketConnectAck, reasonStr)
	}

	// Variable header = session present (1) + reason code (1) + properties.
	frame := func() []byte {
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketConnectAck)
		var vh mqttWriter
		vh.WriteByte(sp)
		vh.WriteByte(reason)
		vh.writeProperties(props)
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		return w.Bytes()
	}
	buf := frame()
	// Drop the Reason String (only) if it pushed the packet past the client's
	// Maximum Packet Size. Spec5 [3.1.2.11.4].
	if props.reasonString != _EMPTY_ && !c.mqttFitsClientMax(len(buf)) {
		props.reasonString = _EMPTY_
		buf = frame()
	}
	return buf
}

// Reason String sent with the DISCONNECT to a client whose session is taken
// over by a new connection with the same client ID. Spec5 [3.1.4].
const mqttReasonStrSessionTakenOver = "session taken over by a new connection"

// Reason String sent with the 0x89 DISCONNECT to a client whose session record was
// found to have FORKED — a record write derived from the same base as ours survived,
// so our own was erased without anything establishing another owner. Deliberately not
// mqttReasonStrSessionTakenOver: pairing that text with 0x89 would make exactly the
// claim the fork verdict exists to withhold. See sess_fork_lineage.go.
const mqttReasonStrOwnershipUnsettled = "session ownership could not be established; try again"

// Reason String sent with the DISCONNECT to every connected client when the
// broker is draining connections for shutdown. Spec5 [3.14.2.1].
const mqttReasonStrServerShuttingDown = "server shutting down"

// mqttReasonString returns reasonStr if it may accompany a server-generated
// packet of type pktType, or empty otherwise: v5 only; a client that sent
// Request Problem Information 0 gets one only on CONNACK/DISCONNECT (where the
// server MAY send it regardless); and it must be a valid MQTT UTF-8 string
// ([MQTT-1.5.4-1]) no longer than an MQTT UTF-8 string can encode (the writer
// lays down a uint16 length, so an over-long string would wrap it and corrupt
// the block). Callers may pass client-derived error text, so both checks matter.
// Spec5 [1.5.4], [3.1.2.11.7].
func (c *client) mqttReasonString(pktType byte, reasonStr string) string {
	if reasonStr == _EMPTY_ || c.mqtt == nil || c.mqtt.proto != mqttProtoLevel5 {
		return _EMPTY_
	}
	if c.mqtt.noProblemInfo && pktType != mqttPacketConnectAck && pktType != mqttPacketDisconnect {
		return _EMPTY_
	}
	if len(reasonStr) > 0xFFFF || mqttValidateString(reasonStr, "reason string") != nil {
		return _EMPTY_
	}
	return reasonStr
}

// mqttFitsClientMax reports whether a framed packet of n bytes fits the client's
// MQTT 5.0 Maximum Packet Size (0 = no limit). An oversized packet must drop its
// Reason String rather than be sent. Spec5 [3.1.2.11.4].
func (c *client) mqttFitsClientMax(n int) bool {
	return c.mqtt == nil || c.mqtt.maxPacketSize == 0 || uint32(n) <= c.mqtt.maxPacketSize
}

// mqttEnqueueFittingProto enqueues buf unless it would exceed the client's
// Maximum Packet Size; the server MUST NOT send such a packet [MQTT-3.1.2-24].
// Returns whether it was enqueued.
func (c *client) mqttEnqueueFittingProto(buf []byte) bool {
	if !c.mqttFitsClientMax(len(buf)) {
		c.Debugf("Suppressing MQTT packet of %d bytes exceeding client Maximum Packet Size %d", len(buf), c.mqtt.maxPacketSize)
		return false
	}
	return c.mqttEnqueueProtoLocked(buf, false)
}

// mqttEnqueueFittingProtoUnlessDisconnected is mqttEnqueueFittingProto for a packet
// that must not follow a server-initiated DISCONNECT on this connection.
func (c *client) mqttEnqueueFittingProtoUnlessDisconnected(buf []byte) bool {
	if !c.mqttFitsClientMax(len(buf)) {
		c.Debugf("Suppressing MQTT packet of %d bytes exceeding client Maximum Packet Size %d", len(buf), c.mqtt.maxPacketSize)
		return false
	}
	return c.mqttEnqueueProtoLocked(buf, true)
}

// mqttEnqueueProtoLocked enqueues buf under c.mu, optionally refusing when this
// connection's one server-initiated DISCONNECT has already been ATTEMPTED.
//
// The refusal has to happen in the SAME critical section as the write, for the same
// reason every enqueue-path DISCONNECT writer claims in its own: a check made and
// released first is a check-then-act, and the interval between them is exactly when a
// displacement — the watch's eviction worker needs no lock of ours — notifies and detaches.
// A packet appended after that notification violates [MQTT-3.14.4-1].
//
// It gates only the packets whose callers ask for it. Nothing else about the
// connection's outbound traffic changes: an in-flight PUBLISH or a PINGRESP can still
// trail a DISCONNECT, which is the stated residual of the claim mechanism.
func (c *client) mqttEnqueueProtoLocked(buf []byte, unlessDisconnected bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if unlessDisconnected && (c.mqtt == nil || c.mqtt.disconnectSent) {
		return false
	}
	c.enqueueProto(buf)
	return true
}

// mqttServerMaxPacketSize returns the server's MQTT 5.0 Maximum Packet Size for
// this connection, derived from the account max payload, or 0 when there is no
// effective limit (unlimited, or a limit at/above the largest legal control
// packet). Spec5 [3.2.2.3.6].
func (c *client) mqttServerMaxPacketSize() uint32 {
	mpay := atomic.LoadInt32(&c.mpay)
	if mpay <= 0 || mpay > mqttMaxControlPacketSize {
		return 0
	}
	return uint32(mpay)
}

// mqttReasonProps returns a properties block carrying reasonStr as a Reason
// String for a server-generated packet of type pktType, or nil (which encodes as
// a single zero length byte, byte-identical to no properties) when the string is
// empty or not permitted. Spec5 [3.9.2.2.2].
func (c *client) mqttReasonProps(pktType byte, reasonStr string) *mqttProperties {
	if rs := c.mqttReasonString(pktType, reasonStr); rs != _EMPTY_ {
		return &mqttProperties{reasonString: rs}
	}
	return nil
}

// mqttClaimDisconnectLocked consumes this connection's one server-initiated
// DISCONNECT notification and reports whether the caller may write it. The
// second and every later would-be sender is refused: after sending a DISCONNECT
// the sender must not send any more MQTT Control Packets on that Network
// Connection [MQTT-3.14.4-1], and the displacement paths race, so which reason
// the client is told is decided here rather than by whoever reaches the socket
// first. Every server-DISCONNECT writer claims through this one predicate; the
// enqueue-path writers do it in the same c.mu critical section as their write,
// so for them there is no check-then-act window at all. shutdownDisconnect is
// the one exception, and there the claim is a RESERVATION: it claims under the
// lock and writes through writeDirect after releasing it, so it can spend the
// claim — suppressing a competing notification — and then write nothing, or
// fail. That is why the guarantee is at most one ATTEMPT and not one delivered
// packet. c.mu must be held.
func (c *client) mqttClaimDisconnectLocked() bool {
	if c.mqtt == nil || c.mqtt.disconnectSent {
		return false
	}
	c.mqtt.disconnectSent = true
	return true
}

// mqttEnqueueDisconnect sends an MQTT 5.0 DISCONNECT with the given reason code
// (and, when allowed and it fits, a Reason String) to the client, before the
// connection is closed on a protocol/processing error. It is a no-op for 3.1.1
// connections, which have no server-initiated DISCONNECT, and for a connection
// whose one DISCONNECT another path already claimed. Spec5 [3.14].
func (c *client) mqttEnqueueDisconnect(reason byte, reasonStr string) {
	pkt := c.mqttBuildDisconnect(reason, reasonStr)
	if pkt == nil {
		// Nothing was sent, so [MQTT-3.14.4-1] never triggered: the claim stays
		// available to a later sender rather than being spent on no packet.
		return
	}
	c.mu.Lock()
	if c.mqttClaimDisconnectLocked() {
		c.enqueueProto(pkt)
	}
	c.mu.Unlock()
}

// mqttNotifyTakenOver tells a displaced connection that its session was taken
// over (0x8E). It is the single tail shared by all four paths that displace a
// bound connection — a local CONNECT takeover, a remote persist observation
// (the session-record watch's eviction worker), a resolver's foreign disposal and the bound rejection
// settlement — so that "displace this connection" has one notification owner
// instead of four writers racing to the socket.
//
// Two conditions, both required. The connection must already have been told its
// CONNECT succeeded: CONNACK must be the first packet a client sees
// [MQTT-3.2.0-1], and mqttAbortAcceptedConnect leaves a refused connection bound
// until its readLoop detaches it, so a displacement can find a bound but
// pre-CONNACK connection — which is then closed silently. And no server
// DISCONNECT may have been attempted yet ([MQTT-3.14.4-1],
// mqttClaimDisconnectLocked).
//
// The gate is read under c.mu, which orders it against the CONNACK writer's
// store in mqttEnqueueConnAck's own critical section: a notifier entering after
// the CONNACK write completed is guaranteed to see it. The packet is built
// before the lock (mqttBuildDisconnect reads only fields immutable after CONNECT
// parsing) because enqueueProto is called with c.mu held and the mutex is not
// reentrant.
//
// No lock held on entry.
func (c *client) mqttNotifyTakenOver() { c.mqttNotifyDisplaced(mqttDisposalTakenOver) }

// mqttNotifyDisplaced is mqttNotifyTakenOver's body, with WHAT is reported passed in.
//
// The vocabulary travels as one value because it is one claim made four ways: a caller
// that could pass a reason code alone would emit 0x89 "server busy" alongside "session
// taken over by a new connection", which is exactly the assertion a fork verdict exists
// to withhold. Both gates, the DISCONNECT claim and the 3.1.1 no-op are inherited
// unchanged — they belong to the displacement, not to its reason. See
// sess_fork_lineage.go.
func (c *client) mqttNotifyDisplaced(voc mqttDisposalVocabulary) {
	pkt := c.mqttBuildDisconnect(voc.reason, voc.reasonStr)
	if pkt == nil {
		return
	}
	c.mu.Lock()
	if c.connAckWritten.Load() && c.mqttClaimDisconnectLocked() {
		c.enqueueProto(pkt)
	}
	c.mu.Unlock()
}

// mqttBuildDisconnect returns the wire bytes for a server-initiated MQTT 5.0
// DISCONNECT with the given reason code (and, when allowed and it fits, a Reason
// String), or nil for a 3.1.1 connection (which has no server DISCONNECT) or
// when even the 3-byte short form would exceed the client's Maximum Packet Size.
// It builds only; the caller enqueues (mqttEnqueueDisconnect) or writes it
// directly under a deadline (shutdownDisconnect). Spec5 [3.14].
func (c *client) mqttBuildDisconnect(reason byte, reasonStr string) []byte {
	if c.mqtt == nil || c.mqtt.proto != mqttProtoLevel5 {
		return nil
	}
	if rs := c.mqttReasonString(mqttPacketDisconnect, reasonStr); rs != _EMPTY_ {
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketDisconnect)
		var vh mqttWriter
		vh.WriteByte(reason)
		vh.writeProperties(&mqttProperties{reasonString: rs})
		w.WriteVarInt(vh.Len())
		w.Write(vh.Bytes())
		if c.mqttFitsClientMax(w.Len()) {
			return w.Bytes()
		}
		// Falls through to the short form when the Reason String would overflow.
	}
	// Remaining length of 1: just the reason code, properties omitted (allowed
	// when there are no properties). Suppressed if even this doesn't fit (only
	// possible before a success CONNACK bounds the client's tiny limit); the
	// connection closes regardless. Spec5 [3.14.2.2.1].
	proto := [3]byte{mqttPacketDisconnect, 1, reason}
	if !c.mqttFitsClientMax(len(proto)) {
		return nil
	}
	return append([]byte(nil), proto[:]...)
}

// mqttDisconnectReasonFromErr picks a v5 DISCONNECT reason code for a parse or
// processing error. Spec5 [3.14.2.1].
func mqttDisconnectReasonFromErr(err error) byte {
	switch {
	case errors.Is(err, errMQTTMalformedVarInt), errors.Is(err, errMQTTMalformedProperties),
		errors.Is(err, errMQTTMalformedSubOption), errors.Is(err, errMQTTMalformedSharedSub),
		errors.Is(err, errMQTTMalformedPacket), errors.Is(err, errMQTTTopicFilterCannotBeEmpty),
		errors.Is(err, errMQTTUnsupportedCharacters), errors.Is(err, errMQTTInvalidPublishLength):
		return mqttReasonMalformedPacket
	case errors.Is(err, errMQTTUnknownProperty), errors.Is(err, errMQTTPropertyNotAllowed),
		errors.Is(err, errMQTTDuplicateProperty), errors.Is(err, errMQTTProtocolError),
		errors.Is(err, errMQTTSecondConnectPacket), errors.Is(err, errMQTTTopicIsEmpty):
		return mqttReasonProtocolError
	case errors.Is(err, errMQTTTopicAliasInvalid):
		return mqttReasonTopicAliasInvalid
	case errors.Is(err, errMQTTReceiveMaxExceeded):
		return mqttReasonReceiveMaxExceeded
	case errors.Is(err, errMQTTPayloadFormatInvalid):
		return mqttReasonPayloadFormatInvalid
	case errors.Is(err, errMQTTSessionOwnershipUnsettled):
		// Nothing arbitrated the ownership question, so this is "try again later", not
		// 0x8E: a takeover nobody proved must not be announced as one. See
		// sess_rejected_write.go.
		return mqttReasonServerBusy
	}
	return mqttReasonUnspecifiedError
}

// mqttConnAckReasonFromConnectErr picks a v5 CONNACK reason code for a CONNECT
// that failed to parse without a return code: property errors are Protocol
// Error (0x82), everything else Malformed Packet (0x81). Spec5 [3.1.4.1].
func mqttConnAckReasonFromConnectErr(err error) byte {
	switch {
	case errors.Is(err, errMQTTPayloadFormatInvalid):
		return mqttReasonPayloadFormatInvalid
	case errors.Is(err, errMQTTUnknownProperty), errors.Is(err, errMQTTPropertyNotAllowed),
		errors.Is(err, errMQTTDuplicateProperty), errors.Is(err, errMQTTProtocolError):
		return mqttReasonProtocolError
	}
	return mqttReasonMalformedPacket
}

// mqttPayloadFormatIsUTF8 reports whether a raw MQTT 5.0 properties block carries
// a Payload Format Indicator of 1 (the payload is declared UTF-8). Spec5
// [3.3.2.3.2].
func mqttPayloadFormatIsUTF8(props []byte) bool {
	off := mqttPropValueOffset(props, mqttPropPayloadFormat)
	return off >= 0 && off < len(props) && props[off] == 1
}

// mqttHandleWill publishes a disconnected client's Will Message, if any. For an
// MQTT 5.0 client that set a Will Delay Interval on a session that survives the
// connection, publication is deferred so a timely reconnect can cancel it. Spec5
// [3.1.3.2.2], [MQTT-3.1.2-8].
func (s *Server) mqttHandleWill(c *client, sessionExpiry uint32, sessGen string, originSessionSeq uint64, originSessEpoch string) {
	c.mu.Lock()
	// Consume the Will under the same lock the disposition is transitioned under:
	// this runs twice for a graceful DISCONNECT (see mqttHandleClosedClient), and
	// only the first invocation may act on it.
	will, disposition := c.mqttTakeWillLocked()
	if will == nil {
		c.mu.Unlock()
		return
	}
	// MQTT 5.0 Will Delay Interval: defer publication when a delay is set and the
	// session survives the connection. The session survives iff its (effective)
	// Session Expiry Interval is non-zero; a zero interval ends the session at
	// connection close, so the Will is published immediately. The pending Will is
	// tracked on the account manager keyed by client ID (not on the session object,
	// which a clean-start client removes on disconnect) so a reconnect can cancel
	// it. Spec5 [3.1.3.2.2], [MQTT-3.1.2-8].
	//
	// The close classified this already; recompute only for a Will that somehow
	// reached here still armed, so the two can never disagree.
	delayed := disposition == mqttWillOwedDelayed
	if disposition == mqttWillArmed {
		delayed = mqttWillIsDelayed(c.mqtt.proto, will, sessionExpiry)
	}
	if delayed {
		asm := c.mqtt.asm
		// Spec5 [3.1.3.2.2]: publish at the Will Delay Interval OR when the session
		// ends, whichever is first, so cap the delay at the Session Expiry Interval.
		// (mqttSessionNeverExpire, being the max uint32, never lowers the delay.)
		effDelay := will.props.willDelay
		if sessionExpiry < effDelay {
			effDelay = sessionExpiry
		}
		fireAt := time.Now().Add(time.Duration(effDelay) * time.Second).UnixNano()
		props := mqttWillForwardProps(will.props)
		user := c.opts.Username
		idHash := getHash(c.mqtt.cid)
		// A delayed Will cannot re-check publish permissions at fire time (they are
		// not serializable), so evaluate them now and never enqueue a denied Will.
		willAllowed := c.pubAllowedFullCheck(string(will.subject), false, true)
		c.mu.Unlock()
		// Record the Will on the durable outbox ($MQTT_willq) so a surviving
		// replica fires it if this broker exits during the delay. Every replica's
		// claim loop drives it to publication once FireAt passes; a reconnect
		// cancels it. See will_outbox.go. Spec5 [3.1.3.2.2], [MQTT-3.1.2-8].
		if asm != nil && willAllowed {
			// enqueueDelayedWill retries internally and logs on exhausted retries;
			// there is nothing further to do on the disconnect path.
			_ = asm.enqueueDelayedWill(s, idHash, sessGen, will, props, user, fireAt, originSessionSeq, originSessEpoch)
		}
		return
	}
	pp := c.mqtt.pp
	pp.topic = will.topic
	pp.subject = will.subject
	pp.mapped = will.mapped
	pp.msg = will.message
	pp.sz = len(will.message)
	pp.pi = 0
	pp.flags = will.qos << 1
	if will.retain {
		pp.flags |= mqttPubFlagRetain
	}
	// pp is the shared publish struct; replace any properties left over from
	// the client's last PUBLISH with the Will's own. Spec5 [3.1.3.2].
	pp.props = mqttWillForwardProps(will.props)
	c.mu.Unlock()
	// A Will is server-originated: no No Local origin, matching the delayed Will
	// path so suppression does not depend on the Will Delay Interval. Spec5 [3.8.3.1].
	s.mqttInitiateMsgDelivery(c, pp, _EMPTY_)
	c.flushClients(0)
}

// mqttPublishOwedWill discharges the Will of a connection whose teardown could
// not take the session gate because the session has been rebound to a newer
// connection. Nothing else will: the teardown that would have published it is
// the one that just gave up, and the takeover deliberately does not publish the
// Will of the connection it displaces.
//
// Only an immediate Will is published here. A delayed one is precisely what a
// reconnect is entitled to cancel, and the reconnect that took this session over
// is that cancellation — it advanced the durable cancel watermark before its
// CONNACK. Spec5 [3.1.2.5].
//
// Publication goes through the account's internal client rather than this dying
// one, whose NATS connection the concurrent takeover close may be tearing down
// underneath it (mqttProcessConnect closes the displaced client asynchronously).
//
// No lock held on entry.
func (s *Server) mqttPublishOwedWill(c *client, asm *mqttAccountSessionManager) {
	c.mu.Lock()
	if c.mqtt == nil || c.mqtt.willDisposition != mqttWillOwedNow {
		c.mu.Unlock()
		return
	}
	will, _ := c.mqttTakeWillLocked()
	user := c.opts.Username
	// The internal account client publishes with permissions of its own, so the
	// dying connection's are checked here, while it is still around to ask —
	// exactly as the delayed path does before it enqueues.
	allowed := will != nil && c.pubAllowedFullCheck(string(will.subject), false, true)
	c.mu.Unlock()
	if will == nil || !allowed {
		return
	}
	rec := &mqttWillOutbox{WillID: nuid.Next(), Account: asm.jsa.c.acc.GetName()}
	willOutboxMessage(rec, will, mqttWillForwardProps(will.props), user)
	if err := asm.deliverWillRecord(s, rec); err != nil {
		c.Errorf("Unable to publish the Will of a session taken over mid-disconnect: %v", err)
	}
}

// mqttWillForwardProps encodes a Will's properties as the raw block (length
// prefix + body, the pp.props format) to attach to the Will PUBLISH. Will Delay
// Interval is consumed by the server and has no encode case, so it is never
// forwarded. Spec5 [3.1.3.2].
func mqttWillForwardProps(p *mqttProperties) []byte {
	if p == nil {
		return nil
	}
	w := newMQTTWriter(0)
	w.writeProperties(p)
	// Just the zero-length varint: nothing to forward.
	if w.Len() <= 1 {
		return nil
	}
	return w.Bytes()
}

//////////////////////////////////////////////////////////////////////////////
//
// PUBLISH protocol related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParsePub(r *mqttReader, pl int, pp *mqttPublish, hasMappings bool) error {
	qos := mqttGetQoS(pp.flags)
	if qos > 2 {
		return fmt.Errorf("%w: QoS=%v is invalid in MQTT", errMQTTMalformedPacket, qos)
	}

	if c.mqtt.rejectQoS2Pub && qos == 2 {
		return fmt.Errorf("QoS=2 is disabled for PUBLISH messages")
	}

	// Keep track of where we are when starting to read the variable header
	start := r.pos

	var err error
	pp.topic, err = r.readBytes("topic", false)
	if err != nil {
		return err
	}
	v5 := c.mqtt.proto == mqttProtoLevel5
	// For v5 an empty topic is legal when a previously bound Topic Alias
	// resolves it, so defer the check until after properties are read. 3.1.1
	// has no aliases, so reject an empty topic immediately.
	if len(pp.topic) == 0 && !v5 {
		return errMQTTTopicIsEmpty
	}

	if qos > 0 {
		pp.pi, err = r.readUint16("packet identifier")
		if err != nil {
			return err
		}
		if pp.pi == 0 {
			return fmt.Errorf("with QoS=%v, packet identifier cannot be 0", qos)
		}
	} else {
		pp.pi = 0
	}

	// MQTT 5.0: PUBLISH properties follow the packet identifier (or topic, for
	// QoS 0) and precede the payload. Spec5 [3.3.2.3]. Reading here also
	// advances the reader so the payload-size computation below is correct.
	// pp is reused across packets (c.mqtt.pp), so pp.props is assigned on every
	// path below, never left carrying the previous packet's block.
	pp.props = nil
	if v5 {
		propStart := r.pos
		props, perr := r.readProperties(mqttPacketPub)
		if perr != nil {
			return perr
		}
		// Raw block (length prefix + body) as received: forwarded verbatim to v5
		// subscribers unless it carries a Topic Alias, which is hop-local and
		// stripped below. An inbound client PUBLISH can otherwise only carry
		// forwardable properties (Subscription Identifier is server->client
		// only), so verbatim re-emit is spec-correct. Spec5 [3.3.2.3].
		raw := r.buf[propStart:r.pos]
		if props != nil && props.present[mqttPropTopicAlias] {
			// A Topic Alias of 0, or above the maximum we advertised, is
			// invalid; this also covers the disabled case (maximum 0). Spec5
			// [3.3.2.3.4].
			alias := props.topicAlias
			if alias == 0 || alias > c.mqtt.topicAliasMax {
				return fmt.Errorf("%w: %d (maximum %d)", errMQTTTopicAliasInvalid, alias, c.mqtt.topicAliasMax)
			}
			if len(pp.topic) > 0 {
				// Bind (or re-bind, which the spec allows) the alias to this
				// topic. Copy: the reader buffer is transient.
				if c.mqtt.topicAliases == nil {
					c.mqtt.topicAliases = make(map[uint16][]byte)
				}
				c.mqtt.topicAliases[alias] = copyBytes(pp.topic)
			} else {
				// Alias-only PUBLISH: resolve to the bound topic. An unknown
				// alias with an empty topic is a Protocol Error. Spec5 [3.3.2.3.4].
				topic, ok := c.mqtt.topicAliases[alias]
				if !ok {
					return fmt.Errorf("%w: unknown topic alias %d", errMQTTProtocolError, alias)
				}
				pp.topic = topic
			}
			// Strip the alias so it is never forwarded or stored (hop-local).
			pp.props = mqttStripTopicAlias(raw)
		} else {
			if len(pp.topic) == 0 {
				return errMQTTTopicIsEmpty
			}
			// Keep the block only if it carries something (more than the single
			// zero length byte). Copy: the reader buffer is transient.
			if len(raw) > 1 {
				pp.props = copyBytes(raw)
			}
		}
	}

	if err := mqttValidateTopic(pp.topic, "topic"); err != nil {
		return err
	}
	// Convert the topic to a NATS subject. This call will also check that
	// there is no MQTT wildcards (Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1])
	// Note that this may not result in a copy if there is no conversion.
	// It is good because after the message is processed we won't have a
	// reference to the buffer and we save a copy.
	pp.subject, err = mqttTopicToNATSPubSubject(pp.topic)
	if err != nil {
		return err
	}

	// Check for subject mapping. pp is reused across packets, so reset mapped
	// unconditionally: it is only set when a mapping applies, and a stale value
	// from a prior mapped publish must not leak into this one.
	pp.mapped = nil
	if hasMappings {
		// For selectMappedSubject to work, we need to have c.pa.subject set.
		// If there is a change, c.pa.mapped will be set after the call.
		c.pa.subject = pp.subject
		if changed := c.selectMappedSubject(); changed {
			// We need to keep track of the NATS subject/mapped in the `pp` structure.
			pp.subject = c.pa.subject
			pp.mapped = c.pa.mapped
			// We also now need to map the original MQTT topic to the new topic
			// based on the new subject.
			pp.topic = natsSubjectToMQTTTopic(pp.subject)
		}
		// Reset those now.
		c.pa.subject, c.pa.mapped = nil, nil
	}

	// The message payload will be the total packet length minus
	// what we have consumed for the variable header
	payloadSize := pl - (r.pos - start)
	if payloadSize < 0 {
		return fmt.Errorf("%w: invalid remaining length %d for PUBLISH packet", errMQTTMalformedPacket, pl)
	}
	pp.sz = payloadSize
	if pp.sz > 0 {
		start = r.pos
		r.pos += pp.sz
		pp.msg = r.buf[start:r.pos]
	} else if pp.sz == 0 {
		pp.msg = nil
	} else {
		return errMQTTInvalidPublishLength
	}
	return nil
}

func mqttValidateTopic(topic []byte, field string) error {
	if !utf8.Valid(topic) {
		return fmt.Errorf("%w: invalid utf8 for %s %q", errMQTTMalformedPacket, field, topic)
	}
	if bytes.IndexByte(topic, 0) >= 0 {
		return fmt.Errorf("%w: invalid null character in %s %q", errMQTTMalformedPacket, field, topic)
	}
	return nil
}

func mqttValidateString(value string, field string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: invalid utf8 for %s %q", errMQTTMalformedPacket, field, value)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return fmt.Errorf("%w: invalid null character in %s %q", errMQTTMalformedPacket, field, value)
	}
	return nil
}

func mqttPubTrace(pp *mqttPublish) string {
	dup := pp.flags&mqttPubFlagDup != 0
	qos := mqttGetQoS(pp.flags)
	retain := mqttIsRetained(pp.flags)
	var piStr string
	if pp.pi > 0 {
		piStr = fmt.Sprintf(" pi=%v", pp.pi)
	}
	return fmt.Sprintf("%s dup=%v QoS=%v retain=%v size=%v%s",
		pp.topic, dup, qos, retain, pp.sz, piStr)
}

// mqttOriginMarker returns the authenticated No Local origin marker for a
// message published by session idHash on subject with the given payload: an
// HMAC of (idHash, subject, payload) under the server's per-run secret.
//
// It authenticates only a narrow claim used by the No Local decision — "this
// subscriber's own session published this subject+payload" — not that the whole
// stored message is server-created or fresh. Because the secret never leaves the
// server, a client cannot forge a marker for a session it does not control. The
// marker is deterministic (hence emitted in a readable header and observable),
// so it can be replayed; that is not a weakness here, because the marker is
// keyed by the origin idHash: replaying a victim's marker can only re-assert the
// victim's own origin for that same subject+payload, which No Local suppresses
// anyway, while an independently-authored message carries a different origin's
// marker and is delivered. Empty when there is no key or no session (nothing to
// authenticate). idHash and subject contain no NUL, so the NUL separators
// delimit the fields unambiguously. Spec5 [3.8.3.1].
func mqttOriginMarker(key []byte, idHash, subject string, payload []byte) string {
	if len(key) == 0 || idHash == _EMPTY_ {
		return _EMPTY_
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(idHash))
	mac.Write([]byte{0})
	mac.Write([]byte(subject))
	mac.Write([]byte{0})
	mac.Write(payload)
	// 16 bytes of tag is ample against forgery and keeps the header small.
	return base64.RawStdEncoding.EncodeToString(mac.Sum(nil)[:16])
}

// mqttStampedOrigin is the origin marker to stamp on a message this client is
// publishing on subject with payload, or empty when the server does not run v5
// (no No Local subscription can exist) or the client has no session (e.g. an
// internal Will-delivery client, so a Will is never treated as self-published).
// Spec5 [3.8.3.1].
func (c *client) mqttStampedOrigin(subject string, payload []byte) string {
	if c.mqtt == nil || c.mqtt.sess == nil || c.srv == nil || !c.srv.mqtt.v5Enabled {
		return _EMPTY_
	}
	return mqttOriginMarker(c.srv.mqtt.originKey, c.mqtt.sess.idHash, subject, payload)
}

// mqttOriginRetained reports whether pp was published with the RETAIN flag and
// v5 is enabled on this server, so its original RETAIN should be recorded (in
// the Nmqtt-Ret header) for the Retain As Published subscription option. Must be
// called while pp.flags still carries the original RETAIN (before fan-out clears
// it). A 3.1.1-only server records nothing. Spec5 [3.8.3.1].
func (c *client) mqttOriginRetained(pp *mqttPublish) bool {
	return c.srv != nil && c.srv.mqtt.v5Enabled && mqttIsRetained(pp.flags)
}

// mqttNoLocalSuppress reports whether a No Local subscription must drop this
// delivery: the message was published by the same session (its ID hash matches
// the subscriber's). An unknown origin (empty hash) never suppresses. Spec5
// [3.8.3.1].
func mqttNoLocalSuppress(noLocal bool, originHash, subHash string) bool {
	return noLocal && originHash != _EMPTY_ && originHash == subHash
}

// mqttComputeNatsMsgSize computes the size the NATS message to be delivered
// based on a MQTT PUBLISH packet.
// encodePP: whether to encode complete MQTT PUBLISH packet header information
//   - false: initial delivery (QoS 0/1) needs only base header
//   - true: QoS2 storage needs to encode Nmqtt-Subject and Nmqtt-Mapped
//
// msgId and q2Gen are the inbound-QoS 2 move protocol's two headers and are
// mutually exclusive by construction: msgId (Nats-Msg-Id) rides the RELEASE form
// stored onto $MQTT_msgs, q2Gen (Nmqtt-Q2Gen) rides the HOLD form stored onto
// $MQTT_qos2in. Every other call site passes _EMPTY_ for both. This function must
// stay byte-exact with mqttNewDeliverableMessage below: a disagreement makes
// headerLen wrong and the stored message corrupt.
func mqttComputeNatsMsgSize(pp *mqttPublish, encodePP bool, ttl uint32, origin string, retained bool, msgId, q2Gen string) int {
	size := len(hdrLine) +
		len(mqttNatsHeader) + 2 + 2 + // 2 for ':<qos>', and 2 for CRLF
		2 + // end-of-header CRLF
		pp.sz
	if len(origin) > 0 {
		size += len(mqttNatsHeaderOrigin) + 1 + // +1 for ':'
			len(origin) + 2 // 2 for CRLF
	}
	if retained {
		size += len(mqttNatsHeaderRetain) + 1 + 1 + 2 // ':' + "1" + CRLF
	}
	if encodePP {
		size += len(mqttNatsHeaderSubject) + 1 + // +1 for ':'
			len(pp.subject) + 2 // 2 for CRLF

		if len(pp.mapped) > 0 {
			size += len(mqttNatsHeaderMapped) + 1 + // +1 for ':'
				len(pp.mapped) + 2 // 2 for CRLF
		}
	}
	// MQTT 5.0 properties are carried (base64) so they can be forwarded to v5
	// subscribers. Independent of encodePP: needed for both delivery and storage.
	if len(pp.props) > 0 {
		size += len(mqttNatsHeaderProps) + 1 + // +1 for ':'
			base64.StdEncoding.EncodedLen(len(pp.props)) + 2 // 2 for CRLF
	}
	// MQTT 5.0 Message Expiry Interval maps to a JetStream per-message TTL so the
	// stored copy is dropped once its lifetime elapses. Spec5 [3.3.2.3.3].
	if ttl > 0 {
		size += len(JSMessageTTL) + 1 + // +1 for ':'
			len(strconv.FormatUint(uint64(ttl), 10)) + 1 + 2 // +1 for 's', 2 for CRLF
	}
	// Inbound QoS 2 move protocol. See qos2_move.go: msgId makes a repeated
	// RELEASE store non-duplicating for stream-backed subscribers, q2Gen is the
	// hold record's immutable identity. Both are bounded by named constants that
	// the max_payload preflight uses (mqttQoS2MsgIdHeaderMax /
	// mqttQoS2GenHeaderMax), so a value longer than its bound would be accepted
	// here and then rejected by the broker — only ever pass the fixed forms.
	if len(msgId) > 0 {
		size += len(JSMsgId) + 1 + // +1 for ':'
			len(msgId) + 2 // 2 for CRLF
	}
	if len(q2Gen) > 0 {
		size += len(mqttQoS2GenHeader) + 1 + // +1 for ':'
			len(q2Gen) + 2 // 2 for CRLF
	}
	return size
}

// mqttMaxQoS2FormSize returns the largest NATS message a QoS 2 PUBLISH will ever
// be encoded as. delivered is the size of the ordinary delivered/stored form
// (already computed by the caller, since it is the QoS-independent baseline).
//
// Every form must fit the account's max payload, and the check has to happen at
// admission: accepting a PUBLISH at PUBREC time that a LATER form then pushes
// past max_payload leaves the client retrying an identical packet forever with
// its hold record never released — worse than the losses these protocols exist
// to fix. Bounds come from named constants shared with the encoders, never from
// live values, because the preflight runs before any generation, claim or stream
// sequence exists.
//
// Extracted from mqttProcessPub so each form can be compared against the others
// directly in a test: several of them do not scale with the payload, so which one
// dominates is header-size dependent, and a form that is only reachable for
// (say) a tiny payload with a short topic is exactly the one an implementation
// forgets to budget.
func mqttMaxQoS2FormSize(pp *mqttPublish, delivered int, retained bool) int {
	// RELEASE form: the delivered form plus the Nats-Msg-Id that makes a repeated
	// onward store non-duplicating. See qos2_move.go.
	total := delivered + mqttQoS2MsgIdHeaderMax
	// HOLD form: the subject-encoded form plus the immutable generation header and
	// the create-only expected-sequence header. The CAS header is NOT stripped
	// before storage, so it counts.
	if hold := mqttComputeNatsMsgSize(pp, true, 0, _EMPTY_, retained, _EMPTY_, _EMPTY_) +
		mqttQoS2GenHeaderMax + mqttQoS2HoldCASHeaderMax; hold > total {
		total = hold
	}
	// TERMINAL form: an empty body, so its size does not depend on pp at all — for
	// a small payload with a short topic it can be the largest. Its failure mode is
	// the worst available: the onward store succeeds, the commit is rejected for
	// size, and every retry repeats the delivery.
	if terminal := mqttQoS2TerminalRecordMax(); terminal > total {
		total = terminal
	}
	// CLAIM form: the release claim written on a sibling of the hold subject before
	// the onward delivery. Payload-independent like the terminal form, so it too can
	// dominate for a small payload with a short topic, and the same eternal wedge if
	// it is not budgeted: the PUBLISH is accepted at PUBREC time and its PUBREL can
	// then never write the claim it must hold before delivering. See
	// qos2_release_claim.go.
	if claim := mqttQoS2ClaimRecordMax(); claim > total {
		total = claim
	}
	// AFFINE form: the copy written when this message is delivered to a shared
	// subscription at effective QoS 2 and moved onto the chosen session's own
	// subject. Same class of eternal wedge: the claim never materializes, the group
	// delivery is never acked, and AckWait retries forever. The subscription
	// identifier and Retain As Published option that size it belong to a SUBSCRIBE
	// that has not happened yet, so both are budgeted at their maximum. See
	// qos2_shared_affine.go.
	if affine := mqttQ2AffineFormMax(pp, retained); affine > total {
		total = affine
	}
	return total
}

// Composes a NATS message from a MQTT PUBLISH packet. The message includes an
// internal header containint the original packet's QoS, and for QoS2 packets
// the original subject.
//
// Example (QoS2, subject: "foo.bar"):
//
//	NATS/1.0\r\n
//	Nmqtt-Pub:2foo.bar\r\n
//	\r\n
func mqttNewDeliverableMessage(pp *mqttPublish, encodePP bool, ttl uint32, origin string, retained bool, msgId, q2Gen string) (natsMsg []byte, headerLen int) {
	size := mqttComputeNatsMsgSize(pp, encodePP, ttl, origin, retained, msgId, q2Gen)

	buf := bytes.NewBuffer(make([]byte, 0, size))

	qos := mqttGetQoS(pp.flags)

	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsHeader)
	buf.WriteByte(':')
	buf.WriteByte(qos + '0')
	buf.WriteString(_CRLF_)

	// Authenticated No Local origin marker. Absent when the publisher has no
	// session or v5 is off. Spec5 [3.8.3.1].
	if len(origin) > 0 {
		buf.WriteString(mqttNatsHeaderOrigin)
		buf.WriteByte(':')
		buf.WriteString(origin)
		buf.WriteString(_CRLF_)
	}

	// Original RETAIN flag, for the Retain As Published subscription option.
	// Spec5 [3.8.3.1].
	if retained {
		buf.WriteString(mqttNatsHeaderRetain)
		buf.WriteByte(':')
		buf.WriteByte('1')
		buf.WriteString(_CRLF_)
	}

	if encodePP {
		buf.WriteString(mqttNatsHeaderSubject)
		buf.WriteByte(':')
		buf.Write(pp.subject)
		buf.WriteString(_CRLF_)

		if len(pp.mapped) > 0 {
			buf.WriteString(mqttNatsHeaderMapped)
			buf.WriteByte(':')
			buf.Write(pp.mapped)
			buf.WriteString(_CRLF_)
		}
	}

	if len(pp.props) > 0 {
		buf.WriteString(mqttNatsHeaderProps)
		buf.WriteByte(':')
		buf.WriteString(base64.StdEncoding.EncodeToString(pp.props))
		buf.WriteString(_CRLF_)
	}

	// MQTT 5.0 Message Expiry Interval: attach a JetStream per-message TTL so the
	// stored copy expires after the requested lifetime. This is server-internal
	// metadata (not one of the forwarded Nmqtt-* headers). Spec5 [3.3.2.3.3].
	if ttl > 0 {
		buf.WriteString(JSMessageTTL)
		buf.WriteByte(':')
		buf.WriteString(strconv.FormatUint(uint64(ttl), 10))
		buf.WriteByte('s')
		buf.WriteString(_CRLF_)
	}

	// Inbound QoS 2 move protocol headers, sized identically above. Nats-Msg-Id
	// on the RELEASE form gives the $MQTT_msgs store a deduplication key that is
	// stable across every PUBREL retry of one hold generation; Nmqtt-Q2Gen on the
	// HOLD form is that generation. This function is the SOLE writer of
	// Nmqtt-Q2Gen onto a held record — storeQoS2HoldCAS adds only the expected-
	// sequence header, and emitting the generation in both places would double it
	// and invalidate every size bound. See qos2_move.go.
	if len(msgId) > 0 {
		buf.WriteString(JSMsgId)
		buf.WriteByte(':')
		buf.WriteString(msgId)
		buf.WriteString(_CRLF_)
	}
	if len(q2Gen) > 0 {
		buf.WriteString(mqttQoS2GenHeader)
		buf.WriteByte(':')
		buf.WriteString(q2Gen)
		buf.WriteString(_CRLF_)
	}

	// End of header
	buf.WriteString(_CRLF_)

	headerLen = buf.Len()

	buf.Write(pp.msg)
	return buf.Bytes(), headerLen
}

// Composes a NATS message for a pending PUBREL packet. The message includes an
// internal header containing the PI for PUBREL/PUBCOMP.
//
// Example (PI:123):
//
//		NATS/1.0\r\n
//	 	Nmqtt-PubRel:123\r\n
//		\r\n
func mqttNewDeliverablePubRel(pi uint16) (natsMsg []byte, headerLen int) {
	size := len(hdrLine) +
		len(mqttNatsPubRelHeader) + 6 + 2 + // 6 for ':65535', and 2 for CRLF
		2 // end-of-header CRLF
	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.WriteString(hdrLine)
	buf.WriteString(mqttNatsPubRelHeader)
	buf.WriteByte(':')
	buf.WriteString(strconv.FormatInt(int64(pi), 10))
	buf.WriteString(_CRLF_)
	buf.WriteString(_CRLF_)
	return buf.Bytes(), buf.Len()
}

// Process the PUBLISH packet.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPub(c *client, pp *mqttPublish, trace bool) error {
	qos := mqttGetQoS(pp.flags)

	// Reject a client PUBLISH into the broker's internal subject namespace
	// ("$MQTT.*"). An MQTT client can map a "$MQTT/..." topic onto e.g.
	// "$MQTT.JSA.<...>.SP.<hash>.<uuid>" and forge session-persist / JSA traffic
	// to evict or corrupt other sessions. The broker's own internal publishes go
	// through the internal client's processInboundClientMsg, never this client
	// PUBLISH path, so this only blocks external clients from the reserved
	// namespace. Defense in depth: production should ALSO have the NATS account
	// used for auth passthrough deny publish to "$MQTT.>", but the broker must
	// not depend on that config being correct. Drop the message (do not deliver
	// or store); ack QoS>0 with Not authorized (0x87) so the client's flow stays
	// consistent, and do not disconnect on QoS0. Spec5 [3.3.5], reason 0x87.
	if bytes.HasPrefix(pp.subject, []byte(mqttPrefix)) {
		c.Warnf("Rejecting PUBLISH to reserved internal subject %q", pp.subject)
		switch qos {
		case 1:
			c.mqttEnqueuePubResponseReason(mqttPacketPubAck, pp.pi, mqttReasonNotAuthorized, "publish to reserved topic not allowed", trace)
		case 2:
			c.mqttEnqueuePubResponseReason(mqttPacketPubRec, pp.pi, mqttReasonNotAuthorized, "publish to reserved topic not allowed", trace)
		}
		return nil
	}

	// MQTT 5.0 flow control [MQTT-4.9]: the server enforces the portion of the
	// client's send quota it can observe — the QoS 2 messages held between PUBREC
	// and PUBREL. Any QoS>0 PUBLISH arriving while those fill the quota is a
	// violation => DISCONNECT 0x93 via the read loop's error exit. Pure QoS 1
	// overrun is not reliably observable (the slot is returned when the client
	// receives the PUBACK, which the server cannot see) and is not policed here.
	// A DUP re-send of an already-held QoS 2 PI is not a new send and must not
	// double-count. Checked before the message is examined so a flow-control
	// violation trumps any per-message reason.
	if qos > 0 && c.mqtt.recvMaxIn > 0 {
		if _, held := c.mqtt.qos2InFlight[pp.pi]; !(qos == 2 && held) &&
			len(c.mqtt.qos2InFlight) >= int(c.mqtt.recvMaxIn) {
			return fmt.Errorf("%w: %d QoS 2 messages already await PUBREL, receive maximum is %d",
				errMQTTReceiveMaxExceeded, len(c.mqtt.qos2InFlight), c.mqtt.recvMaxIn)
		}
	}

	// Authenticated No Local origin marker for this publish, stamped on the
	// delivered/stored message and read back by both delivery callbacks
	// (mqttDeliverMsgCbQoS0, mqttDeliverMsgCbQoS12) to suppress self-delivery.
	// Computed for every QoS (including plain QoS0): unlike upstream, where a
	// QoS0 delivery runs in-process with a genuine *client identifying the
	// publisher (so No Local needs no header marker there), this standalone
	// broker's QoS0 fan-out always travels over a real NATS subscription whose
	// callback never carries real publisher identity (see
	// mqttDeliverMsgCbQoS0's non-MQTT branch) — the marker is the only way QoS0
	// No Local can work here. Empty for 3.1.1 or when v5 is off. Spec5
	// [3.8.3.1].
	origin := c.mqttStampedOrigin(string(pp.subject), pp.msg)

	// Enforce max_payload using existing client max payload logic (mpay) by
	// checking the largest NATS message size that would be processed, including
	// the Nats-TTL header that a Message Expiry Interval adds to the stored
	// message. QoS2 is first held with the PUBLISH subject headers (no TTL) and
	// later delivered with the TTL header (no subject headers); the two header
	// sets never coexist, so we compare the actual forms rather than sum them.
	//
	// For QoS 2 there are FOUR forms, and every one of them must fit; see
	// mqttMaxQoS2FormSize.
	if maxPayload := atomic.LoadInt32(&c.mpay); maxPayload != jwt.NoLimit {
		// The delivered form carries the Nmqtt-Origin header; the QoS2 hold copy
		// does not (origin is re-derived from the client on PUBREL release). Both
		// carry the Nmqtt-Ret marker for a retained publish (the hold copy so it
		// survives to PUBREL).
		retained := c.mqttOriginRetained(pp)
		total := mqttComputeNatsMsgSize(pp, false, c.mqttStoredMsgTTL(pp), origin, retained, _EMPTY_, _EMPTY_)
		if qos == 2 {
			total = mqttMaxQoS2FormSize(pp, total, retained)
		}
		if total > int(maxPayload) {
			// A v5 PUBLISH within the advertised Maximum Packet Size can still
			// exceed the account max payload once re-encoded as a NATS message
			// (header overhead). Rather than close the connection, discard the
			// message and report a non-fatal Quota Exceeded so the advertised
			// packet-size limit stays truthful. QoS0 has no ack, so drop silently.
			// Spec5 [3.4.2.1], [3.5.2.1], reason 0x97.
			if c.mqtt.proto == mqttProtoLevel5 {
				rs := fmt.Sprintf("message size %d exceeds server maximum of %d after encoding", total, maxPayload)
				switch qos {
				case 1:
					c.mqttEnqueuePubResponseReason(mqttPacketPubAck, pp.pi, mqttReasonQuotaExceeded, rs, trace)
				case 2:
					c.mqttEnqueuePubResponseReason(mqttPacketPubRec, pp.pi, mqttReasonQuotaExceeded, rs, trace)
				default:
					c.Debugf("Discarding QoS0 PUBLISH: %s", rs)
				}
				return nil
			}
			c.maxPayloadViolation(total, maxPayload)
			return ErrMaxPayload
		}
	}

	// MQTT 5.0 Payload Format Indicator: when the publisher declares the payload
	// is UTF-8 (PFI=1) but it is not well-formed, this is a Payload Format Invalid
	// error. Like Quota Exceeded above it is non-fatal for QoS>0 (the ack carries
	// reason 0x99 and the message is discarded); a QoS0 violation cannot be acked,
	// so the connection is closed with DISCONNECT 0x99. Spec5 [3.3.2.3.2], reason
	// 0x99. Only v5 carries the property; 3.1.1 has no PFI.
	if c.mqtt.proto == mqttProtoLevel5 && mqttPayloadFormatIsUTF8(pp.props) && !utf8.Valid(pp.msg) {
		rs := "payload is not valid UTF-8 but the Payload Format Indicator is 1"
		switch qos {
		case 1:
			c.mqttEnqueuePubResponseReason(mqttPacketPubAck, pp.pi, mqttReasonPayloadFormatInvalid, rs, trace)
			return nil
		case 2:
			c.mqttEnqueuePubResponseReason(mqttPacketPubRec, pp.pi, mqttReasonPayloadFormatInvalid, rs, trace)
			return nil
		default:
			return fmt.Errorf("%w: %s", errMQTTPayloadFormatInvalid, rs)
		}
	}

	switch qos {
	case 0:
		return s.mqttInitiateMsgDelivery(c, pp, origin)

	case 1:
		// [MQTT-4.3.2-2]. Initiate onward delivery of the Application Message,
		// Send PUBACK.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBACK. When its original sender receives
		// the PUBACK packet, ownership of the Application Message is
		// transferred to the receiver.
		err := s.mqttInitiateMsgDelivery(c, pp, origin)
		if err == nil {
			c.mqttEnqueuePubResponse(mqttPacketPubAck, pp.pi, trace)
		}
		return err

	case 2:
		// [MQTT-4.3.3-2]. Method A, Store message, send PUBREC.
		//
		// The receiver is not required to complete delivery of the Application
		// Message before sending the PUBREC or PUBCOMP. When its original
		// sender receives the PUBREC packet, ownership of the Application
		// Message is transferred to the receiver.
		err := s.mqttStoreQoS2MsgOnce(c, pp)
		if err == nil {
			// A success PUBREC leaves the client's quota slot occupied until the
			// matching PUBREL frees it. A DUP re-store returns nil too, so re-adding
			// the PI is idempotent. Only tracked when a Receive Maximum applies.
			// Spec5 [4.9].
			if c.mqtt.recvMaxIn > 0 {
				if c.mqtt.qos2InFlight == nil {
					c.mqtt.qos2InFlight = make(map[uint16]struct{})
				}
				c.mqtt.qos2InFlight[pp.pi] = struct{}{}
			}
			c.mqttEnqueuePubResponse(mqttPacketPubRec, pp.pi, trace)
		}
		return err

	default:
		return fmt.Errorf("unreachable: invalid QoS in mqttProcessPub: %v", qos)
	}
}

// mqttStoredMsgTTL returns the JetStream per-message TTL to attach to pp's
// stored copy: its MQTT 5.0 Message Expiry Interval. AllowMsgTTL is a startup
// prerequisite of the messages stream (validateMsgsStream), so there is no
// stream that would reject the header.
func (c *client) mqttStoredMsgTTL(pp *mqttPublish) uint32 {
	if mqttTestSuppressMsgTTLV.Load() {
		// Test seam. The Message Expiry Interval drives BOTH the delivery-side
		// expiry check and this stored-copy TTL, so a message that is expired for
		// delivery is normally also being reaped from the stream — the two coexist
		// only during JetStream's reaping lag. Dropping just the TTL keeps the
		// stored copy addressable while its MQTT lifetime elapses, which is what
		// makes the "started deliveries are never dropped on expiry" rule testable
		// without racing a reaper.
		return 0
	}
	return mqttMessageExpiryTTL(pp.props)
}

func (s *Server) mqttInitiateMsgDelivery(c *client, pp *mqttPublish, origin string) error {
	return s.mqttInitiateMsgDeliveryJSA(c, c.mqtt.sess.jsa, pp, origin, _EMPTY_)
}

// mqttInitiateMsgDeliveryDedup is mqttInitiateMsgDelivery for the inbound QoS 2
// PUBREL release: the stored copy carries a Nats-Msg-Id derived from the hold
// record's immutable generation, so a PUBREL retried after an unconfirmed move
// does not write a second copy onto $MQTT_msgs. Best effort by design — the move
// protocol's durable terminal state is what actually prevents re-delivery, and
// the deduplication window only narrows the unresolved interval. See qos2_move.go.
func (s *Server) mqttInitiateMsgDeliveryDedup(c *client, pp *mqttPublish, origin, msgId string) error {
	return s.mqttInitiateMsgDeliveryJSA(c, c.mqtt.sess.jsa, pp, origin, msgId)
}

// mqttInitiateMsgDeliveryJSA is mqttInitiateMsgDelivery with an explicit
// JetStream access handle for the QoS1/2 store step, instead of taking it from
// c.mqtt.sess. This lets a delayed Will (MQTT 5.0 Will Delay Interval,
// [MQTT-3.1.2-8]) be published after its originating client is gone, using the
// account's internal client and JSA.
//
// origin is the authenticated No Local origin marker stamped for the No Local
// option, passed explicitly so it does not depend on which client (real or
// internal) drives delivery: a normal PUBLISH passes c.mqttStampedOrigin(subj),
// while a Will is server-originated and passes _EMPTY_. Spec5 [3.8.3.1].
//
// msgId, when non-empty, is a Nats-Msg-Id for the $MQTT_msgs store so a repeated
// store of the same logical message is deduplicated by JetStream. Only the
// inbound QoS 2 PUBREL release passes one (see mqttInitiateMsgDeliveryDedup); it
// is NOT set on the live fan-out's behalf, which is at-least-once regardless.
func (s *Server) mqttInitiateMsgDeliveryJSA(c *client, jsa *mqttJSA, pp *mqttPublish, origin, msgId string) error {
	// A stored (QoS 1/2) PUBLISH carrying an MQTT 5.0 Message Expiry Interval is
	// given a matching JetStream per-message TTL so it is dropped from the
	// delivery stream once its lifetime elapses. Spec5 [3.3.2.3.3]. Degraded
	// mode drops the header; delivery-side expiry checks still apply.
	//
	// pp.flags still carries the original RETAIN flag here (it is cleared later,
	// during fan-out); record it for the Retain As Published option when v5 is
	// enabled. Spec5 [3.8.3.1].
	retained := c.mqttOriginRetained(pp)
	natsMsg, headerLen := mqttNewDeliverableMessage(pp, false, c.mqttStoredMsgTTL(pp), origin, retained, msgId, _EMPTY_)

	// Set the client's pubarg for processing.
	c.pa.subject = pp.subject
	c.pa.mapped = pp.mapped
	c.pa.reply = nil
	c.pa.hdr = headerLen
	c.pa.hdb = []byte(strconv.FormatInt(int64(c.pa.hdr), 10))
	c.pa.size = len(natsMsg)
	c.pa.szb = []byte(strconv.FormatInt(int64(c.pa.size), 10))
	// Expose the resolved No Local origin so mqttHandlePubRetain (invoked inside
	// processInboundClientMsg for a retained PUBLISH) stamps the stored copy with
	// the same value as the live delivery header. Spec5 [3.8.3.1].
	c.mqtt.pubOrigin = origin
	defer func() {
		c.pa.subject = nil
		c.pa.mapped = nil
		c.pa.reply = nil
		c.pa.hdr = -1
		c.pa.hdb = nil
		c.pa.size = 0
		c.pa.szb = nil
		c.mqtt.pubOrigin = _EMPTY_
	}()

	_, permIssue := c.processInboundClientMsg(natsMsg)
	if permIssue {
		return nil
	}

	// If QoS 0 messages don't need to be stored, other (1 and 2) do. Store them
	// JetStream under "$MQTT.msgs.<delivery-subject>"
	if qos := mqttGetQoS(pp.flags); qos == 0 {
		return nil
	}

	// We need to call flushClients now since this we may have called c.addToPCD
	// with destination clients (possibly a route). Without calling flushClients
	// the following call may then be stuck waiting for a reply that may never
	// come because the destination is not flushed (due to c.out.fsp > 0,
	// see addToPCD and writeLoop for details).
	c.flushClients(0)

	// Wait out any SUBSCRIBE currently being processed in this account before
	// storing: $MQTT_msgs uses interest-based retention, and a message stored
	// before a racing SUBSCRIBE's consumer registers interest would be
	// discarded outright — permanently invisible to the new subscription. See
	// mqttSubsBarrier. The abort channel lets a delayed-Will worker unblock on
	// shutdown so it cannot hang Phase 3 (nil-safe: a nil server means no abort).
	var barrierAbort <-chan struct{}
	if c.srv != nil {
		barrierAbort = c.srv.willClaimStopCh
	}
	jsa.subsBarrier.wait(barrierAbort)

	_, err := jsa.storeMsg(mqttStreamSubjectPrefix+string(c.pa.subject), headerLen, natsMsg)

	return err
}

// mqttStoreQoS2MsgOnce stores an inbound QoS 2 PUBLISH as a HELD record on
// $MQTT_qos2in, so it is not broadcast until the sender releases it with PUBREL.
// Returning nil means "the client may be sent PUBREC": either this payload is now
// durably held, or an identical held record for this packet identifier already is
// (a DUP PUBLISH).
//
// This is a bounded create-and-reconcile loop, mirroring enqueueDelayedWill. The
// create is a create-only CAS (expected last subject sequence 0), so a rejection
// means "the subject is occupied" or "transient contention" — never "the write
// landed". Only a re-read can tell those apart, and the re-read's classification
// then decides:
//
//   - absent: the record raced away between the CAS and the read; retry the create.
//   - corrupt: unusable, and checked BEFORE held — see below. Reap it and retry.
//   - delivered: a tombstone from a completed earlier exchange on this identifier
//     that terminal cleanup has not reaped yet. Reap it and retry; misreading it as
//     a duplicate would silently lose THIS payload.
//   - held: a genuine DUP PUBLISH; answer PUBREC without storing again.
//
// The held case is safe to treat as a duplicate because no path lets a client
// believe an exchange finished while a held record survives (the corrupt, malformed
// and expired branches all confirm their cleanup before PUBCOMP — invariant 3) and
// clear() purges hold state on session discard. So a held record on this subject
// always belongs to the exchange the client is currently retrying.
//
// Reaping is what must NOT be assumed: an unconfirmed reap leaves a nonterminal or
// unidentifiable record in place, so it withholds PUBREC entirely (invariant 3, one
// packet earlier) rather than proceeding.
func (s *Server) mqttStoreQoS2MsgOnce(c *client, pp *mqttPublish) error {
	asm := c.mqtt.asm
	subject := c.mqttQoS2InternalSubject(pp.pi)

	// The record's immutable identity for the whole exchange: it gates the terminal
	// CAS (invariant 1) and becomes the onward store's Nats-Msg-Id. Generated once
	// per PUBLISH, never rewritten.
	gen := nuid.Next()

	// `true` means encode the MQTT PUBLISH packet in the NATS message header. No
	// TTL here: this is the QoS2 dedup hold stream (no AllowMsgTTL); the expiry
	// TTL is applied when the message is moved to the delivery stream on PUBREL.
	// The original RETAIN flag is recorded so Retain As Published survives the
	// hold and is restored on PUBREL. Spec5 [3.8.3.1].
	//
	// mqttNewDeliverableMessage is the sole writer of Nmqtt-Q2Gen onto the held
	// record; storeQoS2HoldCAS adds only the expected-sequence header. Emitting the
	// generation in both places would double the header and invalidate the
	// max_payload bounds computed in mqttProcessPub.
	natsMsg, headerLen := mqttNewDeliverableMessage(pp, true, 0, _EMPTY_, c.mqttOriginRetained(pp), _EMPTY_, gen)

	var lastErr error
	for attempt := 0; attempt < mqttQoS2StoreAttempts; attempt++ {
		_, err := asm.storeQoS2HoldCAS(subject, 0, headerLen, natsMsg)
		if err == nil {
			return nil // freshly held; PUBREC
		}
		lastErr = err
		if !isQoS2CASRejection(err) {
			// A real store failure, not a contested subject. No PUBREC; the client
			// retries the PUBLISH.
			return err
		}
		rec, seq, lerr := asm.loadQoS2Record(subject)
		if lerr != nil {
			// Indeterminate: never guessed into an absence, because retrying the
			// create-only CAS against a subject that actually holds a record would
			// just fail again, and treating it as a duplicate would answer PUBREC for
			// a payload we did not store.
			return lerr
		}
		if rec == nil {
			continue // raced away; retry the create
		}
		switch rec.state {
		case mqttQoS2Corrupt:
			// Checked before held, and it must never fall through to held: a record we
			// cannot identify is not this PUBLISH's payload, and answering PUBREC for
			// it means the following PUBREL releases the OLD record and reports 0x92
			// while this payload is lost.
			c.Errorf("Reaping unusable QoS2 hold record (%s)",
				mqttQoS2CorruptDetail(subject, rec.stored, "no valid generation, or a malformed done marker"))
			if st, rerr := asm.releaseQoS2Record(seq); st != mqttQoS2Released {
				return rerr
			}
		case mqttQoS2Done:
			// A leftover tombstone whose asynchronous cleanup has not landed yet
			// (invariant 4 permits that). Reap it so this identifier is usable again.
			if st, rerr := asm.releaseQoS2Record(seq); st != mqttQoS2Released {
				return rerr
			}
		default: // mqttQoS2Held
			return nil // genuine DUP PUBLISH; PUBREC
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("QoS2 hold not confirmed after %d attempts", mqttQoS2StoreAttempts)
	}
	return lastErr
}

// mqttQoS2InternalSubject is the $MQTT_qos2in hold subject for one of this
// client's inbound packet identifiers.
//
// The client ID is hashed into a NATS-safe token (as sessions do): the raw client
// ID may contain characters invalid in a NATS subject now that arbitrary UTF-8
// client IDs are accepted. mqtt-nats-bridge divergence from upstream.
//
// The session's incarnation generation is part of the subject, so a hold belongs
// to one incarnation and a discard purge from a retired one can never reach it.
// Both callers run on the inbound QoS 2 path, well after processConnect installed
// c.mqtt.sess (which the delivery path already dereferences unguarded), and a
// re-mint only ever happens on a discard, which ends the connection.
func (c *client) mqttQoS2InternalSubject(pi uint16) string {
	sess := c.mqtt.sess
	sess.mu.Lock()
	idHash, gen := sess.idHash, sess.gen
	sess.mu.Unlock()
	return mqttQoS2HoldSubject(idHash, gen, pi)
}

// Process a PUBREL packet (QoS2, acting as Receiver).
//
// This is the recoverable MOVE half of the inbound QoS 2 exchange: the held record
// is delivered onward FIRST and only then replaced by a terminal tombstone, so no
// window can delete the message and then fail to deliver it. The branch order is
// load, corrupt, delivered, malformed, expired, claim, prove, store, commit; see
// qos2_move.go for the state machine, the four design invariants it obeys, and the
// honest guarantee (no silent loss; exactly-once for durable subscribers only while
// deduplication survives the unresolved store..commit interval).
//
// The claim (R) and the proof (P) are the two fences that serialize this release
// against a concurrent session discard, and neither substitutes for the other: the
// claim fences a DISCARD, which a re-read cannot because read-then-fan-out is
// check-then-act, and the proof fences an ALREADY-COMPLETED exchange, which the
// claim cannot because a completed exchange leaves nothing to contend with. Both
// must precede the onward store, whose fan-out cannot be taken back. See
// qos2_release_claim.go.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (s *Server) mqttProcessPubRel(c *client, pi uint16, trace bool) error {
	// A PUBCOMP (with any reason, including 0x92) ends the exchange and returns
	// the client's send-quota slot, so the two travel together: sendPubComp
	// gates both. It stays true for every outcome that answers the client —
	// success, an unknown PI, a released corrupt or malformed record, an
	// expiry-dropped hold, a release the discard won, and a superseded finalize — and
	// is cleared ONLY where a broker-side JetStream failure means the exchange is
	// unfinished (an indeterminate lookup, an unresolved claim or proof, a failed
	// onward store, an unconfirmed terminal commit, or an unconfirmed nonterminal
	// release), because MQTT 5
	// gives PUBCOMP no reason code for "the broker failed" (only 0x00 and 0x92
	// are defined) and answering anyway would tell the client an exactly-once
	// exchange completed for a message still held. Spec5 [4.9], [MQTT-4.3.3-1].
	//
	// Freeing the identifier is exactly what makes unconfirmed cleanup unsafe, so
	// the corrupt, malformed and expired branches below clear sendPubComp when their
	// delete cannot be confirmed (invariant 3), while the terminal tombstone's
	// delete is allowed to be asynchronous (invariant 4).
	sendPubComp := true
	reason := mqttReasonSuccess
	var reasonStr string
	defer func() {
		if !sendPubComp {
			return
		}
		delete(c.mqtt.qos2InFlight, pi)
		c.mqttEnqueuePubResponseReason(mqttPacketPubComp, pi, reason, reasonStr, trace)
	}()

	subject := c.mqttQoS2InternalSubject(pi)
	asm := c.mqtt.asm

	// releaseNonterminal disposes of a record that is NOT a terminal tombstone
	// (corrupt, malformed, or expiry-dropped) and reports whether the client may be
	// answered. An unconfirmed delete must not be reported as done: PUBCOMP frees
	// the packet identifier, and the client's next PUBLISH on it would then be
	// matched against the surviving record and silently lost. Invariant 3.
	releaseNonterminal := func(seq uint64, what string) error {
		st, rerr := asm.releaseQoS2Record(seq)
		if st == mqttQoS2Released {
			return nil
		}
		sendPubComp = false
		c.Errorf("Unable to release %s QoS2 hold record for packet identifier %v: %v", what, pi, rerr)
		return rerr
	}

	// L: load and classify the current record on this identifier's hold subject.
	rec, seq, err := asm.loadQoS2Record(subject)
	switch {
	case err != nil:
		// A broker-side lookup failure, NOT a missing PI. Nothing has been
		// delivered or removed yet, so the record is still there: suppress the
		// PUBCOMP and close the connection so the client retries PUBREL with its
		// session state intact.
		sendPubComp = false
		c.Errorf("Unable to look up QoS2 hold entry for packet identifier %v: %v", pi, err)
		return err
	case rec == nil:
		// No record found: no session state for this PI, which a v5 PUBCOMP
		// reports as 0x92. This includes an exchange that DID complete whose
		// tombstone has since been reaped — absence is indistinguishable from
		// never-existed, and Spec5 sanctions 0x92 during recovery. Spec5
		// [MQTT-4.3.3-1].
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	}

	// K: the record carries no usable identity (missing/duplicated/malformed
	// generation, or a done marker we cannot attribute). It can be neither released
	// as ours nor delivered as a message, so dispose of it and admit the identifier's
	// state is gone. Metadata only in the log line: the body is an application
	// payload and can be as large as max_payload.
	if rec.state == mqttQoS2Corrupt {
		c.Errorf("Discarding unusable QoS2 hold record (%s)",
			mqttQoS2CorruptDetail(subject, rec.stored, "no valid generation, or a malformed done marker"))
		if rerr := releaseNonterminal(seq, "unusable"); rerr != nil {
			return rerr
		}
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	}

	// T: the terminal tombstone. This exchange already delivered onward and
	// committed, so answer success WITHOUT re-delivering — indefinitely, and
	// independently of any deduplication window. This branch is what the whole
	// protocol buys. Reap the tombstone asynchronously (invariant 4): a surviving
	// tombstone is inert, since this branch answers from it and a later PUBLISH on
	// this identifier reaps it.
	if rec.state == mqttQoS2Done {
		_ = asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, seq, false)
		return nil
	}

	stored := rec.stored
	gen := rec.gen

	// V: only MQTT QoS2 messages should be here, and they must have a subject. The
	// generation IS valid here (K already handled the case where it is not), so this
	// is a record we can prove is ours but cannot deliver. Release it before
	// answering, for the same reason as K. Metadata only, again.
	h := mqttParsePublishNATSHeader(stored.Header)
	if h == nil || h.qos != 2 || len(h.subject) == 0 {
		c.Errorf("Discarding QoS2 hold record with malformed publish headers (%s)",
			mqttQoS2CorruptDetail(subject, stored, "missing or invalid Nmqtt-Pub/Nmqtt-Subject"))
		if rerr := releaseNonterminal(seq, "malformed"); rerr != nil {
			return rerr
		}
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	}

	pp := &mqttPublish{
		topic:   natsSubjectToMQTTTopic(h.subject),
		subject: h.subject,
		mapped:  h.mapped,
		msg:     stored.Data,
		sz:      len(stored.Data),
		pi:      pi,
		flags:   h.qos << 1,
		props:   h.props, // forward v5 properties captured at store time
	}
	// Restore the original RETAIN flag recorded at store time so Retain As
	// Published works for a QoS2 retained publish released on PUBREL. Spec5
	// [3.8.3.1].
	if h.retained {
		pp.flags |= mqttPubFlagRetain
	}

	// X: MQTT 5.0 Message Expiry Interval. A QoS2 message ages while it waits in the
	// server for the PUBREL. Measure the interval from the original PUBLISH receipt
	// (the hold-stream store time) rather than restarting it here: age the carried
	// interval by the time already elapsed, and drop the message if it has expired
	// before release. The drop is this path's own responsibility — release the record
	// before answering, again per invariant 3. Spec5 [3.3.2.3.3].
	var expired bool
	if pp.props, expired = mqttForwardExpiry(pp.props, stored.Time.UnixNano()); expired {
		if rerr := releaseNonterminal(seq, "expired"); rerr != nil {
			return rerr
		}
		return nil
	}

	// R: the release claim — fence 1, and the last point at which a concurrent
	// session discard can still win. It is a create-only CAS on a sibling of the hold
	// subject, so exactly one of "release it" and "discard it" can ever succeed and
	// the loser does nothing irreversible. It must precede everything below: the
	// fan-out inside the onward store cannot be taken back. See qos2_release_claim.go.
	if sess := c.mqtt.sess; sess != nil {
		if !sess.beginQoS2ReleaseOp() {
			// This session is being discarded on this broker. Deliver nothing and claim
			// nothing: the discard is about to destroy this hold, and the client's
			// session state with it. Answering 0x92 is the same answer the durable claim
			// would have produced had the discard been on another broker.
			reason = mqttReasonPacketIDNotFound
			reasonStr = "no message found for the packet identifier"
			return nil
		}
		defer sess.endQoS2ReleaseOp()
	}

	claimStatus, claimSeq, cerr := asm.claimQoS2Release(s, subject, gen, seq)
	switch claimStatus {
	case mqttQoS2ClaimClaimed, mqttQoS2ClaimAlreadyClaimed:
		// Ours. AlreadyClaimed is not an error and not a conflict: the generation
		// identifies the EXCHANGE, not the broker, so it means our own earlier attempt
		// or a peer releasing the same exchange. The claim is discard-exclusive, never
		// releaser-exclusive.
	case mqttQoS2ClaimDiscarded, mqttQoS2ClaimUnreadable:
		// A discard won, or a record we cannot attribute holds the claim subject.
		// Nothing may be delivered. Release the hold record before answering
		// (invariant 3) and report the identifier as gone. The CLAIM is deliberately
		// not reaped: a `discarded` claim is the only durable proof that a later
		// releaser must lose, and an unreadable one may be a corrupted instance of one.
		if rerr := releaseNonterminal(seq, "discarded"); rerr != nil {
			return rerr
		}
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	default: // mqttQoS2ClaimIndeterminate
		// We cannot prove we hold the claim, so we must not deliver, and the exchange
		// is unfinished: suppress the PUBCOMP and let the client retry.
		sendPubComp = false
		c.Errorf("Unable to claim the QoS2 release for packet identifier %v: %v", pi, cerr)
		return cerr
	}

	// P: prove the exchange is still the one we loaded — fence 2. The claim cannot
	// catch a peer that completed this exchange and reaped both records while we were
	// between the load and the claim: our create-only claim then SUCCEEDS against an
	// empty subject and we would fan out a second time from the payload we still hold
	// in memory. Every accepting outcome is generation-qualified.
	vStatus, vSeq, verr := asm.verifyQoS2HoldForRelease(subject, gen, seq)
	switch vStatus {
	case mqttQoS2VerifyHeld:
		// Our record, at our sequence. Nothing can remove it now: a discard must win
		// the claim before it purges, and we hold the claim.
	case mqttQoS2VerifyDelivered:
		// Our generation's own tombstone: a peer completed this exchange. Answer from
		// it without re-delivering, exactly as branch T does, and reap both records
		// asynchronously (invariant 4).
		asm.reapQoS2RelClaim(claimSeq)
		_ = asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, vSeq, false)
		return nil
	case mqttQoS2VerifyGone:
		// Terminated and already reaped. Absence means terminated rather than
		// discarded precisely because a discard claims before it purges, and we hold
		// the claim.
		//
		// One further case reaches here and answers 0x00 for an exchange that was in
		// fact DISCARDED: the orphan sweep retires an aged `discarded` marker, but only
		// after destroying the hold and confirming it gone, so a releaser that preloaded
		// the payload and resumed later finds an empty claim subject and then an absent
		// record. Nothing is delivered and nothing is lost — only the reason code is
		// more optimistic than the marker-backed 0x92. See orphan_qos2_sweep.go and the
		// residual in docs/CONFORMANCE.md.
		asm.reapQoS2RelClaim(claimSeq)
		return nil
	case mqttQoS2VerifySuperseded:
		// A foreign generation holds the subject and was NOT touched. Our exchange's
		// state is no longer ours to close, so 0x92 is the honest answer; our own claim
		// is stale and can go.
		asm.reapQoS2RelClaim(claimSeq)
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	default: // mqttQoS2VerifyIndeterminate
		sendPubComp = false
		c.Errorf("Unable to verify the QoS2 hold record for packet identifier %v before release: %v", pi, verr)
		return verr
	}

	// S: the onward store, carrying a Nats-Msg-Id derived from the immutable
	// generation. A failure here means the exchange is unfinished and the record is
	// still held: suppress the PUBCOMP so the client retries. (Before this fix the
	// failure was returned with sendPubComp still true, telling the client an
	// exactly-once exchange had completed over a message that existed nowhere.)
	//
	// Note that the live core-NATS fan-out happens INSIDE this call, before the
	// stream store. A retry therefore repeats the fan-out: QoS 0 subscribers, plain
	// NATS subscribers and the retained store may see the message twice. That
	// duplicate is irreducible (see residual 1 in qos2_move.go); the msgId is what
	// keeps the stored copy single for stream-backed subscribers.
	if err := s.mqttInitiateMsgDeliveryDedup(c, pp,
		c.mqttStampedOrigin(string(pp.subject), pp.msg), mqttQoS2ReleaseMsgId(gen)); err != nil {
		sendPubComp = false
		c.Errorf("Unable to deliver released QoS2 message for packet identifier %v: %v", pi, err)
		return err
	}

	// C: commit the terminal state. Only now is it safe to leave `held`, and only
	// against our own generation (invariant 1).
	status, doneSeq, ferr := asm.finalizeQoS2Move(s, subject, gen, seq)
	switch status {
	case mqttQoS2FinalizeCommitted:
		// Terminal state is durable, so the tombstone's own removal is inert
		// (invariant 4): push it without waiting. The claim goes the same way, and only
		// now — reaping it before the commit would let a peer stalled since before our
		// claim win a fresh one (fence 2 would still stop it delivering, but the claim
		// must not be the thing that stops working).
		asm.reapQoS2RelClaim(claimSeq)
		_ = asm.jsa.deleteMsg(mqttQoS2IncomingMsgsStreamName, doneSeq, false)
		return nil
	case mqttQoS2FinalizeGone:
		// Our generation is already absent and the onward store succeeded, so the
		// exchange is complete. Only our claim may be left.
		asm.reapQoS2RelClaim(claimSeq)
		return nil
	case mqttQoS2FinalizeSuperseded:
		// A newer generation owns this subject and was NOT touched. Our message was
		// delivered, but this identifier's state is no longer ours to close, so 0x92
		// is the honest answer (residual 5). Our own claim is stale and can go.
		asm.reapQoS2RelClaim(claimSeq)
		reason = mqttReasonPacketIDNotFound
		reasonStr = "no message found for the packet identifier"
		return nil
	default: // mqttQoS2FinalizeIndeterminate
		// Delivery happened but the terminal state is unconfirmed. Suppress the
		// PUBCOMP: the client retries, and the retry either finds the tombstone (T,
		// answered 0x00 with no re-delivery) or re-delivers with the same msgId. The
		// claim STAYS: the retry adopts it, and while it stands a discard still loses.
		sendPubComp = false
		c.Errorf("Unable to commit QoS2 move for packet identifier %v: %v", pi, ferr)
		return ferr
	}
}

// Invoked when processing an inbound client message. If the "retain" flag is
// set, the message is stored so it can be later resent to (re)starting
// subscriptions that match the subject.
//
// Invoked from the MQTT publisher's readLoop. No client lock is held on entry.
func (c *client) mqttHandlePubRetain() {
	pp := c.mqtt.pp
	retainMQTT := mqttIsRetained(pp.flags)
	isBirth, _, isCertificate := sparkbParseBirthDeathTopic(pp.topic)
	retainSparkbBirth := isBirth && !isCertificate

	// [tck-id-topics-nbirth-mqtt] NBIRTH messages MUST be published with MQTT
	// QoS equal to 0 and retain equal to false.
	//
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware MQTT
	// Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with the
	// MQTT retain flag set to true.
	if retainMQTT == retainSparkbBirth {
		// (retainSparkbBirth && retainMQTT) : not valid, so ignore altogether.
		// (!retainSparkbBirth && !retainMQTT) : nothing to do.
		return
	}

	asm := c.mqtt.asm
	key := string(pp.subject)

	// Always clear the retain flag to deliver a normal published message.
	defer func() {
		pp.flags &= ^mqttPubFlagRetain
	}()

	// Spec [MQTT-3.3.1-11]. Payload of size 0 removes the retained message, but
	// should still be delivered as a normal message.
	//
	// A deletion is stored, not signalled: we write the message with an empty
	// body and every broker's retained-stream consumer removes the subject when
	// it processes that record. The STREAM is what orders adds and removes
	// durably — the empty-body tombstone is the only deletion authority, and
	// MaxMsgsPer=1 evicts the record it replaces at store time, so no explicit
	// delete request is involved. The volatile INDEX is a different matter: its
	// writers (the consumer callback, the local seed below, and the
	// decode-failure eviction in processRetainedMsg) apply what they observe
	// without comparing sequences, so index state can go stale under reordering.
	// That is finding F-059, not something this path arbitrates.

	rm := &mqttRetainedMsg{
		Origin: asm.jsa.id,
		Msg:    pp.msg, // will copy these bytes later as we process rm.
		Flags:  pp.flags,
		Source: c.opts.Username,
		Props:  pp.props, // v5 properties to replay to subscribers
		// Same No Local origin the live delivery used (empty for a Will), so
		// retained replay is suppressed under No Local consistently. Spec5 [3.8.3.1].
		SessionHash: c.mqtt.pubOrigin,
	}

	// MQTT 5.0 Message Expiry Interval on a retained message: record the absolute
	// deadline so the remaining interval can be recomputed on replay and the
	// stored message dropped once it expires. A present interval of 0 means the
	// message expires immediately, so the deadline is "now" (distinct from an
	// absent interval, which never expires and leaves the deadline zero). Spec5
	// [3.3.2.3.3].
	if me, ok := mqttMessageExpiry(pp.props); ok {
		rm.expires = time.Now().Add(time.Duration(me) * time.Second)
	}

	if retainSparkbBirth {
		// [tck-id-conformance-mqtt-aware-store] A Sparkplug Aware MQTT Server
		// MUST store NBIRTH and DBIRTH messages as they pass through the MQTT
		// Server.
		//
		// [tck-id-conformance-mqtt-aware-nbirth-mqtt-topic]. A Sparkplug Aware
		// MQTT Server MUST make NBIRTH messages available on a topic of the
		// form: $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id
		//
		// [tck-id-conformance-mqtt-aware-dbirth-mqtt-topic] A Sparkplug Aware
		// MQTT Server MUST make DBIRTH messages available on a topic of the
		// form:
		// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
		topic := append(sparkbCertificatesTopicPrefix, pp.topic...)
		subject, _ := mqttTopicToNATSPubSubject(topic)
		rm.Topic = string(topic)
		rm.Subject = string(subject)

		// will use to save the retained message.
		key = string(subject)

		// Store the retained message with the RETAIN flag set.
		rm.Flags |= mqttPubFlagRetain

		if pp.sz > 0 {
			// Copy the payload out of pp since we will be sending the message
			// asynchronously.
			msg := make([]byte, pp.sz)
			copy(msg, pp.msg[:pp.sz])
			asm.jsa.sendMsg(key, msg)
		}

	} else { // isRetained
		// Spec [MQTT-3.3.1-5]. Store the retained message with its QoS.
		//
		// When coming from a publish protocol, `pp` is referencing a stack
		// variable that itself possibly references the read buffer.
		rm.Topic = string(pp.topic)
	}

	// Set the key to the subject of the message for retained, or the composed
	// $sparkplug subject for sparkB.
	rm.Subject = key
	rmBytes, hdr := mqttEncodeRetainedMessage(rm) // will copy the payload bytes
	resp, err := asm.jsa.storeMsg(mqttRetainedMsgsStreamSubject+key, hdr, rmBytes)
	if err != nil {
		c.mu.Lock()
		acc := c.acc
		c.mu.Unlock()
		c.Errorf("unable to store retained message for account %q, subject %q: %v",
			acc.GetName(), key, err)
		return
	}

	// Seed the local retained index synchronously with the just-stored message.
	// The index (as.retmsgs / rmsCache) is otherwise only populated by
	// processRetainedMsg reacting to the retained-messages stream — a real NATS
	// round trip in this standalone broker (unlike upstream's in-process hop),
	// so a SUBSCRIBE arriving right after this PUBLISH could miss the retained
	// message. Seeding here makes retained-then-subscribe deterministic on the
	// publishing node. The later processRetainedMsg callback applies the same
	// add/remove again — identically when nothing raced it, but neither writer
	// compares sequences, so a callback delayed past a newer publish re-applies
	// an older generation (F-059). An empty body is a tombstone [MQTT-3.3.1-11].
	if len(rm.Msg) == 0 {
		asm.removeRetainedMsg(key, 0)
	} else {
		asm.addRetainedMsg(key, resp.Sequence, rm)
	}
}

// After a config reload, it is possible that the source of a publish retained
// message is no longer allowed to publish on the given topic. If that is the
// case, the retained message is removed from the map and will no longer be
// sent to (re)starting subscriptions.
//
// Server lock MUST NOT be held on entry.
func (s *Server) mqttCheckPubRetainedPerms() {
	sm := &s.mqtt.sessmgr
	sm.mu.RLock()
	done := len(sm.sessions) == 0
	sm.mu.RUnlock()

	if done {
		return
	}

	s.mu.Lock()
	users := make(map[string]*User, len(s.users))
	for un, u := range s.users {
		users[un] = u
	}
	s.mu.Unlock()

	// First get a list of all of the sessions.
	sm.mu.RLock()
	asms := make([]*mqttAccountSessionManager, 0, len(sm.sessions))
	for _, asm := range sm.sessions {
		asms = append(asms, asm)
	}
	sm.mu.RUnlock()

	// For each session we will obtain a list of retained messages.
	var _rms [128]uint64
	rms := _rms[:0]
	for _, asm := range asms {
		// Get all of the retained messages. Then we will sort them so
		// that they are in sequence order, which should help the file
		// store to not have to load out-of-order blocks so often.
		asm.mu.RLock()
		rms = rms[:0] // reuse slice
		// Copy the sequence out of the tree. The tree entry itself can be
		// updated concurrently by addRetainedMsg() after we release the lock,
		// so keeping a pointer here would race with the later sort.
		asm.retmsgs.IterOrdered(func(_ []byte, rm *mqttRetainedMsgRef) bool {
			rms = append(rms, rm.sseq)
			return true
		})
		jsaID := asm.jsa.id
		asm.mu.RUnlock()
		slices.Sort(rms)

		perms := map[string]*mqttPerm{}
		for _, rf := range rms {
			jsm, err := asm.jsa.loadMsg(mqttRetainedMsgsStreamName, rf)
			if err != nil || jsm == nil {
				continue
			}
			rm, err := mqttDecodeRetainedMessage(jsm.Subject, jsm.Header, jsm.Data)
			if err != nil {
				continue
			}
			// We deal only with messages that have a source (the username that produced
			// this message) and were produced on this server.
			if rm.Source == _EMPTY_ || rm.Origin != jsaID {
				continue
			}
			// Lookup source from global users.
			u := users[rm.Source]
			if u != nil {
				p, ok := perms[rm.Source]
				if !ok {
					p = generatePubPerms(u.Permissions)
					perms[rm.Source] = p // possibly nil
				}
				// If there is permission and no longer allowed to publish in
				// the subject, remove the publish retained message from the map.
				if p != nil && !pubAllowed(p, rm.Subject) {
					u = nil
				}
			}

			// Not present or permissions have changed such that the source can't
			// publish on that subject anymore: delete this retained message.
			if u == nil {
				// Set the payload to empty to notify that we are deleting this
				// retained message. We will send this message async.
				rm.Msg = nil
				rmBytes, hdrLen := mqttEncodeRetainedMessage(rm)
				asm.jsa.storeMsgNoWait(mqttRetainedMsgsStreamSubject+rm.Subject, hdrLen, rmBytes)
			}
		}
	}
}

// Helper to generate only pub permissions from a Permissions object
type mqttPerm struct {
	allow *gsl.SimpleSublist
	deny  *gsl.SimpleSublist
}

func generatePubPerms(perms *Permissions) *mqttPerm {
	// If given permissions is `nil`, then it means that permissions block
	// has been removed (so the user is now allowed to publish on everything)
	// or was never there in the first place. Returning `nil` will let the
	// caller know that there are no permissions to enforce.
	if perms == nil {
		return nil
	}
	var p *mqttPerm
	if perms.Publish.Allow != nil {
		p = &mqttPerm{}
		p.allow = gsl.NewSimpleSublist()
		for _, pubSubject := range perms.Publish.Allow {
			_ = p.allow.Insert(pubSubject, struct{}{})
		}
	}
	if len(perms.Publish.Deny) > 0 {
		if p == nil {
			p = &mqttPerm{}
		}
		p.deny = gsl.NewSimpleSublist()
		for _, pubSubject := range perms.Publish.Deny {
			_ = p.deny.Insert(pubSubject, struct{}{})
		}
	}
	return p
}

// Helper that checks if given `perms` allow to publish on the given `subject`
func pubAllowed(perms *mqttPerm, subject string) bool {
	if perms == nil {
		return true
	}
	allowed := true
	if perms.allow != nil {
		allowed = perms.allow.HasInterest(subject)
	}
	// If we have a deny list and are currently allowed, check that as well.
	if allowed && perms.deny != nil {
		allowed = !perms.deny.HasInterest(subject)
	}
	return allowed
}

func (c *client) mqttEnqueuePubResponse(packetType byte, pi uint16, trace bool) {
	c.mqttEnqueuePubResponseReason(packetType, pi, mqttReasonSuccess, _EMPTY_, trace)
}

func (c *client) mqttEnqueuePubResponseReason(packetType byte, pi uint16, reason byte, reasonStr string, trace bool) {
	proto := [5]byte{packetType, 0x2, 0, 0, 0}
	proto[2] = byte(pi >> 8)
	proto[3] = byte(pi)
	n := 4
	// A non-success reason needs the longer v5 form: PI + reason code,
	// properties omitted. Spec5 [3.4.2.1]. 3.1.1 has no reason codes.
	if reason != mqttReasonSuccess && c.mqtt.proto == mqttProtoLevel5 {
		proto[1] = 0x3
		proto[4] = reason
		n = 5
	}

	// Bits 3,2,1 and 0 of the fixed header in the PUBREL Control Packet are
	// reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat
	// any other value as malformed and close the Network Connection [MQTT-3.6.1-1].
	if packetType == mqttPacketPubRel {
		proto[0] |= 0x2
	}

	// A failure reason may carry a Reason String when the client permits it and
	// it fits; otherwise fall back to the fixed short form. Spec5 [3.4.2.2].
	buf := proto[:n]
	if n == 5 {
		if rs := c.mqttReasonString(packetType, reasonStr); rs != _EMPTY_ {
			w := newMQTTWriter(0)
			w.WriteByte(proto[0])
			var vh mqttWriter
			vh.WriteUint16(pi)
			vh.WriteByte(reason)
			vh.writeProperties(&mqttProperties{reasonString: rs})
			w.WriteVarInt(vh.Len())
			w.Write(vh.Bytes())
			if c.mqttFitsClientMax(w.Len()) {
				buf = w.Bytes()
			}
		}
	}

	// Unreachable in practice: a connected client's Maximum Packet Size is bounded
	// below by its success CONNACK (~15+ bytes) so a 4/5-byte response always fits.
	// The check keeps every enqueue site provably compliant. [MQTT-3.1.2-24].
	if !c.mqttEnqueueFittingProto(buf) {
		return
	}

	if trace {
		name := "(???)"
		switch packetType {
		case mqttPacketPubAck:
			name = "PUBACK"
		case mqttPacketPubRec:
			name = "PUBREC"
		case mqttPacketPubRel:
			name = "PUBREL"
		case mqttPacketPubComp:
			name = "PUBCOMP"
		}
		if n == 5 {
			c.traceOutOp(name, []byte(fmt.Sprintf("pi=%v rc=%v", pi, reason)))
		} else {
			c.traceOutOp(name, []byte(fmt.Sprintf("pi=%v", pi)))
		}
	}
}

func mqttParsePIPacket(r *mqttReader) (uint16, error) {
	pi, err := r.readUint16("packet identifier")
	if err != nil {
		return 0, err
	}
	if pi == 0 {
		return 0, errMQTTPacketIdentifierIsZero
	}
	return pi, nil
}

// mqttParsePubResponsePacket parses an inbound PUBACK/PUBREC/PUBREL/PUBCOMP. In
// MQTT 5.0 these packets may carry a reason code and a properties block after
// the packet identifier. The reason code is returned so the caller can honor
// failure codes (e.g. a PUBREC >= 0x80 must not be answered with a PUBREL,
// Spec5 [4.3.3]); an absent reason code (short form, or 3.1.1) is Success
// (0x00). It consumes exactly pl bytes so the reader is positioned at the start
// of the next packet. Spec5 [3.4.2.1] and following.
func mqttParsePubResponsePacket(r *mqttReader, proto byte, pl int) (uint16, byte, error) {
	end := r.pos + pl
	pi, err := mqttParsePIPacket(r)
	if err != nil {
		return 0, 0, err
	}
	reason := mqttReasonSuccess
	if proto == mqttProtoLevel5 {
		// Optional reason code (1 byte).
		if r.pos < end {
			if reason, err = r.readByte("reason code"); err != nil {
				return 0, 0, err
			}
		}
		// Optional properties. The allowed property set (Reason String, User
		// Property) is identical across the four pub-response packets, so the
		// PUBACK context is sufficient for validation.
		if r.pos < end {
			if _, err = r.readProperties(mqttPacketPubAck); err != nil {
				return 0, 0, err
			}
		}
	}
	if r.pos != end {
		return 0, 0, fmt.Errorf("%w: invalid remaining length %d for pub-response packet", errMQTTMalformedPacket, pl)
	}
	return pi, reason, nil
}

// Process a PUBACK (QoS1) or a PUBREC (QoS2) packet, acting as Sender. Set
// isPubRec to false to process as a PUBACK. reason is the MQTT 5.0 reason code
// (Success for 3.1.1 or the short form). A PUBREC carrying a failure code
// (>= 0x80) means the receiver rejected the QoS2 message: per Spec5 [4.3.3] the
// sender MUST NOT send a PUBREL and treats the message as unacknowledged/done,
// so we release the packet identifier and stop redelivery without moving it
// into the PUBREL (awaiting-PUBCOMP) state.
//
// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttProcessPublishReceived(pi uint16, isPubRec bool, reason byte) (err error) {
	sess := c.mqtt.sess
	if sess == nil {
		return errMQTTInvalidSession
	}

	pubRecFailed := isPubRec && reason >= 0x80

	var jsAckSubject, q2AffClaim string
	var removed *mqttPending
	// Set when this PUBREC stored a PUBREL for a DRAINED source, whose claim only a
	// confirmed PUBREL may release.
	var drainedPubRel bool
	sess.mu.Lock()
	// Must be the same client, and the session must have been setup for QoS2.
	if sess.c != c {
		sess.mu.Unlock()
		return errMQTTInvalidSession
	}
	if ack := sess.pendingPublish[pi]; ack != nil && ack.regForeign {
		// A FOREIGN reservation: the register behind this identifier is another
		// session object's claimed identity (a lost shared-claim arbitration), so the
		// exchange is not this object's to advance. Acting here would convert the
		// reservation into durable state for an exchange this object does not own —
		// a PUBREC would store an UNCORRELATED PUBREL (the bodyless entry has no
		// source), which a later resume reads as identifier-first with no source
		// mapping, and the still-live register then conflicts with it. Decline
		// instead: the reservation stands, nothing durable is written, and the
		// client's retransmitted acknowledgement is answered by the owner it ends up
		// bound to (this object is losing, or has lost, the record arbitration).
		sess.mu.Unlock()
		return nil
	}
	if isPubRec && !pubRecFailed {
		// The JS ACK subject for the PUBREL will be filled in at the delivery
		// attempt.
		sess.trackAsPubRel(pi, _EMPTY_)
	}
	removed = sess.untrackPublishEntry(pi)
	if removed != nil {
		jsAckSubject, q2AffClaim = removed.jsAckSubject, removed.q2AffClaim
	}
	// PUBACK / failed PUBREC freed an in-flight slot (a successful PUBREC is
	// net-zero under a Receive Maximum and yields no NAKs): release withheld
	// deliveries promptly. Spec5 [4.9].
	deferredNaks := sess.deferredNaksLocked()
	sess.mu.Unlock()

	if isPubRec && !pubRecFailed {
		// Name the source delivery this PUBREL completes. After a restart nothing
		// else links the two — cpending is memory-only — so without it a redelivery
		// of the source would allocate a fresh identifier and resend the payload of
		// an exchange already past PUBREC. Absent for a delivery we have no pending
		// entry for, which is exactly the uncorrelated form written before this
		// existed. See pubrel_correlation.go.
		//
		// Deliberately NO deterministic Nats-Msg-Id here. On this path the source is
		// acked right after the store, so the only legitimate re-store of the same
		// (durable, sequence) is the post-restart one that follows a redelivery — and
		// a surviving dedupe entry would silently drop it, leaving the client waiting
		// for a PUBREL that will never exist. A deduplication identity is only safe
		// where a presence check backs it up.
		var srcDur string
		var srcSeq uint64
		if removed != nil {
			srcDur, srcSeq = removed.jsDur, removed.sseq
		}
		// A DRAINED source records its intent BEFORE the store (I11): the record
		// either owes a PUBREL — recreate it, under this identifier — or it does
		// not and the payload may be redelivered. Nothing else makes recovery
		// positive. A definite failure rolls the whole PUBREC back so the exchange
		// simply restarts; an ambiguous one leaves the phase uncertain, where
		// neither resending nor finalizing is permitted.
		var msgId string
		if _, _, drained := sess.drainPubRelState(srcDur, srcSeq); drained {
			// The source claim may NOT be released here. A store response is not a
			// record (I12): on the interest-retained $MQTT_out a store with no
			// consumer succeeds, keeps nothing, and reports an ordinary success — so
			// acking the source now would discard the payload for a PUBREL that does
			// not exist, and the client would wait for a PUBCOMP exchange nothing can
			// advance. Only a CONFIRMED PUBREL — delivered, or found present at its
			// sequence by the resume barrier — may finalize it (I2b).
			drainedPubRel = true
			attempt, definite, ierr := sess.recordPubRelIntent(c, srcDur, srcSeq, pi)
			if ierr != nil {
				if definite {
					sess.mu.Lock()
					if ack, ok := sess.pendingPubRel[pi]; ok && ack != nil && ack.jsAckSubject == _EMPTY_ {
						delete(sess.pendingPubRel, pi)
						sess.retrackPublishLocked(pi, removed)
					}
					sess.mu.Unlock()
				}
				return ierr
			}
			// Only a drained PUBREL carries a deduplication identity, because only
			// here is it backed by a presence check on recovery. See
			// pubrel_correlation.go.
			msgId = mqttPubRelMsgId(sess.idHash, srcDur, srcSeq, attempt)
		}
		natsMsg, headerLen := mqttNewCorrelatedPubRel(pi, srcDur, srcSeq, msgId)
		_, err = sess.jsa.storeMsg(sess.pubRelSubject, headerLen, natsMsg)
		if err != nil {
			// The in-memory state was advanced BEFORE the store (it has to be:
			// mqttDeliverPubRelCb calls trackAsPubRel itself, so storing first would
			// race a real ack subject against our empty placeholder and lose it). With
			// the store failed, that leaves the identifier wedged in pendingPubRel
			// forever — nothing will ever fill its ack subject — and the delivery's
			// PUBLISH tracking gone, so it can neither be redelivered nor acked. Roll
			// both back.
			//
			// CONDITIONALLY, though. A storeMsg request can time out while the store
			// LANDED and its PUBREL was already delivered, and the callback's
			// trackAsPubRel(pi, reply) has then filled jsAckSubject. An unconditional
			// rollback would delete that real entry — leaking the identifier and
			// leaving a delivered PUBREL that no PUBCOMP can ack — and reinstate stale
			// PUBLISH tracking. So roll back ONLY while the entry is still our empty
			// placeholder: a filled one is proof the store landed, and the callback
			// owns completion from there.
			sess.mu.Lock()
			if ack, ok := sess.pendingPubRel[pi]; ok && ack != nil && ack.jsAckSubject == _EMPTY_ {
				delete(sess.pendingPubRel, pi)
				sess.retrackPublishLocked(pi, removed)
			}
			sess.mu.Unlock()
			// Failure to send out PUBREL will terminate the connection.
			return err
		}
	}

	// Send the ack to JS to remove the pending message from the consumer. On a
	// failed PUBREC this still applies: the receiver has rejected the message,
	// so it must not be redelivered.
	//
	// A session-affine shared QoS 2 copy takes the confirmed route instead: its
	// assignment claim may only be reaped once the affine side is PROVEN complete,
	// and a queued ack proves nothing. Ordered after the PUBREL store above, so the
	// copy is never released before the exchange has somewhere to continue from.
	// Whichever branch runs, the identifier has been released, so its durable
	// in-flight register goes with it (R5). Ordered per branch below rather than
	// once here, because on the QoS 2 path the delete is what completes the handoff
	// of the identifier's authority to the PUBREL record and may only follow a
	// CONFIRMED source ack.
	switch {
	case q2AffClaim != _EMPTY_:
		c.mqttFinishQ2AffineDelivery(q2AffClaim, jsAckSubject)
		sess.deleteInflightRegisterFor(removed)
	case drainedPubRel:
		// Deliberately nothing — not even the register delete. See the intent block
		// above: the claim and its JetStream delivery are held until the PUBREL is
		// proven to exist, so a PUBREL that was silently kept nowhere costs a
		// redelivery rather than the message. The register is that redelivery's
		// identity and must outlive an unproven PUBREL; the drain's own completion
		// path releases it. A non-drained QoS 2 source still acks here, as it always
		// has.
	case sess.finalizeDrainSource(c, removed):
		// A drained source, already resolved (or resolving) by the drain state
		// machine, which owns the ack. Only the register is ours to remove.
		sess.deleteInflightRegisterFor(removed)
	case isPubRec && !pubRecFailed && removed != nil && removed.regSeq != 0:
		// The PUBLISH phase is over and the identifier's durable authority has moved
		// to the PUBREL record stored above (R2). Deleting the register completes that
		// handoff, and the source ack is what licenses the delete — so here, and only
		// here, the ack is CONFIRMED (the jsa.confirmAck rule: a durable record is
		// being deleted on the strength of an ack).
		//
		// What the confirmation buys is CONTINUITY, not the absence of a duplicate: it
		// keeps the register from being removed over a source that was never actually
		// released, which would cost the redelivery its original identifier. A source
		// ack that never lands means JetStream redelivers regardless — that was true
		// before the register existed.
		if aerr := sess.confirmInflightSourceAck(jsAckSubject); aerr != nil {
			// The ack is unproven, so the source may still be pending and will be
			// re-offered. Two things have to happen, and the second is the one that is
			// easy to miss.
			//
			// The register is KEPT, so a later resume can restore this identifier.
			//
			// And the SOURCE is retired: its payload is past PUBREC and must never be
			// sent again, so the next delivery attempt is acknowledged instead of
			// delivered. Without that, the re-offered source is delivered again — before
			// PUBCOMP under a fresh identifier (the cpending link is gone), and after
			// PUBCOMP under any identifier at all, because the guard that would have
			// suppressed it tests pendingPubRel, which PUBCOMP removes. Retiring the
			// source is keyed by the source rather than by the identifier precisely so it
			// outlives the identifier's bookkeeping. See retireSourceLocked.
			//
			// The source is deliberately left UNACKNOWLEDGED here, with no best-effort
			// retry. A retry that happened to land would settle the message but strand its
			// retirement for the life of the session, because nothing else would ever drain
			// it. Leaving the source unacked instead means JetStream re-offers it exactly
			// once more, and settleRetiredSource then acknowledges that attempt WITH
			// CONFIRMATION — under a fresh reply subject rather than the stale one that just
			// failed — and releases both the retirement and the register.
			//
			// The retirement is recorded in BOTH places, and it has to be. In memory it
			// suppresses every redelivery on this broker. On the register it survives a
			// restart: without the durable half the next resume sees an ordinary
			// PUBLISH-phase register over a live source — indistinguishable from a delivery
			// that never reached PUBREC — and re-sends a payload the client has already
			// completed. Leaving the source deliberately unacked is exactly what makes that
			// window reachable, so the durable mark is not optional.
			// In memory FIRST, then durably. The in-memory half is what suppresses a
			// redelivery arriving on this broker, and the PUBREL whose delivery is already
			// in flight can bring one at any moment — so it must be in place before this
			// path spends a JetStream round trip on the durable half.
			//
			// The durable half is attempted here and, if it does not land, AGAIN at PUBCOMP,
			// which is the moment the alternative evidence disappears: while this exchange's
			// PUBREL record exists a restart is safe anyway, because recovery reads
			// register-plus-PUBREL as "past PUBREC". So this attempt failing is recoverable
			// rather than a residual.
			sess.mu.Lock()
			sess.retireSourceLocked(removed.jsDur, removed.sseq, &mqttRetiredSource{
				pi:         pi,
				regSeq:     removed.regSeq,
				nonce:      removed.regNonce,
				q2AffClaim: removed.q2AffClaim,
			})
			sess.mu.Unlock()
			// An unproven acknowledgement is AMBIGUOUS, and most often it landed with only
			// its confirmation lost. Read the source before making anything durable: if it
			// is provably complete the retirement has nothing to suppress and is dropped
			// here, which is what keeps a retirement from outliving its exchange and being
			// re-established by every later resume. Only a source that is still live (or
			// unprovable) is worth a durable mark.
			retiredKey := mqttDeferredKey{removed.jsDur, removed.sseq}
			if !sess.resolveRetiredSource(c, retiredKey) {
				sess.secureRetiredSource(c, retiredKey)
			}
			c.Warnf("MQTT: keeping the in-flight register of packet identifier %d for client %q and retiring its source %s/%d: "+
				"the source acknowledgement was not confirmed: %v", pi, sess.id, removed.jsDur, removed.sseq, aerr)
			break
		}
		sess.deleteInflightRegisterFor(removed)
	default:
		// QoS 1, a failed PUBREC, and any QoS 2 source that carried no register. The
		// register goes first, then the ordinary fire-and-forget ack: a lost ack here
		// costs one duplicate PUBLISH, which QoS 1 permits and which [MQTT-4.4.0-1]
		// does not bind — it governs UNACKNOWLEDGED messages — so a confirmed round
		// trip on the hottest path would buy nothing.
		sess.deleteInflightRegisterFor(removed)
		sess.jsa.sendAck(jsAckSubject)
	}
	for _, subj := range deferredNaks {
		sess.jsa.sendMsg(subj, AckNak)
	}
	return nil
}

func (c *client) mqttProcessPubAck(pi uint16) error {
	return c.mqttProcessPublishReceived(pi, false, mqttReasonSuccess)
}

func (c *client) mqttProcessPubRec(pi uint16, reason byte) error {
	return c.mqttProcessPublishReceived(pi, true, reason)
}

// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttProcessPubComp(pi uint16) {
	sess := c.mqtt.sess
	if sess == nil {
		return
	}

	var jsAckSubject string
	sess.mu.Lock()
	if sess.c != c {
		sess.mu.Unlock()
		return
	}
	jsAckSubject = sess.untrackPubRel(pi)
	// Does this exchange still hold the ONLY durable evidence that its source is past
	// PUBREC? Looked up before the ack below, because that ack is what destroys it.
	retiredKey, insecure := sess.retiredSourceForLocked(pi)
	deferredNaks := sess.deferredNaksLocked()
	sess.mu.Unlock()

	// A retired source whose register does not yet record the retirement is protected
	// across a restart only by this exchange's PUBREL record: recovery reads
	// register-plus-PUBREL as "past PUBREC" and suppresses the source, while an unmarked
	// register ALONE looks like an ordinary owed delivery and its payload is re-sent. So
	// the evidence is not destroyed until the retirement is durable — persist before ack,
	// the same rule the UNSUBSCRIBE drain follows.
	//
	// Withholding the acknowledgement costs a PUBREL redelivery, which [MQTT-4.4.0-1]
	// makes legitimate and which the client answers with another PUBCOMP; that brings us
	// back here to retry. Paced by AckWait, and self-healing.
	if insecure && !sess.secureRetiredSource(c, retiredKey) {
		c.Warnf("MQTT: withholding the acknowledgement of the PUBREL for packet identifier %d of client %q: "+
			"its retired source %s/%d is not durably recorded yet, and this record is the only other evidence of it",
			pi, sess.id, retiredKey.jsDur, retiredKey.sseq)
		for _, subj := range deferredNaks {
			sess.jsa.sendMsg(subj, AckNak)
		}
		return
	}

	// Send the ack to JS to remove the pending message from the consumer.
	sess.jsa.sendAck(jsAckSubject)
	for _, subj := range deferredNaks {
		sess.jsa.sendMsg(subj, AckNak)
	}
}

// Return the QoS from the given PUBLISH protocol's flags
func mqttGetQoS(flags byte) byte {
	return flags & mqttPubFlagQoS >> 1
}

func mqttIsRetained(flags byte) bool {
	return flags&mqttPubFlagRetain != 0
}

func sparkbParseBirthDeathTopic(topic []byte) (isBirth, isDeath, isCertificate bool) {
	if bytes.HasPrefix(topic, sparkbCertificatesTopicPrefix) {
		isCertificate = true
		topic = topic[len(sparkbCertificatesTopicPrefix):]
	}
	if !bytes.HasPrefix(topic, sparkbNamespaceTopicPrefix) {
		return false, false, false
	}
	topic = topic[len(sparkbNamespaceTopicPrefix):]

	parts := bytes.Split(topic, []byte{'/'})
	if len(parts) < 3 || len(parts) > 4 {
		return false, false, false
	}
	typ := bytesToString(parts[1])
	switch typ {
	case sparkbNBIRTH, sparkbDBIRTH:
		isBirth = true
	case sparkbNDEATH, sparkbDDEATH:
		isDeath = true
	default:
		return false, false, false
	}
	return isBirth, isDeath, isCertificate
}

//////////////////////////////////////////////////////////////////////////////
//
// SUBSCRIBE related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParseSubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, true)
}

func (c *client) mqttParseSubsOrUnsubs(r *mqttReader, b byte, pl int, sub bool) (uint16, []*mqttFilter, error) {
	var expectedFlag byte
	var action string
	if sub {
		expectedFlag = mqttSubscribeFlags
	} else {
		expectedFlag = mqttUnsubscribeFlags
		action = "un"
	}
	// Spec [MQTT-3.8.1-1], [MQTT-3.10.1-1]
	if rf := b & 0xf; rf != expectedFlag {
		return 0, nil, fmt.Errorf("%w: wrong %ssubscribe reserved flags: %x", errMQTTMalformedPacket, action, rf)
	}
	pi, err := mqttParsePIPacket(r)
	if err != nil {
		return 0, nil, err
	}
	// end is the absolute end of this packet. It is unaffected by the v5
	// properties block below, which only advances r.pos within [r.pos, end).
	end := r.pos + (pl - 2)
	// MQTT 5.0: a properties block follows the packet identifier in both
	// SUBSCRIBE and UNSUBSCRIBE. Spec5 [3.8.2.1], [3.10.2.1].
	// subID is the packet's single v5 Subscription Identifier (0 = none); it
	// applies to every filter the packet carries. Spec5 [3.8.2.1.2].
	var subID int
	if c.mqtt.proto == mqttProtoLevel5 {
		ctx := mqttPacketSub
		if !sub {
			ctx = mqttPacketUnsub
		}
		props, perr := r.readProperties(ctx)
		if perr != nil {
			return 0, nil, perr
		}
		// readProperties accepts repeated Subscription Identifiers because a
		// delivered PUBLISH may legitimately carry several; in a SUBSCRIBE more
		// than one is a Protocol Error. A value of 0 is already rejected there.
		// Spec5 [MQTT-3.8.2.1.2].
		if sub && props != nil {
			if len(props.subIDs) > 1 {
				return 0, nil, fmt.Errorf("%w: more than one subscription identifier in SUBSCRIBE", errMQTTProtocolError)
			}
			if len(props.subIDs) == 1 {
				subID = props.subIDs[0]
			}
		}
	}
	var filters []*mqttFilter
	for r.pos < end {
		// Don't make a copy now because, this will happen during conversion
		// or when processing the sub.
		topic, err := r.readBytes("topic filter", false)
		if err != nil {
			return 0, nil, err
		}
		if len(topic) == 0 {
			return 0, nil, errMQTTTopicFilterCannotBeEmpty
		}
		// Spec [MQTT-3.8.3-1], [MQTT-3.10.3-1]
		if err := mqttValidateTopic(topic, "topic filter"); err != nil {
			return 0, nil, err
		}
		// MQTT 5.0 Shared Subscription: "$share/{ShareName}/{filter}". v5 only;
		// for 3.1.1 "$share/..." has no special meaning and is treated as a
		// literal topic filter. Detect and strip the prefix + ShareName before
		// conversion so all downstream per-subject logic sees only the underlying
		// filter. Applies to both SUBSCRIBE and UNSUBSCRIBE. Spec5 [4.8.2].
		var shared bool
		var shareName []byte
		innerTopic := topic
		if c.mqtt.proto == mqttProtoLevel5 && bytes.HasPrefix(topic, mqttSharedSubPrefix) {
			rest := topic[len(mqttSharedSubPrefix):]
			idx := bytes.IndexByte(rest, mqttTopicLevelSep)
			// ShareName must be non-empty, must not contain '+' or '#', and must
			// be followed by a non-empty topic filter. Any violation is a
			// Malformed Packet (fail the whole packet). Spec5 [MQTT-4.8.2-1],
			// [MQTT-4.8.2-2].
			if idx <= 0 || idx == len(rest)-1 ||
				bytes.IndexByte(rest[:idx], mqttSingleLevelWC) >= 0 ||
				bytes.IndexByte(rest[:idx], mqttMultiLevelWC) >= 0 {
				return 0, nil, fmt.Errorf("%w: %q", errMQTTMalformedSharedSub, topic)
			}
			shared = true
			shareName = rest[:idx]
			innerTopic = rest[idx+1:]
		}
		var qos, reason byte
		var reasonStr string
		var retainHandling byte
		var noLocal bool
		var retainAsPublished bool
		// We are going to report if we had an error during the conversion,
		// but we don't fail the parsing. When processing the sub, we will
		// have an error then, and the processing of subs code will send
		// the proper mqttSubAckFailure flag for this given subscription.
		filter, err := mqttFilterToNATSSubject(innerTopic)
		if err != nil {
			c.Errorf("invalid topic %q: %v", topic, err)
			reason = mqttSubAckFailure
			reasonStr = err.Error()
		}
		if sub {
			if c.mqtt.proto == mqttProtoLevel5 {
				// v5 replaces the single QoS byte with a Subscription Options
				// byte: bits 0-1 QoS, 2 No Local, 3 Retain As Published, 4-5
				// Retain Handling, 6-7 reserved. Spec5 [3.8.3.1].
				var opts byte
				opts, err = r.readByte("subscription options")
				if err != nil {
					return 0, nil, err
				}
				// Reserved bits set is a Malformed Packet [MQTT-3-8.3-5].
				if opts&0xC0 != 0 {
					return 0, nil, fmt.Errorf("%w: reserved bits set in options byte 0x%x", errMQTTMalformedSubOption, opts)
				}
				retainHandling = (opts >> 4) & 0x03
				// Retain Handling 3 is reserved; sending it is a Protocol Error.
				// Checked before the No Local / Retain As Published rejection so a
				// reserved value is always classified as a Protocol Error, even
				// when those bits are also set. Spec5 [3.8.3.1].
				if retainHandling == 3 {
					return 0, nil, fmt.Errorf("%w: retain handling value 3 is reserved", errMQTTProtocolError)
				}
				noLocal = opts&0x04 != 0
				retainAsPublished = opts&0x08 != 0
				// No Local on a Shared Subscription is a Protocol Error
				// [MQTT-3.8.3-4]: fail the whole packet.
				if noLocal && shared {
					return 0, nil, fmt.Errorf("%w: No Local set on a shared subscription", errMQTTProtocolError)
				}
				qos = opts & 0x03
			} else {
				qos, err = r.readByte("QoS")
				if err != nil {
					return 0, nil, err
				}
			}
			// Spec [MQTT-3-8.3-4].
			if qos > 2 {
				return 0, nil, fmt.Errorf("%w: subscribe QoS value must be 0, 1 or 2, got %v", errMQTTMalformedSubOption, qos)
			}
		}
		f := &mqttFilter{ttopic: topic, filter: string(filter), qos: qos, retainHandling: retainHandling, noLocal: noLocal, retainAsPublished: retainAsPublished, subID: subID, reason: reason, reasonStr: reasonStr, shared: shared, group: string(shareName)}
		filters = append(filters, f)
	}
	// Spec [MQTT-3.8.3-3], [MQTT-3.10.3-2]
	if len(filters) == 0 {
		return 0, nil, fmt.Errorf("%w: %ssubscribe protocol must contain at least 1 topic filter", errMQTTProtocolError, action)
	}
	return pi, filters, nil
}

func mqttSubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := &strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(") QoS=")
		sb.WriteString(fmt.Sprintf("%v", f.qos))
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}

// For a MQTT QoS0 subscription, we create a single NATS subscription on the
// actual subject, for instance "foo.bar".
//
// For a MQTT QoS1+ subscription, we create 2 subscriptions, one on "foo.bar"
// (as for QoS0, but sub.mqtt.qos will be 1 or 2), and one on the subject
// "$MQTT.sub.<uid>" which is the delivery subject of the JS durable consumer
// with the filter subject "$MQTT.msgs.foo.bar".
//
// This callback delivers messages to the client as QoS0 messages, either
// because: (a) they have been produced as MQTT QoS0 messages (and therefore
// only this callback can receive them); (b) they are MQTT QoS1+ published
// messages but this callback is for a subscription that is QoS0; or (c) the
// published messages come from (other) NATS publishers on the subject.
//
// This callback must reject a message if it is known to be a QoS1+ published
// message and this is the callback for a QoS1+ subscription because in that
// case, it will be handled by the other callback. This avoid getting duplicate
// deliveries.
func mqttDeliverMsgCbQoS0(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if pc.kind == JETSTREAM && len(reply) > 0 && strings.HasPrefix(reply, jsAckPre) {
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// Lock here, otherwise we may be called with sub.mqtt == nil. Ignore
	// wildcard subscriptions if this subject starts with '$', per Spec
	// [MQTT-4.7.2-1].
	sess.subsMu.RLock()
	subQoS := sub.mqtt.qos
	noLocal := sub.mqtt.noLocal
	retainAsPublished := sub.mqtt.retainAsPublished
	subID := sub.mqtt.subID
	// For a shared subscription the raw-subject sub must deliver only
	// QoS0-published messages; stored (QoS 1/2 published) messages arrive through
	// the shared JS consumer instead, so treat shared like a QoS1+ sub below when
	// deciding to defer to the QoS 1/2 callback. Spec5 [4.8.2].
	shared := sub.mqtt.shared
	ignore := mqttMustIgnoreForReservedSub(sub, subject)
	sess.subsMu.RUnlock()

	if ignore {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	var topic []byte
	var props []byte
	// originHash identifies the publishing session for the No Local option, set
	// only when the publisher is a local MQTT client on this server. It is never
	// taken from the message header on this direct path (see the non-MQTT branch
	// below for why a stamped header is not trusted here). Spec5 [3.8.3.1].
	var originHash string
	// origRetain is the original RETAIN flag, for the Retain As Published option.
	// It always comes from the stamped Nmqtt-Ret header (the publisher's live
	// pp.flags RETAIN is cleared before fan-out); parsed only when the sub wants
	// it, since a retained publish is uncommon.
	var origRetain bool
	if pc.isMqtt() {
		// This is an MQTT publisher directly connected to this server.

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 and the sub has the QoS>0 then the message will be delivered by
		// mqttDeliverMsgCbQoS12.
		msgQoS := mqttGetQoS(pc.mqtt.pp.flags)
		if (subQoS > 0 || shared) && msgQoS > 0 {
			return
		}
		topic = pc.mqtt.pp.topic
		props = pc.mqtt.pp.props
		// If the subject is different than the one in pp.subject, then some
		// mapping/transform occurred and we need to recreate the topic.
		if subject != bytesToString(pc.mqtt.pp.subject) {
			topic = natsSubjectStrToMQTTTopic(subject)
		}
		if noLocal && pc.mqtt.sess != nil {
			originHash = pc.mqtt.sess.idHash
		}
		if retainAsPublished {
			if h := mqttParsePublishNATSHeader(hdr); h != nil {
				origRetain = h.retained
			}
		}

	} else {
		// Non MQTT client, could be NATS publisher, or ROUTER, etc.. In the
		// standalone broker this is also the path for a genuine local MQTT
		// publisher: unlike upstream (where the MQTT connection is an in-process
		// client and pc is that same object for a same-server delivery), every
		// delivery here — including a client's own QoS0 publish echoed back to
		// its own subscription — arrives over the real NATS wire through a
		// synthetic per-subscription client (see (*client).processSub,
		// shim_engine.go), so pc.isMqtt() never reports true for a live message.
		h := mqttParsePublishNATSHeader(hdr)

		// Check the subscription's QoS. If the message was published with a
		// QoS>0 (in the header) and the sub has the QoS>0 then the message will
		// be delivered by mqttDeliverMsgCbQoS12.
		if (subQoS > 0 || shared) && h != nil && h.qos > 0 {
			return
		}

		// No Local: same authenticated-marker check as mqttDeliverMsgCbQoS12. The
		// stored/live origin is an HMAC over (publisher's idHash, subject,
		// payload); recomputing it for the subscribing session and comparing
		// proves self-authorship without trusting a client-forgeable header
		// value directly. Spec5 [3.8.3.1].
		if noLocal && h != nil {
			expected := mqttOriginMarker(cc.srv.mqtt.originKey, sess.idHash, subject, msg)
			if mqttNoLocalSuppress(true, string(h.origin), expected) {
				return
			}
		}

		// If size is more than what a MQTT client can handle, we should probably reject,
		// for now just truncate.
		if len(msg) > mqttMaxPayloadSize {
			msg = msg[:mqttMaxPayloadSize]
		}
		topic = natsSubjectStrToMQTTTopic(subject)
		if h != nil {
			props = h.props
			origRetain = h.retained
		}
	}

	// No Local: drop a message published by this same session. Spec5 [3.8.3.1].
	if mqttNoLocalSuppress(noLocal, originHash, sess.idHash) {
		return
	}

	// MQTT 5.0 Message Expiry Interval of 0 expires the message immediately, so
	// it must not reach any subscriber, whatever its protocol level. This live
	// path otherwise waits ~0s, so no interval rewrite is needed. Spec5
	// [3.3.2.3.3].
	if len(props) > 0 {
		if _, expired := mqttForwardExpiry(props, 0); expired {
			return
		}
	}

	// Message never has a packet identifier nor is marked as duplicate. A QoS0
	// message dropped for exceeding the client's Maximum Packet Size needs no
	// completion (it is not JetStream-backed here), so the result is ignored.
	pc.mqttEnqueuePublishMsgTo(cc, sub, 0, 0, false, retainAsPublished && origRetain, subID, topic, msg, props)
}

// This is the callback attached to a JS durable subscription for a MQTT QoS 1+
// sub. Only JETSTREAM should be sending a message to this subject (the delivery
// subject associated with the JS durable consumer), but in cluster mode, this
// can be coming from a route, gw, etc... We make sure that if this is the case,
// the message contains a NATS/MQTT header that indicates that this is a
// published QoS1+ message.
func mqttDeliverMsgCbQoS12(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	// Message on foo.bar is stored under $MQTT.msgs.foo.bar, so the subject has to be
	// at least as long as the stream subject prefix "$MQTT.msgs.", and after removing
	// the prefix, has to be at least 1 character long.
	if len(subject) < len(mqttStreamSubjectPrefix)+1 {
		return
	}

	hdr, msg := pc.msgParts(rmsg)
	h := mqttParsePublishNATSHeader(hdr)
	if pc.kind != JETSTREAM && (h == nil || h.qos == 0) {
		// MQTT QoS 0 messages must be ignored, they will be delivered by the
		// other callback, the direct NATS subscription. All JETSTREAM messages
		// will have the header.
		return
	}

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// We lock to check some of the subscription's fields and if we need to keep
	// track of pending acks, etc. There is no need to acquire the subsMu RLock
	// since sess.Lock is overarching for modifying subscriptions.
	sess.mu.Lock()
	// A resume-reconciliation subscription holds its consumer's interest before
	// the CONNACK, so JetStream pushes the session's offline backlog into a
	// window in which nothing may be transmitted yet. WAIT for the phase to
	// release it rather than skipping: these are first transmissions, and
	// skipping them would stall each until AckWait and then mark it DUP. The
	// lock is released while waiting — the releasing goroutine needs it — and
	// every check below is re-evaluated afterwards. See sess_resume_reconcile.go.
	if sub.mqtt != nil && sub.mqtt.phaseGate != nil {
		gate := sub.mqtt.phaseGate
		sess.mu.Unlock()
		timer := time.NewTimer(mqttPhaseGateWaitFor(cc))
		select {
		case <-gate:
		case <-timer.C:
		}
		timer.Stop()
		sess.mu.Lock()
	}
	if sess.c != cc || sub.mqtt == nil || sub.mqtt.closed {
		// A delivery the phase never released — the wait ran out, or the filter
		// was reclaimed unadopted — was NEVER TRANSMITTED, so its redelivery is a
		// first send however many attempts JetStream has counted. Record it the
		// way an in-flight-cap withholding is recorded, which is what makes
		// trackPublish clear DUP on the attempt that finally goes out
		// [MQTT-3.3.1-1] (and lets a freed slot NAK it forward rather than
		// waiting out AckWait). Only for a phase-owned subscription: an ordinary
		// closed one was torn down (a QoS downgrade, an UNSUBSCRIBE) and its
		// messages are not owed to this client at all.
		// sess.c == nil is the refused-CONNECT case: the connection was detached
		// before this delivery woke, and the message is still owed to the SESSION,
		// so the marker belongs to it. A session rebound to a DIFFERENT client is
		// that connection's to account for, not this stale callback's.
		//
		// The nil arm covers a RACE, not a state a test can schedule: a refusal
		// releases the gates and then detaches, microseconds apart, so whether a
		// woken delivery sees the client or nil depends on which goroutine takes
		// sess.mu first. A mutation removing it therefore SURVIVES — the honest
		// signal that this is defensive breadth rather than a pinned behavior;
		// without it, the deliveries that lose that race are the ones whose
		// redelivery reaches the client wrongly marked DUP. Same posture as the
		// stale-attempt re-validation in sess_uncertain.go.
		if (sess.c == cc || sess.c == nil) && sub.mqtt != nil && sub.mqtt.phaseOwned && sub.mqtt.jsDur != _EMPTY_ {
			sseq, _, _, _, _ := ackReplyInfo(reply)
			if sseq > 0 {
				if sess.deferred == nil {
					sess.deferred = make(map[mqttDeferredKey]string)
				}
				sess.deferred[mqttDeferredKey{sub.mqtt.jsDur, sseq}] = reply
			}
		}
		sess.mu.Unlock()
		return
	}

	// A DRAINED consumer delivers only what it already started. Branch before any
	// of the ordinary checks, and in this order — the record is the authority, not
	// the in-memory tracking maps, which say nothing after a restart. See
	// unsub_drain.go.
	var drainClaimed bool
	if d := sess.drainByDur[sub.mqtt.jsDur]; d != nil {
		sseq, _, _, _, _ := ackReplyInfo(reply)
		st := d.seqs[sseq]
		switch {
		case st == nil || !st.claimed:
			// A FRESH message on a drained filter: the client unsubscribed, so it is
			// not owed this. While the drain is still staged it is left to AckWait —
			// acking before the packet's save would lose it if that save rolls back.
			//
			// The same applies to a sequence whose removal is UNCONFIRMED: its record
			// state is unknown, so acking would destroy a message the stored record
			// may still claim (I1). Leave it pending; the resolution discharges the
			// ack once it proves the removal durable, and a foreign verdict makes it
			// somebody else's message entirely. See unsub_drain.go.
			unconfirmed := st != nil && st.unclaimUnconfirmed
			staged := d.staged || unconfirmed
			sess.mu.Unlock()
			if unconfirmed {
				// This redelivery IS the retry clock. The finalizer's own immediate attempt
				// may itself have failed, and the periodic pass only runs on its own
				// schedule — so retrying here is what keeps an unresolved removal from
				// sitting unacked and unrestored, with every further save on the session
				// refused, for longer than one AckWait. AckWait paces it.
				sess.resolveUncertainNow(cc)
			}
			if !staged {
				sess.jsa.sendAck(reply)
			}
			return
		case st.phase == drainPubRelConfirmed:
			// The exchange is past PUBREC with a proven PUBREL: never deliver the
			// payload again. Finalize (which acks on a confirmed save).
			sess.mu.Unlock()
			sess.finalizeDrainDelivery(cc, sub.mqtt.jsDur, sseq, reply)
			return
		case st.phase == drainPubRelUncertain:
			// Past PUBREC, PUBREL existence unknown: deliver nothing, ack nothing,
			// finalize nothing. The durable intent resolves this on resume.
			st.ack = reply
			sess.mu.Unlock()
			return
		case st.phase == drainPublish:
			// Claimed and still in the PUBLISH phase: a forced redelivery.
			drainClaimed = true
		default:
			// An unknown phase must never fall through into a delivery. Leave the
			// message pending so AckWait retries it once the state is understood.
			sess.mu.Unlock()
			cc.Errorf("MQTT: drained sequence %v is in an unknown phase %v; withholding delivery", sseq, st.phase)
			return
		}
	}

	// In this callback we handle only QoS-published messages to QoS
	// subscriptions. Ignore if either is 0, will be delivered by the other
	// callback, mqttDeliverMsgCbQos1.
	var qos byte
	if h != nil {
		qos = h.qos
	}
	if qos > sub.mqtt.qos {
		qos = sub.mqtt.qos
	}
	// A QoS0 member of a shared subscription still consumes stored messages
	// through the group consumer; it just delivers them at QoS0 and acks the JS
	// message (handled below after the reserved/deny checks). For a non-shared
	// QoS0 result the direct NATS subscription delivers instead. Spec5 [4.8.2].
	sharedQoS0 := qos == 0 && sub.mqtt.shared
	if qos == 0 && !sharedQoS0 {
		sess.mu.Unlock()
		return
	}

	// Check for reserved subject violation. If so, we will send the ack to
	// remove the message, and do nothing else.
	strippedSubj := subject[len(mqttStreamSubjectPrefix):]
	if mqttMustIgnoreForReservedSub(sub, strippedSubj) {
		sess.mu.Unlock()
		sess.jsa.sendAck(reply)
		return
	}

	// A broad wildcard subscription can overlap a subscribe deny clause.
	cc.mu.Lock()
	denied := cc.mperms != nil && cc.checkDenySub(strippedSubj)
	cc.mu.Unlock()
	if denied {
		sess.mu.Unlock()
		sess.jsa.sendAck(reply)
		return
	}

	// No Local: a message published by this same session must not be delivered
	// to this subscription. Ack it so the JS consumer does not redeliver it.
	if sub.mqtt.noLocal {
		// The stored origin is an HMAC authenticating "this session published this
		// payload on this subject". Recompute it for the subscribing session; a
		// match proves authenticity (a forged/replayed header cannot reproduce the
		// server secret for a different payload) and self-authorship. A mismatch or
		// absent marker delivers. Spec5 [3.8.3.1].
		var marker string
		if h != nil {
			marker = string(h.origin)
		}
		expected := mqttOriginMarker(cc.srv.mqtt.originKey, sess.idHash, strippedSubj, msg)
		if mqttNoLocalSuppress(true, marker, expected) {
			sess.mu.Unlock()
			sess.jsa.sendAck(reply)
			return
		}
	}

	// Retain As Published: capture the option under the lock; the delivered
	// message keeps the original RETAIN flag (from the stored Nmqtt-Ret header)
	// only when the subscription set it. Spec5 [3.8.3.1].
	retainAsPub := sub.mqtt.retainAsPublished && h != nil && h.retained
	// Subscription Identifier: capture under the lock, injected on enqueue below.
	// Spec5 [3.8.2.1.2].
	subID := sub.mqtt.subID

	if sharedQoS0 {
		// Deliver at QoS0 (no packet identifier, no tracking) and ack the JS
		// message so the group consumer does not redeliver it. Spec5 [4.8.2].
		sess.mu.Unlock()
		originalTopic := natsSubjectStrToMQTTTopic(strippedSubj)
		var props []byte
		if h != nil {
			props = h.props
		}
		if len(props) > 0 {
			_, _, _, ts, _ := ackReplyInfo(reply)
			if p, expired := mqttForwardExpiry(props, ts); expired {
				sess.jsa.sendAck(reply)
				return
			} else {
				props = p
			}
		}
		pc.mqttEnqueuePublishMsgTo(cc, sub, 0, 0, false, retainAsPub, subID, originalTopic, msg, props)
		sess.jsa.sendAck(reply)
		return
	}

	// A shared-subscription delivery at effective QoS 2 is MOVED, never tracked
	// here: the exchange has to complete with the client the server chose, and a
	// message pending on the group durable is redelivered to whichever member
	// AckWait reaches next. The move takes durable ownership of it and hands it to
	// this session's own consumer. Spec5 [MQTT-4.8.2-5], [MQTT-4.8.2-6]; see
	// qos2_shared_affine.go.
	//
	// Unconditional, deliberately: there is no "only if this delivery already
	// started" test, because `started` reads in-memory tracking maps that say
	// nothing after a restart, a failover, or on another broker.
	if sub.mqtt.shared && qos == 2 {
		groupDurable := sub.mqtt.jsDur
		sess.mu.Unlock()
		var props []byte
		if h != nil {
			props = h.props
		}
		sess.moveStartedSharedQoS2(cc.srv, cc.mqtt.asm, &mqttQ2AffMoveInput{
			groupDurable: groupDurable,
			natsSubject:  strippedSubj,
			reply:        reply,
			props:        props,
			msg:          msg,
			retained:     retainAsPub,
			subID:        subID,
			maxPayload:   atomic.LoadInt32(&cc.mpay),
		})
		return
	}

	sseq, _, _, _, _ := ackReplyInfo(reply)
	// A source RETIRED by an unproven acknowledgement is past PUBREC: its payload must
	// never go out again, whatever the identifier bookkeeping now says. Tested BEFORE
	// trackPublish, because after PUBCOMP nothing in those maps remembers the exchange
	// and the payload would simply be re-sent under a fresh identifier. Not applied to a
	// drain-managed sequence: there the drain state machine owns both the decision and
	// the acknowledgement.
	if !drainClaimed && sess.sourceRetiredLocked(sub.mqtt.jsDur, sseq) {
		sess.mu.Unlock()
		sess.settleRetiredSource(cc, sub.mqtt.jsDur, sseq, reply)
		return
	}

	pi, dup, started, isNew := sess.trackPublish(sub.mqtt.jsDur, reply)
	// The allocation's nonce, read under the same lock that allocated it: it is the
	// identity every later reservation touch is guarded by, and reading it later would
	// race the teardown that replaces the entry with a reservation.
	var regNonce string
	if isNew {
		if ack := sess.pendingPublish[pi]; ack != nil {
			regNonce = ack.regNonce
		}
	}
	if drainClaimed && pi != 0 {
		// The RECORD says this delivery was already transmitted, so override what
		// the in-memory maps inferred: on a broker that has not recovered this
		// delivery's register cpending is empty, so trackPublish reports
		// started=false and derives dup only from JetStream's delivery count.
		// started=true keeps the message out of the !started expiry discard below,
		// and dup=true is simply correct — it WAS sent before.
		started, dup = true, true
		if d := sess.drainByDur[sub.mqtt.jsDur]; d != nil {
			if st := d.seqs[sseq]; st != nil {
				st.ack = reply
				// Normally a reconciliation of equal values: the resume barrier restored
				// the identifier from this delivery's in-flight register before any source
				// interest existed, so trackPublish returned the very identifier the
				// record already names. It still assigns, because where the register was
				// legitimately absent — a claim that never got one, or one dropped as
				// provably complete — a later PUBREL intent must name the identifier
				// actually on the wire.
				st.pi = pi
				// Repair the register sequence from the live entry. A drain record written
				// before RegSeq existed carries none, and a drain that acquired its register
				// through recovery rather than through staging has it only here. The drain
				// releases the register when the exchange completes, so a zero left in place
				// would leak it (finalizeDrainDelivery).
				if ack := sess.pendingPublish[pi]; ack != nil && ack.regSeq != 0 {
					st.regSeq = ack.regSeq
				}
			}
		}
	}
	sess.mu.Unlock()

	if pi == 0 {
		// We have reached max pending, don't send the message now.
		// JS will cause a redelivery and if by then the number of pending
		// messages has fallen below threshold, the message will be resent.
		return
	}

	originalTopic := natsSubjectStrToMQTTTopic(strippedSubj)
	var props []byte
	if h != nil {
		props = h.props
	}
	// MQTT 5.0 Message Expiry Interval: a v5 subscriber must receive the interval
	// reduced by the time the message waited in the server; if it has already
	// elapsed the message must not be delivered and its stored copy dropped. The
	// store timestamp is carried in the JetStream ack reply subject. Expiry is a
	// property of the message, so it equally gates delivery to a 3.1.1
	// subscriber (whose properties are then stripped on the wire).
	// Only a message whose onward delivery has NOT started may be dropped on
	// expiry; a redelivery of an already-sent PUBLISH must be resent, not dropped.
	// Spec5 [3.3.2.3.3], [MQTT-4.4.0-1].
	if len(props) > 0 && !started {
		_, _, _, ts, _ := ackReplyInfo(reply)
		var expired bool
		if props, expired = mqttForwardExpiry(props, ts); expired {
			sess.mu.Lock()
			removed := sess.untrackPublishEntry(pi)
			sess.mu.Unlock()
			// The identifier is released, so its register must go with it (R5). Ordered
			// before the source ack, so a crash in between leaves a register over a
			// source that is still live rather than one over a completed delivery.
			// Normally a no-op: expiry is decided BEFORE the register is written, so a
			// message dropped here never paid for one. It runs anyway because `started`
			// is not the only way an entry can carry a register — a drain-claimed
			// redelivery reaches this point with one.
			sess.deleteInflightRegisterFor(removed)
			if !sess.finalizeDrainSource(cc, removed) {
				sess.jsa.sendAck(reply)
			}
			return
		}
	}

	// A newly allocated identifier is not sendable until its durable in-flight
	// register is confirmed (R1, inflight_register.go). Synchronous, as the shared
	// QoS 2 move above already is on this same path: the identifier the client is
	// about to hold has to survive this broker.
	//
	// As late as possible, and after the expiry check above: a message that is
	// discarded rather than sent has no identifier to make durable, so it should not
	// pay a round trip for one. Still strictly BEFORE the enqueue below, which is the
	// send.
	if isNew && !sess.storeInflightRegister(cc, pi, qos, sub.mqtt.jsDur, sseq, regNonce, _EMPTY_) {
		return
	}
	if !pc.mqttEnqueuePublishMsgTo(cc, sub, pi, qos, dup, retainAsPub, subID, originalTopic, msg, props) {
		// The PUBLISH exceeds the client's MQTT 5.0 Maximum Packet Size. Discard
		// it and complete the delivery so JetStream does not redeliver it forever:
		// release the packet identifier and ack the JS message. Spec5 [3.1.2.11.4].
		sess.mu.Lock()
		removed := sess.untrackPublishEntry(pi)
		sess.mu.Unlock()
		sess.deleteInflightRegisterFor(removed)
		if !sess.finalizeDrainSource(cc, removed) {
			sess.jsa.sendAck(reply)
		}
	}
}

func mqttDeliverPubRelCb(sub *subscription, pc *client, _ *Account, subject, reply string, rmsg []byte) {
	if sub.client.mqtt == nil || sub.client.mqtt.sess == nil || reply == _EMPTY_ {
		return
	}
	if h := mqttTestPubRelDeliveryDelayV.Load(); h != nil {
		(*h)()
	}

	hdr, _ := pc.msgParts(rmsg)
	pi := mqttParsePubRelNATSHeader(hdr)
	if pi == 0 {
		return
	}
	corr := mqttParsePubRelCorrelation(hdr)

	// This is the client associated with the subscription.
	cc := sub.client

	// This is immutable
	sess := cc.mqtt.sess

	// A PUBREL that completes a DRAINED source is owned end to end here, with ONE
	// ordering on every path — first delivery and replay alike: confirm the phase
	// (the delivery itself is proof the record exists), remove and persist the
	// source claim, ack the source, and only THEN forward.
	//
	// There is deliberately no fast path that forwards first. With one, the client
	// could PUBCOMP, the PUBREL record could be acked away, and a crash before the
	// readLoop's finalize would leave a claimed source sequence whose exchange is
	// already complete — so the next redelivery would send the payload a second
	// time. The cost is one session-record save before a drained PUBREL reaches the
	// client (I10). See unsub_drain.go.
	if corr.Ok {
		if d, claimed := sess.confirmDrainPubRel(corr.SrcDur, corr.SrcSeq); d != nil && claimed {
			sess.finalizeDrainDelivery(cc, corr.SrcDur, corr.SrcSeq, _EMPTY_)
			sess.mu.Lock()
			unproven := false
			if st := d.seqs[corr.SrcSeq]; st != nil {
				// Still claimed, or claimed-but-unconfirmed: in neither case is the
				// removal known to be in the record, and only a known-durable removal
				// may be followed by forwarding (I10).
				unproven = st.claimed || st.unclaimUnconfirmed
			}
			sess.mu.Unlock()
			if unproven {
				// The claim removal was not persisted (refused fence, failed save), or
				// its outcome is unknown. Forward nothing and ack nothing; AckWait
				// retries the whole step, and a readback resolves the unknown case.
				return
			}
			if h := mqttTestPubRelPreForwardHookV.Load(); h != nil {
				(*h)()
			}
		}
	}

	sess.mu.Lock()
	if sess.c != cc || sess.pubRelConsumer == nil {
		sess.mu.Unlock()
		return
	}
	// A DUPLICATE RECORD for the same identifier — the "a superseded broker stored
	// after a peer recreated the exchange" race, if it outlives deduplication —
	// must be acked and dropped, not forwarded. The client needs exactly one
	// PUBREL, and a duplicate merely left unacked would redeliver forever.
	//
	// The test is on the STREAM SEQUENCE, which identifies the record. It must not
	// be on the ack subject: every AckWait redelivery of the SAME record arrives on
	// a new ack subject, so comparing those would classify ordinary redelivery as
	// duplication and silently ack away a PUBREL the client is still waiting for.
	//
	// Note also that this is a positive test on TRACKED state, not "no pending
	// entry": after a restart there are no pending entries at all, and a genuine
	// pending PUBREL must still be forwarded.
	if existing, ok := sess.pendingPubRel[pi]; ok && existing != nil && existing.jsAckSubject != _EMPTY_ {
		if sseq, _, _, _, _ := ackReplyInfo(reply); sseq != 0 && existing.sseq != 0 && sseq != existing.sseq {
			sess.mu.Unlock()
			sess.jsa.sendAck(reply)
			return
		}
	}
	sess.trackAsPubRel(pi, reply)
	trace := cc.trace
	sess.mu.Unlock()

	cc.mqttEnqueuePubResponse(mqttPacketPubRel, pi, trace)
}

// The MQTT Server MUST NOT match Topic Filters starting with a wildcard
// character (# or +) with Topic Names beginning with a $ character, Spec
// [MQTT-4.7.2-1]. We will return true if there is a violation.
//
// Session or subMu lock must be held on entry to protect access to sub.mqtt.
func mqttMustIgnoreForReservedSub(sub *subscription, subject string) bool {
	// If the subject does not start with $ nothing to do here.
	if !sub.mqtt.reserved || len(subject) == 0 || subject[0] != mqttReservedPre {
		return false
	}
	return true
}

// Check if a sub is a reserved wildcard. E.g. '#', '*', or '*/" prefix.
func isMQTTReservedSubscription(subject string) bool {
	if len(subject) == 1 && (subject[0] == fwc || subject[0] == pwc) {
		return true
	}
	// Match "*.<>"
	if len(subject) > 1 && (subject[0] == pwc && subject[1] == btsep) {
		return true
	}
	return false
}

func sparkbReplaceDeathTimestamp(msg []byte) []byte {
	const VARINT = 0
	const TIMESTAMP = 1

	orig := msg
	buf := bytes.NewBuffer(make([]byte, 0, len(msg)+16)) // 16 bytes should be enough if we need to add a timestamp
	writeDeathTimestamp := func() {
		// [tck-id-conformance-mqtt-aware-ndeath-timestamp] A Sparkplug Aware
		// MQTT Server MAY replace the timestamp of NDEATH messages. If it does,
		// it MUST set the timestamp to the UTC time at which it attempts to
		// deliver the NDEATH to subscribed clients
		//
		// sparkB spec: 6.4.1. Google Protocol Buffer Schema
		//      optional uint64 timestamp = 1; // Timestamp at message sending time
		//
		// SparkplugB timestamps are milliseconds since epoch, represented as
		// uint64 in go, transmitted as protobuf varint.
		ts := uint64(time.Now().UnixMilli())
		buf.Write(protoEncodeVarint(TIMESTAMP<<3 | VARINT))
		buf.Write(protoEncodeVarint(ts))
	}

	for len(msg) > 0 {
		fieldNumericID, fieldType, size, err := protoScanField(msg)
		if err != nil {
			return orig
		}
		if fieldType != VARINT || fieldNumericID != TIMESTAMP {
			// Add the field as is
			buf.Write(msg[:size])
			msg = msg[size:]
			continue
		}

		writeDeathTimestamp()

		// Add the rest of the message as is, we are done
		buf.Write(msg[size:])
		return buf.Bytes()
	}

	// Add timestamp if we did not find one.
	writeDeathTimestamp()

	return buf.Bytes()
}

// Common function to mqtt delivery callbacks to serialize and send the message
// to the `cc` client.
// mqttEnqueuePublishMsgTo frames and queues an outbound PUBLISH to cc. It
// returns false without sending when the packet would exceed the client's MQTT
// 5.0 Maximum Packet Size; the caller is then responsible for completing the
// message (for QoS1/2, acking the JS delivery so it is not redelivered).
// subID is the subscription's v5 Subscription Identifier (0 = none), injected
// into the properties block for a v5 receiver. Spec5 [3.8.2.1.2].
func (c *client) mqttEnqueuePublishMsgTo(cc *client, sub *subscription, pi uint16, qos byte, dup, retainAsPub bool, subID int, topic, msg, props []byte) bool {
	// [tck-id-conformance-mqtt-aware-nbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make NBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/NBIRTH/edge_node_id with
	// the MQTT retain flag set to true
	//
	// [tck-id-conformance-mqtt-aware-dbirth-mqtt-retain] A Sparkplug Aware
	// MQTT Server MUST make DBIRTH messages available on the topic:
	// $sparkplug/certificates/namespace/group_id/DBIRTH/edge_node_id/device_id
	// with the MQTT retain flag set to true
	//
	// $sparkplug/certificates messages are sent as NATS messages, so we
	// need to add the retain flag when sending them to MQTT clients.

	// retainAsPub keeps the original RETAIN flag for a v5 Retain As Published
	// subscription. Spec5 [3.8.3.1].
	retain := retainAsPub
	isBirth, isDeath, isCertificate := sparkbParseBirthDeathTopic(topic)
	if isBirth && qos == 0 {
		retain = retain || isCertificate
	} else if isDeath && !isCertificate {
		msg = sparkbReplaceDeathTimestamp(msg)
	}

	// MQTT 5.0 Subscription Identifier: inject the subscription's identifier into
	// the delivered PUBLISH's properties. Only for a v5 receiver (a 3.1.1 receiver
	// gets no properties section) and before the size check below so the extra
	// bytes count against Maximum Packet Size. Spec5 [3.8.2.1.2].
	v5 := cc.mqtt.proto == mqttProtoLevel5
	if v5 && subID > 0 {
		props = mqttInjectSubID(props, subID)
	}

	// MQTT 5.0 outbound Topic Alias: for a v5 client that advertised a Topic Alias
	// Maximum, the alias table lookup/assignment and framing must be atomic with
	// queueOutbound so a binding PUBLISH (full topic + alias) is always buffered
	// before any alias-only PUBLISH referencing it. That work moves under cc.mu
	// below. Every other receiver keeps the original path: frame and size-check
	// before the lock. Spec5 [3.3.2.3.4].
	useAlias := v5 && cc.mqtt.outTopicAliasMax > 0 && len(topic) > 0

	var flags byte
	var headerBytes []byte
	var alias uint16
	if !useAlias {
		flags, headerBytes = mqttMakePublishHeader(pi, qos, dup, retain, v5, topic, props, len(msg))
		// MQTT 5.0 Maximum Packet Size: never send a PUBLISH larger than the client
		// is willing to accept. Discard it instead and let the caller complete the
		// delivery. Spec5 [3.1.2.11.4]. mqtt.maxPacketSize is 0 for 3.1.1 or when the
		// client did not advertise a limit.
		if mp := cc.mqtt.maxPacketSize; mp != 0 && len(headerBytes)+len(msg) > int(mp) {
			return false
		}
	}

	cc.mu.Lock()
	if useAlias {
		// Build under the lock so the binding commit and queueOutbound cannot be
		// interleaved by a concurrent delivery on the same topic. ok is false only
		// when even the plain full-topic frame exceeds Maximum Packet Size, matching
		// the pre-lock discard above; a fresh binding is committed only when its
		// frame is actually queued.
		var ok bool
		flags, headerBytes, alias, ok = cc.mqttMakeAliasedPublishHeader(pi, qos, dup, retain, topic, props, len(msg))
		if !ok {
			cc.mu.Unlock()
			return false
		}
	}
	if sub.mqtt.prm != nil {
		for _, data := range sub.mqtt.prm {
			cc.queueOutbound(data)
		}
		sub.mqtt.prm = nil
	}
	cc.queueOutbound(headerBytes)
	cc.queueOutbound(msg)
	c.addToPCD(cc)
	trace := cc.trace
	cc.mu.Unlock()

	if trace {
		pp := mqttPublish{
			topic: topic,
			flags: flags,
			pi:    pi,
			sz:    len(msg),
		}
		// Trace the resolved full topic (clearer than the empty wire topic) and note
		// the alias when one was used.
		tr := mqttPubTrace(&pp)
		if alias > 0 {
			tr += fmt.Sprintf(" alias=%v", alias)
		}
		cc.traceOutOp("PUBLISH", []byte(tr))
	}
	return true
}

// Serializes to the given writer the message for the given subject.
func (w *mqttWriter) WritePublishHeader(pi uint16, qos byte, dup, retained, v5 bool, topic, props []byte, msgLen int) byte {
	// Compute len (will have to add packet id if message is sent as QoS>=1)
	pkLen := 2 + len(topic) + msgLen
	var flags byte

	// Set flags for dup/retained/qos1
	if dup {
		flags |= mqttPubFlagDup
	}
	if retained {
		flags |= mqttPubFlagRetain
	}
	if qos > 0 {
		pkLen += 2
		flags |= qos << 1
	}
	// MQTT 5.0 PUBLISH carries a properties section after the packet identifier.
	// props (when set) is the raw block from the publisher, including its length
	// prefix, forwarded verbatim; otherwise a single 0 length byte. For 3.1.1
	// receivers (v5 false) there is no properties section, which is how
	// properties get stripped when delivering to a 3.1.1 subscriber. Spec5 [3.3.2.3].
	if v5 {
		if len(props) > 0 {
			pkLen += len(props)
		} else {
			pkLen++
		}
	}

	w.WriteByte(mqttPacketPub | flags)
	w.WriteVarInt(pkLen)
	w.WriteBytes(topic)
	if qos > 0 {
		w.WriteUint16(pi)
	}
	if v5 {
		if len(props) > 0 {
			w.Write(props)
		} else {
			w.WriteByte(0)
		}
	}

	return flags
}

// Serializes to the given writer the message for the given subject. For v5
// receivers it appends the properties section (props, if any, else empty).
func mqttMakePublishHeader(pi uint16, qos byte, dup, retained, v5 bool, topic, props []byte, msgLen int) (byte, []byte) {
	headerBuf := newMQTTWriter(mqttInitialPubHeader + len(topic) + len(props))
	flags := headerBuf.WritePublishHeader(pi, qos, dup, retained, v5, topic, props, msgLen)
	return flags, headerBuf.Bytes()
}

// mqttMakeAliasedPublishHeader frames an outbound v5 PUBLISH applying this
// connection's outbound Topic Alias table, returning the flags, header, the
// alias used (0 = none/full topic), and ok=false when the packet must be dropped
// because even the plain full-topic frame exceeds the client's Maximum Packet
// Size. cc.mu must be held: the table read/write is atomic with the caller's
// queueOutbound so a binding frame is buffered before any alias-only frame that
// references it, and a fresh alias is committed only once its binding frame has
// passed the size check. Aliasing never causes an extra drop — the plain frame
// is always the fallback. Spec5 [3.3.2.3.4].
func (cc *client) mqttMakeAliasedPublishHeader(pi uint16, qos byte, dup, retain bool, topic, props []byte, msgLen int) (byte, []byte, uint16, bool) {
	mp := int(cc.mqtt.maxPacketSize)
	fits := func(h []byte) bool { return mp == 0 || len(h)+msgLen <= mp }

	// Already bound: send an empty topic + the alias. The alias-only frame can be
	// larger than the plain frame for a 1-2 byte topic (+3 alias bytes, -len(topic)
	// topic bytes), so it too is size-checked; on overflow fall through to the
	// plain frame while keeping the binding.
	if a, bound := cc.mqtt.outTopicAliases[string(topic)]; bound {
		if ap, ok := mqttInjectTopicAlias(props, a); ok {
			flags, h := mqttMakePublishHeader(pi, qos, dup, retain, true, nil, ap, msgLen)
			if fits(h) {
				return flags, h, a, true
			}
		}
	} else if len(cc.mqtt.outTopicAliases) < int(cc.mqtt.outTopicAliasMax) {
		// Unbound with room: bind the next alias (FCFS, table size + 1, never 0 and
		// never above the client's max since outTopicAliasMax = min(client, server)).
		// The binding frame carries the full topic; commit only if it fits.
		a := uint16(len(cc.mqtt.outTopicAliases) + 1)
		if ap, ok := mqttInjectTopicAlias(props, a); ok {
			flags, h := mqttMakePublishHeader(pi, qos, dup, retain, true, topic, ap, msgLen)
			if fits(h) {
				if cc.mqtt.outTopicAliases == nil {
					cc.mqtt.outTopicAliases = make(map[string]uint16)
				}
				cc.mqtt.outTopicAliases[string(topic)] = a
				return flags, h, a, true
			}
		}
	}

	// Table full, injection failed, or an aliased frame did not fit: send the plain
	// full-topic frame. alias is 0 so the trace does not claim an alias was sent.
	flags, h := mqttMakePublishHeader(pi, qos, dup, retain, true, topic, props, msgLen)
	return flags, h, 0, fits(h)
}

// Process the SUBSCRIBE packet.
//
// Process the list of subscriptions and update the given filter
// with the QoS that has been accepted (or failure).
//
// Spec [MQTT-3.8.4-3] says that if an exact same subscription is
// found, it needs to be replaced with the new one (possibly updating
// the qos) and that the flow of publications must not be interrupted,
// which I read as the replacement cannot be a "remove then add" if there
// is a chance that in between the 2 actions, published messages
// would be "lost" because there would not be any matching subscription.
//
// Run from client's readLoop.
// No lock held on entry.
// mqttResolveAfterPacket resolves an ambiguous record write left by the packet that
// just finished, once its record transaction and session lock are both released.
//
// It cannot be done from inside the packet: resolution holds the record fence for
// the whole decide-clear-reconcile transition, so an attempt made while the packet
// still holds its own transaction is simply refused — and blocking instead would
// deadlock the readLoop against itself. Doing it here is what keeps a connected
// client from sitting on a session that can no longer persist anything, since every
// later save is refused while it is unresolved.
//
// Runs from the client's readLoop, with no transaction held.
func (c *client) mqttResolveAfterPacket() {
	c.mu.Lock()
	sess := c.mqtt.sess
	c.mu.Unlock()
	if sess == nil || !sess.isSeqUncertain() {
		return
	}
	sess.resolveUncertainNow(c)
}

// mqttFlushVoidedRegs persists a void-register verdict the packet that just finished may
// have left un-recorded.
//
// It belongs here for the same reason mqttResolveAfterPacket does: an UNSUBSCRIBE that
// cancels a register-pending delivery makes the delivery goroutine own the verdict, and that
// goroutine is refused the fence for as long as the UNSUBSCRIBE holds it — while the
// UNSUBSCRIBE's own record write may already have been marshalled without the entry. This is
// the first moment the fence is free again, so it is where the verdict becomes durable
// rather than waiting for the session's next register store or resume.
//
// Runs from the client's readLoop, with no transaction held.
func (c *client) mqttFlushVoidedRegs() {
	c.mu.Lock()
	sess := c.mqtt.sess
	c.mu.Unlock()
	if sess == nil {
		return
	}
	sess.persistVoidedRegs(c)
}

func (c *client) mqttProcessSubs(filters []*mqttFilter) ([]*subscription, error) {
	// Those things are immutable, but since processing subs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	trace := c.trace
	c.mu.Unlock()

	if err := asm.lockSession(sess, c); err != nil {
		return nil, err
	}
	defer asm.unlockSession(sess)

	// The whole SUBSCRIBE is one record transaction: its membership changes, its
	// per-filter rollbacks and its final persist either all become visible or none
	// do. Outermost readLoop entry point, so it takes the blocking form.
	tok := sess.beginRecordTx()
	defer func() {
		if err := sess.endRecordTx(tok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
	}()
	return asm.processSubs(sess, c, tok, filters, true, trace)
}

// Cleanup that is performed in processSubs if there was an error.
//
// Runs from client's readLoop.
// Lock not held on entry, but session is in the locked map.
func (sess *mqttSession) cleanupFailedSub(c *client, tok recordTx, sub *subscription, cc *ConsumerConfig, jssub *subscription) {
	if sub != nil {
		c.processUnsub(sub.sid)
	}
	if jssub != nil {
		c.processUnsub(jssub.sid)
	}
	if cc != nil {
		// A shared group consumer must never be deleted outright on a local
		// failure — other members may hold it. Route through the last-holder
		// release (the caller has already rolled back this session's membership).
		// Spec5 [4.8.2].
		if cc.DeliverGroup != _EMPTY_ {
			sess.releaseSharedConsumer(c, tok, cc, nil)
		} else {
			sess.deleteConsumer(cc)
		}
	}
}

// Make sure we are set up to deliver PUBREL messages to this QoS2-subscribed
// session.
func (sess *mqttSession) ensurePubRelConsumerSubscription(c *client) error {

	sess.mu.Lock()
	pubRelSubscribed := sess.pubRelSubscribed
	pubRelDeliverySubjectB := sess.pubRelDeliverySubjectB
	pubRelDeliverySubject := sess.pubRelDeliverySubject
	pubRelConsumer := sess.pubRelConsumer
	pubRelVerified := sess.pubRelVerified
	sess.mu.Unlock()

	// Subscribe before the consumer is created so we don't loose any messages.
	if !pubRelSubscribed {
		_, err := c.processSub(pubRelDeliverySubjectB, nil, pubRelDeliverySubjectB, mqttDeliverPubRelCb, false)
		if err != nil {
			c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", pubRelDeliverySubject, err)
			return err
		}
		sess.mu.Lock()
		sess.pubRelSubscribed = true
		sess.mu.Unlock()
	}

	// A non-nil config proves only that a consumer once existed. It may have been
	// deleted out of band, or reaped by the very InactiveThreshold set below — and
	// because $MQTT_out is interest-retained, a session in that state stores PUBRELs
	// that are silently dropped and never delivers a PUBREL again. So the durable is
	// VERIFIED once per connection, with the same semantics as q2AffReady: verify,
	// create when absent (charging tmaxack only then), and fail closed when it
	// cannot be verified. Every restore starts pubRelVerified false.
	if pubRelConsumer != nil && pubRelVerified {
		return nil
	}
	if pubRelConsumer != nil {
		ci, lerr := sess.jsa.lookupConsumer(mqttOutStreamName, pubRelConsumer.Durable)
		if lerr != nil && isErrorOtherThan(lerr, JSConsumerNotFoundErr) {
			// Indeterminate: do NOT assume it is there and do not recreate over it.
			c.Errorf("Unable to verify the PUBREL consumer for client %q: %v", sess.id, lerr)
			return lerr
		}
		if ci != nil {
			sess.mu.Lock()
			sess.pubRelVerified = true
			sess.mu.Unlock()
			return nil
		}
		// Verified ABSENT: fall through and recreate it. It is charged below like any
		// other create, and the deterministic name means the recreate is the same
		// consumer this session's record already refers to.
		c.Warnf("MQTT: recreating the missing PUBREL consumer for client %q", sess.id)
	}

	opts := c.srv.getOpts()
	ackWait := opts.MQTT.AckWait
	if ackWait == 0 {
		ackWait = mqttDefaultAckWait
	}
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}

	sess.mu.Lock()
	pubRelSubject := sess.pubRelSubject
	// The durable is generation-scoped along with the two subjects, so a teardown
	// issued by a retired incarnation cannot delete this one. See
	// sess_incarnation.go.
	_, _, pubRelDurable := mqttPubRelNames(sess.idHash, sess.gen)
	tmaxack := sess.tmaxack
	id := sess.id
	sess.mu.Unlock()

	// Check that the limit of subs' maxAckPending are not going over the limit
	if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
		return fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
			after, mqttMaxAckTotalLimit)
	}

	ccr := &CreateConsumerRequest{
		Stream: mqttOutStreamName,
		Config: ConsumerConfig{
			DeliverSubject: pubRelDeliverySubject,
			Durable:        pubRelDurable,
			AckPolicy:      AckExplicit,
			DeliverPolicy:  DeliverNew,
			FilterSubject:  pubRelSubject,
			AckWait:        ackWait,
			MaxAckPending:  maxAckPending,
			MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
		},
	}
	if opts.MQTT.ConsumerInactiveThreshold > 0 {
		ccr.Config.InactiveThreshold = opts.MQTT.ConsumerInactiveThreshold
	}
	if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
		c.Errorf("Unable to add JetStream consumer for PUBREL for client %q: err=%v", id, err)
		return err
	}
	pubRelConsumer = &ccr.Config

	sess.mu.Lock()
	sess.pubRelConsumer = pubRelConsumer
	// Charged only where it was created, never on a restore — the same rule the
	// plain and shared consumers follow. See refundConsumerChargeLocked.
	sess.chargeConsumerLocked(pubRelDurable, maxAckPending)
	sess.pubRelVerified = true
	sess.mu.Unlock()

	return nil
}

// When invoked with a QoS of 0, looks for an existing JS durable consumer for
// the given sid and if one is found, delete the JS durable consumer and unsub
// the NATS subscription on the delivery subject.
//
// With a QoS > 0, creates or update the existing JS durable consumer along with
// its NATS subscription on a delivery subject.
//
// Session lock is acquired and released as needed. Session is in the locked
// map.
func (sess *mqttSession) processJSConsumer(c *client, tok recordTx, subject, sid string,
	qos byte, noLocal, retainAsPublished bool, subID int, fromSubProto bool) (*ConsumerConfig, *subscription, *ConsumerConfig, error) {

	sess.mu.Lock()
	// A re-SUBSCRIBE to a filter that is DRAINING reclaims that drain rather than
	// creating anything. Because of I7 the drain is invisible in cons, so without
	// this the code below would create a SECOND durable on the same filter and
	// deliver every new message twice. It must therefore run before the cons
	// lookup. A QoS 0 re-SUBSCRIBE deliberately does not adopt: the drain keeps
	// draining independently while the raw subscription delivers new messages at
	// QoS 0. See unsub_drain.go.
	if qos > 0 {
		sess.adoptDrainLocked(sid)
	}
	cc, exists := sess.cons[sid]
	tmaxack := sess.tmaxack
	idHash := sess.idHash
	rmax := sess.rmax
	sess.mu.Unlock()

	// Check if we are already a JS consumer for this SID.
	if exists {
		// If current QoS is 0, it means that we need to delete the existing
		// one (that was QoS > 0)
		if qos == 0 {
			// When processing a SUBSCRIBE packet, DEFER the whole teardown
			// (including removing the sess.cons entry) until the packet's final
			// session save is confirmed: the save can still fail and its rollback
			// must restore the pre-packet QoS1/2 state, and any save issued
			// mid-packet (a later shared filter's early persist) must serialize a
			// record consistent with reality — Subs still says QoS1/2 until the
			// final update, so the consumer entry must stay alongside it. The
			// caller executes execDeferredConsumerDelete after the confirmed save.
			// Spec5 [4.8.2].
			if fromSubProto {
				return nil, nil, cc, nil
			}
			sess.execDeferredConsumerDelete(c, tok, sid, cc)
			return nil, nil, nil, nil
		}
		// If this is called when processing SUBSCRIBE protocol, then if
		// the JS consumer already exists, we are done (it was created
		// during the processing of CONNECT).
		if fromSubProto {
			// A re-SUBSCRIBE to an existing QoS 1/2 filter can change the QoS, the
			// No Local option, and the Subscription Identifier, all read by the QoS
			// 1/2 delivery callback off the delivery subscription. The durable
			// consumer itself is unchanged, so refresh those fields here so the new
			// values take effect. Spec5 [MQTT-3.8.4-3], [3.8.3.1], [3.8.2.1.2].
			c.mu.Lock()
			sub := c.subs[cc.DeliverSubject]
			c.mu.Unlock()
			if sub != nil {
				// The QoS 1/2 callback reads these under sess.mu; the QoS0
				// callback (on the shadow sub) under subsMu. Write under both, as
				// processSub does, so both readers see a consistent value.
				sess.mu.Lock()
				sess.subsMu.Lock()
				if sub.mqtt != nil {
					sub.mqtt.qos = qos
					sub.mqtt.noLocal = noLocal
					sub.mqtt.retainAsPublished = retainAsPublished
					sub.mqtt.subID = subID
				}
				sess.subsMu.Unlock()
				sess.mu.Unlock()
			}
			return nil, nil, nil, nil
		}
		// CONNECT restore of an entry the resume reconciliation phase already
		// verified (or repaired): its delivery subscription was installed CLOSED
		// and phase-owned, and it is adopted IN PLACE — metadata refreshed and the
		// gate opened under the same dual-lock write the re-SUBSCRIBE refresh
		// uses, on the SAME subscription object. Re-issuing it through processSub
		// would unsubscribe the old subscription and install a new one, putting a
		// no-interest gap right back into the flow — the exact stream-forced
		// inactivity race the phase's subscribe-first rule closes. Deliveries that
		// arrived while closed were skipped unacked; AckWait redelivers them now.
		// See sess_resume_reconcile.go.
		c.mu.Lock()
		phaseSub := c.subs[cc.DeliverSubject]
		c.mu.Unlock()
		if phaseSub != nil {
			adopted := false
			sess.mu.Lock()
			sess.subsMu.Lock()
			if phaseSub.mqtt != nil && phaseSub.mqtt.phaseOwned {
				phaseSub.mqtt.qos = qos
				phaseSub.mqtt.noLocal = noLocal
				phaseSub.mqtt.retainAsPublished = retainAsPublished
				phaseSub.mqtt.subID = subID
				phaseSub.mqtt.closed = false
				phaseSub.mqtt.phaseOwned = false
				// Released LAST, in the same critical section: a delivery waiting
				// on the gate re-reads every field above once it wakes, so the
				// backlog JetStream pushed before the CONNACK is delivered as the
				// first transmission it is, in order, with this filter's live
				// options. See mqttSub.phaseGate.
				mqttReleasePhaseGateLocked(phaseSub)
				adopted = true
			}
			sess.subsMu.Unlock()
			sess.mu.Unlock()
			if adopted {
				return cc, phaseSub, nil, nil
			}
		}
	}
	// Here it means we don't have a JS consumer and if we are QoS 0,
	// we have nothing to do.
	if qos == 0 {
		return nil, nil, nil, nil
	}
	var err error
	var inbox string
	// charged is the MaxAckPending this call adds to tmaxack, recorded against the
	// durable so the refund is exactly this and happens exactly once.
	var charged int
	if exists {
		inbox = cc.DeliverSubject
	} else {
		inbox = mqttSubPrefix + nuid.Next()
		opts := c.srv.getOpts()
		ackWait := opts.MQTT.AckWait
		if ackWait == 0 {
			ackWait = mqttDefaultAckWait
		}
		maxAckPending := int(opts.MQTT.MaxAckPending)
		if maxAckPending == 0 {
			maxAckPending = mqttDefaultMaxAckPending
		}
		// Pace JetStream delivery to the client's Receive Maximum so the
		// in-flight gate rarely drops at the cap (a drop waits out AckWait and
		// reorders). The gate stays the hard bound across subscriptions; a
		// consumer that outlives this connection keeps this cap until deleted.
		if rmax != 0 && int(rmax) < maxAckPending {
			maxAckPending = int(rmax)
		}

		// Check that the limit of subs' maxAckPending are not going over the limit
		if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			return nil, nil, nil, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
				after, mqttMaxAckTotalLimit)
		}

		durName := idHash + "_" + nuid.Next()
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{
				DeliverSubject: inbox,
				Durable:        durName,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  mqttStreamSubjectPrefix + subject,
				AckWait:        ackWait,
				MaxAckPending:  maxAckPending,
				MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			},
		}
		if opts.MQTT.ConsumerInactiveThreshold > 0 {
			ccr.Config.InactiveThreshold = opts.MQTT.ConsumerInactiveThreshold
		}
		if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
			c.Errorf("Unable to add JetStream consumer for subscription on %q: err=%v", subject, err)
			return nil, nil, nil, err
		}
		cc = &ccr.Config
		// Record the charge against this durable so its teardown refunds exactly
		// this, exactly once. A consumer restored from the record reaches
		// processQOS12Sub below without passing here and is therefore never charged —
		// and must never be refunded either. See refundConsumerChargeLocked.
		charged = maxAckPending
	}

	// This is an internal subscription on subject like "$MQTT.sub.<nuid>" that is setup
	// for the JS durable's deliver subject.
	sess.mu.Lock()
	if charged > 0 {
		sess.chargeConsumerLocked(cc.Durable, charged)
	}
	sub, err := sess.processQOS12Sub(c, []byte(inbox), []byte(inbox), nil,
		isMQTTReservedSubscription(subject), qos, noLocal, retainAsPublished, false, subID, cc.Durable, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()

	if err != nil {
		sess.deleteConsumer(cc)
		c.Errorf("Unable to create subscription for JetStream consumer on %q: %v", subject, err)
		return nil, nil, nil, err
	}
	return cc, sub, nil, nil
}

// execDeferredConsumerDelete performs the irreversible teardown of a plain
// QoS1/2 consumer whose session entry was already removed (QoS downgrade to 0):
// mark its delivery subscription closed, purge its pending QoS 1/2 deliveries
// (so the packet identifiers do not leak against the in-flight cap), delete the
// durable, and unsubscribe the delivery subject. deleteConsumer is asynchronous,
// so an in-flight delivery callback could re-populate the maps after the purge;
// the closed flag (guarded by sess.mu and sess.subsMu, same as delivery) makes
// mqttDeliverMsgCbQoS12 skip instead while leaving sub.mqtt valid for an
// already-committed enqueue.
func (sess *mqttSession) execDeferredConsumerDelete(c *client, tok recordTx, sid string, cc *ConsumerConfig) {
	c.mu.Lock()
	sub := c.subs[cc.DeliverSubject]
	c.mu.Unlock()

	sess.mu.Lock()
	// The entry stays in sess.cons until this irreversible teardown actually
	// runs, so any save issued in between persists a record that still
	// references the (still-existing) durable. Remove it only when present and
	// still pointing at this consumer (a rollback may have reconciled the map).
	if cur, ok := sess.cons[sid]; ok && cur == cc {
		delete(sess.cons, sid)
	}
	if sub != nil && sub.mqtt != nil {
		sess.subsMu.Lock()
		sub.mqtt.closed = true
		sess.subsMu.Unlock()
	}
	regs, cancels := sess.purgeConsumerPendingLocked(cc.Durable)
	sess.mu.Unlock()
	sess.deleteInflightRegisters(c, tok, regs)

	// Anything the purge found here arrived AFTER this packet staged its cancellations
	// (a delivery already in the callback when the durable's subscription was closed),
	// so its cancellation missed the membership-removal write. Keeping the consumer
	// would not redeem that: the membership is gone either way, so a register that lands
	// has nothing to redeliver through whether the durable lives or not — and an
	// unreferenced durable nothing reaps is worse than none. So it is reported and the
	// obligation is left to the convergence points.
	if !sess.secureRegCancellations(c, tok, cancels) {
		c.Errorf("MQTT: the cancelled in-flight registration(s) of consumer %q for client %q "+
			"are not durably recorded yet; their identifiers stay reserved until the cleanup "+
			"is discharged", cc.Durable, sess.id)
	}
	sess.deleteConsumer(cc)
	if sub != nil {
		c.processUnsub(sub.sid)
	}
}

// processSharedJSConsumer creates (or joins) the single JetStream consumer that
// backs an MQTT 5.0 Shared Subscription group for a (ShareName, subject) pair,
// and subscribes this session to its deliver subject as a queue member so
// JetStream delivers each stored message to exactly one connected member.
//
// The durable, deliver subject, and queue/DeliverGroup are all deterministic
// (mqttSharedNames), so members across the cluster converge on one consumer. All
// members join regardless of granted QoS: a QoS0 member still receives stored
// (QoS 1/2 published) messages through this consumer, delivered at QoS0 (the raw
// subject queue sub only carries QoS0-published messages). The consumer config
// is derived from server options only — never the member's Receive Maximum —
// since CreateOrUpdate would let each joiner rewrite the shared config. No
// InactiveThreshold is set, so an all-offline group stalls and keeps its backlog
// rather than being reaped. On the subscribe failure path the consumer is
// returned so the caller's cleanup can release it. Spec5 [4.8.2].
//
// Runs from the client's readLoop. sess.mu not held on entry.
func (sess *mqttSession) processSharedJSConsumer(c *client, group, subject, sid string,
	qos byte, retainAsPublished bool, subID int, fromSubProto bool) (*ConsumerConfig, *subscription, error) {

	sess.mu.Lock()
	cc, exists := sess.cons[sid]
	tmaxack := sess.tmaxack
	sess.mu.Unlock()

	queue, deliver, durable := mqttSharedNames(group, subject)

	// Already a member with a live deliver subscription (re-SUBSCRIBE): just
	// refresh the mutable per-subscription fields read by the delivery callback.
	if exists {
		c.mu.Lock()
		sub := c.subs[cc.DeliverSubject]
		c.mu.Unlock()
		if sub != nil {
			sess.mu.Lock()
			sess.subsMu.Lock()
			if sub.mqtt != nil {
				sub.mqtt.qos = qos
				sub.mqtt.retainAsPublished = retainAsPublished
				sub.mqtt.subID = subID
			}
			sess.subsMu.Unlock()
			sess.mu.Unlock()
			return cc, sub, nil
		}
		// exists but no live deliver sub (session resume on a new connection):
		// fall through to (re)subscribe without recreating or recharging.
	}

	opts := c.srv.getOpts()
	maxAckPending := int(opts.MQTT.MaxAckPending)
	if maxAckPending == 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	var charged bool
	if !exists {
		// A stream-level consumer InactiveThreshold would give the group durable
		// a delete timer, reaping it while all members are offline. Check the
		// LIVE stream config (it can change after startup) and refuse rather
		// than create a reapable shared consumer. FAIL CLOSED: if the lookup
		// itself fails we cannot prove the limit is absent, so refuse the join
		// (the client can retry) instead of risking a reapable durable. Spec5 [4.8.2].
		si, err := sess.jsa.lookupStream(mqttStreamName)
		if err != nil || si == nil {
			return nil, nil, fmt.Errorf("shared subscriptions unavailable: unable to verify messages stream consumer limits: %v", err)
		}
		if si.Config.ConsumerLimits.InactiveThreshold != 0 {
			return nil, nil, fmt.Errorf("shared subscriptions unavailable: messages stream imposes a consumer inactivity limit")
		}
		ackWait := opts.MQTT.AckWait
		if ackWait == 0 {
			ackWait = mqttDefaultAckWait
		}
		if after := tmaxack + maxAckPending; after > mqttMaxAckTotalLimit {
			return nil, nil, fmt.Errorf("max_ack_pending for all consumers would be %v which exceeds the limit of %v",
				after, mqttMaxAckTotalLimit)
		}
		ccr := &CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{
				DeliverSubject: deliver,
				DeliverGroup:   queue,
				Durable:        durable,
				AckPolicy:      AckExplicit,
				DeliverPolicy:  DeliverNew,
				FilterSubject:  mqttStreamSubjectPrefix + subject,
				AckWait:        ackWait,
				MaxAckPending:  maxAckPending,
				MemoryStorage:  opts.MQTT.ConsumerMemoryStorage,
			},
		}
		// Create before subscribing: a pre-existing group durable must not deliver
		// to (or be ack-consumed by) this member until the join is proven. Create
		// is idempotent (ActionCreateOrUpdate). Spec5 [4.8.2].
		if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
			c.Errorf("Unable to add shared JetStream consumer for subscription on %q: err=%v", subject, err)
			return nil, nil, err
		}
		cc = &ccr.Config
		sess.mu.Lock()
		sess.tmaxack = tmaxack + maxAckPending
		if sess.sharedCharged == nil {
			sess.sharedCharged = make(map[string]int)
		}
		sess.sharedCharged[durable] = maxAckPending
		sess.mu.Unlock()
		charged = true
	}

	sess.mu.Lock()
	sub, err := sess.processQOS12Sub(c, []byte(deliver), []byte(deliver), []byte(queue),
		isMQTTReservedSubscription(subject), qos, false, retainAsPublished, true, subID, durable, mqttDeliverMsgCbQoS12)
	sess.mu.Unlock()
	if err != nil {
		c.Errorf("Unable to create subscription for shared JetStream consumer on %q: %v", subject, err)
		// Return cc (only when we created it this call) so the caller's cleanup,
		// after rolling back the early-persisted membership, can release it.
		if charged {
			return cc, nil, err
		}
		return nil, nil, err
	}
	// Verify/repair the join: a concurrent leaver whose last-holder scan finished
	// before this session's membership was persisted may have issued a (queued,
	// fire-and-forget) delete of the group durable that landed AFTER our create.
	// Re-issue the idempotent CreateOrUpdate now that our membership is persisted
	// and the queue interest is up: it recreates the durable if it was deleted and
	// is a no-op otherwise, closing the join/leave race window. Spec5 [4.8.2].
	if charged {
		ccr := &CreateConsumerRequest{Stream: mqttStreamName, Config: *cc}
		if _, err := sess.jsa.createDurableConsumer(ccr); err != nil {
			c.Warnf("Unable to verify shared JetStream consumer for subscription on %q: %v", subject, err)
		}
	}
	return cc, sub, nil
}

// releaseSharedConsumer removes this session's local delivery subscription and
// pending state for a shared-subscription group consumer, and deletes the group
// durable only if no other session still references it. Pending QoS 1/2
// deliveries are purged WITHOUT acking, so any in-flight messages redeliver to
// the remaining group members via AckWait. The caller must have already removed
// the leaving filter from sess.sharedSubs so the last-holder scan does not count
// it. Spec5 [4.8.2].
//
// Runs from the client's readLoop. sess.mu not held on entry.
func (sess *mqttSession) releaseSharedConsumer(c *client, tok recordTx, cc *ConsumerConfig, held map[string]bool) {
	if cc == nil || cc.DeliverGroup == _EMPTY_ {
		return
	}
	var sub *subscription
	if c != nil {
		c.mu.Lock()
		sub = c.subs[cc.DeliverSubject]
		c.mu.Unlock()
	}
	sess.mu.Lock()
	if sub != nil && sub.mqtt != nil {
		sess.subsMu.Lock()
		sub.mqtt.closed = true
		sess.subsMu.Unlock()
	}
	regs, cancels := sess.purgeConsumerPendingLocked(cc.Durable)
	if amt, ok := sess.sharedCharged[cc.Durable]; ok {
		sess.tmaxack -= amt
		if sess.tmaxack < 0 {
			sess.tmaxack = 0
		}
		delete(sess.sharedCharged, cc.Durable)
	}
	sess.mu.Unlock()
	sess.deleteInflightRegisters(c, tok, regs)
	// As in execDeferredConsumerDelete: anything found here arrived after the staging,
	// and keeping the durable would not make its identifier recoverable.
	if !sess.secureRegCancellations(c, tok, cancels) && c != nil {
		c.Errorf("MQTT: the cancelled in-flight registration(s) of shared consumer %q for client %q "+
			"are not durably recorded yet; their identifiers stay reserved until the cleanup "+
			"is discharged", cc.Durable, sess.id)
	}
	if c != nil && sub != nil {
		c.processUnsub(sub.sid)
	}

	// Delete the group durable only if no other session still references it. A
	// caller releasing several durables in one operation passes one precomputed
	// scan result (held); nil means scan for this durable alone.
	if held == nil {
		held = sess.sharedDurablesHeldByOthers([]string{cc.Durable})
	}
	if !held[cc.Durable] {
		if _, err := sess.jsa.deleteConsumer(mqttStreamName, cc.Durable, true); isErrorOtherThan(err, JSConsumerNotFoundErr) {
			c.Errorf("Unable to delete shared consumer %q: %v", cc.Durable, err)
		}
	}
}

// purgeSharedPending drops this session's pending QoS 1/2 delivery state for its
// shared group consumers WITHOUT acking. Called when the session survives a
// disconnect, so a resume does not NAK-steal group messages (which AckWait
// redelivers to other members) and so the packet identifiers are freed. Spec5 [4.8.2].
//
// The targets are derived from the pending state itself, not from either view of the
// group membership, because both views provably miss real shapes. A session restored
// from its record has membership without configs — shared configs are excluded from
// the persisted Cons (buildPersistedSessionLocked) and rebuilt only by processSubs —
// while recoverInflightRegisters has already rebuilt its pending entries under the
// deterministic group durable names, so a cons walk purges nothing. A SUBSCRIBE whose
// rolled-back save failed leaves a config outside the membership (rollbackSubscribePacket's
// readd restores cons, not sharedSubs), so a sharedSubs walk misses it. A 3.1.1 forced
// leave and UNSUBSCRIBE's no-cons branch each strand transmitted entries with NEITHER
// view left, which is why even the union is not enough.
//
// For broker-minted state the durable name IS the protocol fact "deliveries under this
// consumer are group deliveries": mqttSharedNames is the single source of the prefix, and
// no other class can collide (plain consumers are idHash_nuid, PUBREL and affine QoS 2
// copies carry their own prefixes, and a drained consumer keeps its plain name). For
// state restored from a register the name is trusted, the same trust every other restored
// identity gets.
func (sess *mqttSession) purgeSharedPending() {
	regs, cancels := sess.collectSharedPendingPurge()
	// No consumer is destroyed here — the group durable survives — but the identifiers
	// are freed, so a register that lands afterwards would have the next resume reserve
	// one the session has already given up.
	sess.secureRegCancellations(nil, recordTx{}, cancels)
	sess.deleteInflightRegisters(nil, recordTx{}, regs)
}

// purgeSharedPendingStaged is purgeSharedPending for a caller that owns the record
// write: the in-memory purge and the register-stream rungs run exactly as they do for
// the ordinary close, but a fallback verdict is only STAGED in memory — no record write
// happens here — and the caller's own classified save carries it. It reports whether any
// verdict is so owed.
//
// The deferred close is that caller, and the split is what makes its write ONE
// operation: a purge that saved for itself could land ambiguously and leave the
// caller's own save refused, with its parked close obligations already consumed. See
// applyDeferredDisconnect.
//
// Session lock NOT held on entry: the rungs issue JetStream requests.
func (sess *mqttSession) purgeSharedPendingStaged(c *client) bool {
	regs, cancels := sess.collectSharedPendingPurge()
	owed := sess.tombstoneCancellationsOnStream(c, cancels)
	if sess.voidRegistersOnStream(c, regs) {
		owed = true
	}
	return owed
}

// collectSharedPendingPurge drops the pending state in memory and reports what the
// register stream still owes for it: the registers to delete and the cancellations to
// tombstone. It is the derivation both purge forms share, and the rule it encodes is
// the load-bearing part — see purgeSharedPending.
//
// Session lock NOT held on entry.
func (sess *mqttSession) collectSharedPendingPurge() ([]mqttVoidedReg, []mqttRegCancellation) {
	sess.mu.Lock()
	var regs []mqttVoidedReg
	var cancels []mqttRegCancellation
	purged := make(map[string]struct{})
	purge := func(durable string) {
		if !strings.HasPrefix(durable, mqttSharedConsumerDurablePrefix) {
			return
		}
		if _, dup := purged[durable]; dup {
			return
		}
		purged[durable] = struct{}{}
		pregs, pcancels := sess.purgeConsumerPendingLocked(durable)
		regs = append(regs, pregs...)
		cancels = append(cancels, pcancels...)
	}
	// Every map purgeConsumerPendingLocked removes from is a source of targets, so no
	// pending entry it would drop is out of the collection's reach. A retirement has no
	// cpending entry by construction, and a withheld first transmission may outlive the
	// emptying of its consumer's cpending bucket.
	for durable := range sess.cpending {
		purge(durable)
	}
	for k := range sess.deferred {
		purge(k.jsDur)
	}
	for key := range sess.srcAckOwed {
		purge(key.jsDur)
	}
	sess.mu.Unlock()
	return regs, cancels
}

// sharedConsumerHasOtherHolders reports whether any session still holds a shared
// subscription that maps to the given group durable. See sharedDurablesHeldByOthers.
func (sess *mqttSession) sharedConsumerHasOtherHolders(durable string) bool {
	return sess.sharedDurablesHeldByOthers([]string{durable})[durable]
}

// sharedDurablesHeldByOthers returns which of the given group durables some
// session still holds: this session's own remaining in-memory shared
// subscriptions, or any other session's persisted record (which covers the
// cluster). One record walk answers for all durables, so a caller releasing
// several in one operation (clear, forced leave, UNSUBSCRIBE) pays a single
// scan. Spec5 [4.8.2].
func (sess *mqttSession) sharedDurablesHeldByOthers(durables []string) map[string]bool {
	held := make(map[string]bool, len(durables))
	uniq := make([]string, 0, len(durables))
	for _, d := range durables {
		if _, ok := held[d]; !ok {
			held[d] = false
			uniq = append(uniq, d)
		}
	}
	pending := len(uniq)
	markOwned := func(key string) {
		for _, d := range uniq {
			if !held[d] && mqttSharedKeyOwnsDurable(key, d) {
				held[d] = true
				pending--
			}
		}
	}

	sess.mu.Lock()
	for key := range sess.sharedSubs {
		markOwned(key)
	}
	sess.mu.Unlock()

	filter := mqttSessStreamSubjectPrefix + sess.domainTk + ">"
	for seq := uint64(1); pending > 0; {
		smsg, err := sess.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			// Only "no more messages" means the scan definitively found no other
			// holder. Any other error (JS API timeout, meta-leader failover) is
			// indeterminate — FAIL CLOSED and report every durable held, so the
			// caller never deletes a group durable other members may still hold.
			// Spec5 [4.8.2].
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				for _, d := range uniq {
					held[d] = true
				}
			}
			break
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if err := json.Unmarshal(smsg.Data, ps); err != nil {
			continue
		}
		// This session's own record is covered by the in-memory check above; it
		// may also be stale relative to a just-completed remove.
		if ps.ID == sess.id {
			continue
		}
		for key := range ps.SharedSubs {
			markOwned(key)
		}
	}
	return held
}

// mqttSharedDurablesForKey returns the group durable name(s) a shared
// subscription session key maps to: the main durable, plus the level-up
// durable for a "foo.>" filter ('#' semantics). Nil if the key does not parse.
func mqttSharedDurablesForKey(key string) []string {
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		return nil
	}
	_, _, dur := mqttSharedNames(group, subject)
	durs := []string{dur}
	if mqttNeedSubForLevelUp(subject) {
		_, _, ldur := mqttSharedNames(group, subject[:len(subject)-2])
		durs = append(durs, ldur)
	}
	return durs
}

// mqttParseSharedKey splits a shared-subscription session key
// ("$share/{ShareName}/{NATS subject}") back into its ShareName and subject.
func mqttParseSharedKey(key string) (group, subject string, ok bool) {
	if !strings.HasPrefix(key, string(mqttSharedSubPrefix)) {
		return _EMPTY_, _EMPTY_, false
	}
	rest := key[len(mqttSharedSubPrefix):]
	idx := strings.IndexByte(rest, mqttTopicLevelSep)
	if idx <= 0 || idx == len(rest)-1 {
		return _EMPTY_, _EMPTY_, false
	}
	return rest[:idx], rest[idx+1:], true
}

// mqttSharedKeyOwnsDurable reports whether a session holding the given shared
// subscription key owns the given group durable. A "foo.>" filter owns both its
// own durable and the level-up ("foo") durable created for '#' semantics.
func mqttSharedKeyOwnsDurable(key, durable string) bool {
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		return false
	}
	if _, _, d := mqttSharedNames(group, subject); d == durable {
		return true
	}
	if mqttNeedSubForLevelUp(subject) {
		if _, _, d := mqttSharedNames(group, subject[:len(subject)-2]); d == durable {
			return true
		}
	}
	return false
}

// Queues the published retained messages for each subscription and signals
// the writeLoop.
func (c *client) mqttSendRetainedMsgsToNewSubs(subs []*subscription) {
	c.mu.Lock()
	for _, sub := range subs {
		if sub.mqtt != nil && sub.mqtt.prm != nil {
			for _, data := range sub.mqtt.prm {
				c.queueOutbound(data)
			}
			sub.mqtt.prm = nil
		}
	}
	c.flushSignal()
	c.mu.Unlock()
}

// mqttEnqueueSubAck enqueues a SUBACK and reports whether it was sent. A v5
// SUBACK whose reason-code payload alone exceeds the client's Maximum Packet Size
// is suppressed (returns false); the 3.1.1 path always sends.
func (c *client) mqttEnqueueSubAck(pi uint16, filters []*mqttFilter) bool {
	w := newMQTTWriter(7 + len(filters))
	w.WriteByte(mqttPacketSubAck)
	// The per-filter byte (f.qos) is a granted QoS (0/1/2) or a failure reason
	// code (0x80, or a more specific v5 code such as 0x9E); these are valid
	// SUBACK reason codes, so the payload byte is identical for both protocol
	// levels. v5 only adds a properties block before the reason codes.
	if c.mqtt.proto == mqttProtoLevel5 {
		// Carry the first failed filter's detail as the packet-level Reason
		// String, when the client permits it. Spec5 [3.9.2.2.2].
		var reasonStr string
		for _, f := range filters {
			if f.qos >= mqttSubAckFailure && f.reasonStr != _EMPTY_ {
				reasonStr = f.reasonStr
				break
			}
		}
		props := c.mqttReasonProps(mqttPacketSubAck, reasonStr)
		frame := func() []byte {
			var vh mqttWriter
			vh.WriteUint16(pi)
			vh.writeProperties(props)
			for _, f := range filters {
				vh.WriteByte(f.qos)
			}
			w.Reset()
			w.WriteByte(mqttPacketSubAck)
			w.WriteVarInt(vh.Len())
			w.Write(vh.Bytes())
			return w.Bytes()
		}
		buf := frame()
		// Drop the Reason String (only) if it overflows the client's max.
		if props != nil && !c.mqttFitsClientMax(len(buf)) {
			props = nil
			buf = frame()
		}
		// If the reason-code payload alone still overflows, the SUBACK cannot be
		// sent [MQTT-3.1.2-24]; the subscription side effects stand.
		return c.mqttEnqueueFittingProto(buf)
	}
	// packet length is 2 (for packet identifier) and 1 byte per filter.
	w.WriteVarInt(2 + len(filters))
	w.WriteUint16(pi)
	for _, f := range filters {
		w.WriteByte(f.qos)
	}
	c.mu.Lock()
	c.enqueueProto(w.Bytes())
	c.mu.Unlock()
	return true
}

//////////////////////////////////////////////////////////////////////////////
//
// UNSUBSCRIBE related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttParseUnsubs(r *mqttReader, b byte, pl int) (uint16, []*mqttFilter, error) {
	return c.mqttParseSubsOrUnsubs(r, b, pl, false)
}

// Process the UNSUBSCRIBE packet.
//
// Given the list of topics, this is going to unsubscribe the low level NATS subscriptions
// and delete the JS durable consumers when applicable.
//
// It returns the truthful-UNSUBACK restore for an AMBIGUOUS record write (nil
// otherwise), which the packet tail applies if that write is proven durable before
// it acknowledges. See mqttUnsubAckRestore.
//
// Runs from the client's readLoop.
// No lock held on entry.
func (c *client) mqttProcessUnsubs(filters []*mqttFilter) (*mqttUnsubAckRestore, error) {
	// Those things are immutable, but since processing unsubs is not
	// really in the fast path, let's get them under the client lock.
	c.mu.Lock()
	asm := c.mqtt.asm
	sess := c.mqtt.sess
	c.mu.Unlock()

	if err := asm.lockSession(sess, c); err != nil {
		return nil, err
	}
	defer asm.unlockSession(sess)

	// The whole UNSUBSCRIBE is one record transaction, opened before the first
	// membership mutation so no other writer can persist a half-applied packet.
	// Outermost readLoop entry point, so it takes the blocking form.
	packetTok := sess.beginRecordTx()
	defer func() {
		if err := sess.endRecordTx(packetTok); err != nil {
			c.Errorf("MQTT: %v", err)
		}
	}()

	// One record scan (heldShared, computed after the record save below) decides
	// every shared durable this packet may release.
	var heldShared map[string]bool
	// The record token is a PARAMETER, never the packet's: this closure is reached from
	// commitUnsub, which also runs as a LANDED CONTINUATION under the resolver's
	// transaction — and every write below (the shared release, the cancellation
	// tombstones, the register deletes) is refused at entry under a closed one. A
	// nested closure capturing the enclosing `tok` is exactly how that leak survives a
	// signature change on its caller.
	// v5 UNSUBACK reports whether each subscription existed; the session maps are
	// guarded by the session-manager lock held here. Spec5 [3.11.3].
	for _, f := range filters {
		sid := f.sessionKey()
		var ok bool
		if f.shared {
			_, ok = sess.sharedSubs[sid]
		} else {
			_, ok = sess.subs[sid]
		}
		if !ok {
			f.reason = mqttReasonNoSubscriptionExisted
			f.reasonStr = "no matching subscription existed"
		}
	}
	// UNSUBSCRIBE is transactional: persist the removals FIRST, and only tear down
	// the NATS subscriptions and (shared) consumers once the record save is
	// confirmed. Otherwise a failed save could leave the record claiming a
	// subscription whose live join and group durable were already removed.
	// Snapshot every affected map so a save failure fully rolls back in memory.
	// Spec5 [4.8.2], [3.11.3].
	snap := sess.snapshotSubs()

	// [MQTT-3.10.4-3]: a QoS 1/2 message the broker has already STARTED sending
	// must still be completed. Convert each such consumer into a DRAIN instead of
	// tearing it down — staged now so the save below carries the membership removal
	// and the drain that replaces it in one record (I4). See unsub_drain.go.
	drainedSids := make(map[string]bool)
	// Checked ONCE per packet, and before any staging: the precondition is a
	// property of the stream, not of a filter, and it must be settled before the
	// record can claim a drain. See drainingAllowed.
	mayDrain := sess.drainingAllowed(c)
	sess.mu.Lock()
	for _, f := range filters {
		if !mayDrain {
			break
		}
		if f.shared {
			continue // shared QoS 2 is already session-affine; shared QoS 1 may redistribute
		}
		for _, sid := range mqttUnsubSids(f.sessionKey()) {
			cc, ok := sess.cons[sid]
			if !ok {
				continue
			}
			c.mu.Lock()
			sub := c.subs[cc.DeliverSubject]
			c.mu.Unlock()
			if sess.stageDrain(sid, cc, sub) {
				drainedSids[sid] = true
			}
		}
	}
	stagedCancels, closedDeliveries, voidedSent := sess.stageUnsubCancellationsLocked(c, filters, drainedSids)
	sess.mu.Unlock()

	// Everything the UNSUBSCRIBE does AFTER its record write is confirmed. It is a
	// closure because the confirmation can arrive in two ways: inline, or later from
	// the resolver proving an ambiguous write durable. Skipping it in the second
	// case would leave the record saying "unsubscribed" while the runtime kept the
	// subscriptions, the consumers and the staged (uncommitted) drains — a
	// half-applied packet with nothing left to finish it.
	//
	// The record token is a PARAMETER, never the captured one: as a landed
	// continuation this runs under the RESOLVER'S transaction, and the packet's own
	// is closed by then — every write under it would be refused at entry.
	commitUnsub := func(tok recordTx) {
		// The staged cancellations are durable in the packet's own record now, so this
		// is cleanup: a tombstone lets the register go without waiting for a resume.
		sess.secureRegCancellations(c, tok, stagedCancels)
		// The leaving filters are already out of sharedSubs and persisted, so one scan
		// now decides every shared durable this packet may release; keys the session
		// still holds (e.g. "$share/g/foo" while "$share/g/foo.>" leaves) count as
		// holders. The staged drains are now durable, so fresh deliveries on their
		// filters become ack-discards rather than being left to AckWait.
		// Spec5 [4.8.2].
		sess.mu.Lock()
		timedDrains := sess.commitDrains()
		sess.mu.Unlock()
		// A drain must outlive a disconnect, so it cannot keep the per-consumer
		// inactivity timer an ordinary subscription is created with.
		sess.clearDrainInactivityTimers(c, timedDrains)

		var sharedDurs []string
		for _, f := range filters {
			if f.shared {
				sharedDurs = append(sharedDurs, mqttSharedDurablesForKey(f.sessionKey())...)
			}
		}
		if len(sharedDurs) > 0 {
			heldShared = sess.sharedDurablesHeldByOthers(sharedDurs)
		}
		for _, f := range filters {
			sid := f.sessionKey()
			for _, s := range mqttUnsubSids(sid) {
				// A DRAINED sid keeps its durable, its delivery subscription and its
				// pending deliveries — that is the whole point — so it is skipped here.
				// Only the client-facing subscription goes, because the client really did
				// unsubscribe.
				if !drainedSids[s] {
					sess.removeUnsubConsumer(c, tok, s, heldShared)
				}
				if err := c.processUnsub([]byte(s)); err != nil {
					c.Errorf("error unsubscribing from %q: %v", s, err)
				}
			}
		}
	}

	wasUncertain := sess.isSeqUncertain()
	if err := sess.update(packetTok, filters, false); err != nil {
		// Roll back the in-memory removals ONLY if nothing was stored. An ambiguous
		// write may already carry them, and the resolver settles it forward by
		// re-attempting exactly those bytes — restoring here would make the next save
		// overwrite an unsubscribe that is, or is about to be, durable. The packet
		// still reports failure; the session is unresolved until the resolver settles
		// it, so nothing else can be persisted meanwhile. See sess_uncertain.go.
		undoUnsub := func() {
			// The delivery barrier goes back too: nothing was persisted, so these
			// subscriptions are still the client's and must keep delivering, and their
			// transmitted deliveries still owe their identifiers.
			sess.reopenTeardownDelivery(closedDeliveries)
			sess.unvoidRegNonces(voidedSent)
			sess.mu.Lock()
			sess.subs = snap.subs
			sess.sharedSubs = snap.shared
			sess.rapSubs = snap.rap
			sess.noLocalSubs = snap.noLocal
			sess.subIDs = snap.subIDs
			sess.rollbackStagedDrains()
			sess.mu.Unlock()
		}
		var restore *mqttUnsubAckRestore
		switch sess.classifySave(wasUncertain, err) {
		case saveDefinitelyFailed:
			undoUnsub()
		case saveMaybeLanded:
			// Kept, because the record may hold the removal and the staged drains, and
			// the resolver makes exactly those bytes durable. The confirmed-path work
			// above is then owed and runs from the resolution instead — under ITS
			// token, since this packet's is closed by then.
			//
			// The restore flag is this ATTEMPT's own: the continuation that sets it is
			// the one registered here, so it can fire for nothing else — not for a
			// foreign verdict, and not for some later ambiguity B armed by another
			// writer. Set LAST, so a reader that observes it observes a durable and
			// fully applied unsubscribe. See mqttUnsubAckRestore.
			landed := &atomic.Bool{}
			sess.retainOnLanded(func(rtok recordTx) {
				commitUnsub(rtok)
				landed.Store(true)
			})
			// Snapshotted BEFORE the 0x83 overwrite below, reason strings included: the
			// encoder carries the first failed filter's string as the packet-level
			// Reason String, so restoring the byte alone would tell a client "unable to
			// persist unsubscribe" for a filter that in fact had no subscription.
			restore = newMQTTUnsubAckRestore(landed, filters)
			// Deliberately NOT resolved here: this packet's own record transaction is
			// still open, and the resolver must hold the fence — so an attempt from
			// inside would simply be refused. The readLoop resolves once the
			// transaction is released. See mqttResolveAfterPacket.
		}
		// v5 reports the failure per-filter (0x83) and still sends the UNSUBACK;
		// 3.1.1 has no per-filter code, so surface the error (connection closes).
		if c.mqtt.proto == mqttProtoLevel5 {
			for _, f := range filters {
				f.reason = mqttReasonImplementationSpecificError
				f.reasonStr = "unable to persist unsubscribe"
			}
			return restore, nil
		}
		return nil, err
	}
	commitUnsub(packetTok)
	return nil, nil
}

// mqttUnsubAckRestore is the truthful UNSUBACK a v5 UNSUBSCRIBE owes its client when
// the record write it reported as failed turns out to be DURABLE.
//
// The v5 UNSUBSCRIBE is the one operation this broker ACKNOWLEDGES over an ambiguous
// write: it overwrites every per-filter reason with 0x83 and still sends the
// UNSUBACK, so a write that landed leaves the client told the unsubscribe failed
// while the record says it happened. The resolver now settles such a write forward,
// which would make that contradiction deterministic — so the acknowledgment waits for
// the resolution the readLoop performs anyway (mqttResolveAfterPacket) and tells the
// truth whenever proof has arrived by then.
//
// The signal is the attempt's OWN landed continuation, never a session-state
// predicate: seqUncertain is cleared by a foreign disposition as well as by a
// resolution, and it can be re-armed by a different attempt entirely, so a coarse
// check would restore this packet's reasons on somebody else's outcome. A flag set
// by the continuation registered for THIS write cannot be either.
//
// reasons/strs are positional over the packet's filters, captured before the 0x83
// overwrite.
type mqttUnsubAckRestore struct {
	landed  *atomic.Bool
	reasons []byte
	strs    []string
}

// newMQTTUnsubAckRestore snapshots the per-filter reasons as they stood before the
// failure overwrite. landed is the flag the attempt's landed continuation sets — the
// SAME pointer, never a copy, since that is the whole handoff.
func newMQTTUnsubAckRestore(landed *atomic.Bool, filters []*mqttFilter) *mqttUnsubAckRestore {
	r := &mqttUnsubAckRestore{
		landed:  landed,
		reasons: make([]byte, len(filters)),
		strs:    make([]string, len(filters)),
	}
	for i, f := range filters {
		r.reasons[i], r.strs[i] = f.reason, f.reasonStr
	}
	return r
}

// applyIfLanded puts the true reasons back if this attempt has been proven durable.
//
// The flag LOAD is the linearization point of the guarantee: a verdict that arrives
// after it — including between here and the enqueue — leaves the honest 0x83 standing,
// which is the residual docs/CONFORMANCE.md states.
func (r *mqttUnsubAckRestore) applyIfLanded(filters []*mqttFilter) {
	if r == nil || !r.landed.Load() {
		return
	}
	for i, f := range filters {
		if i >= len(r.reasons) {
			break
		}
		f.reason, f.reasonStr = r.reasons[i], r.strs[i]
	}
}

// mqttAckUnsubscribe is the UNSUBSCRIBE packet's tail: make the UNSUBACK truthful if
// the packet's ambiguous write has since been proven durable, then acknowledge —
// unless this connection has been DISPLACED meanwhile, which is what a foreign
// verdict does. Reports whether the UNSUBACK was sent, so the caller traces exactly
// what went out.
//
// What ACTUALLY makes the guarantee hold is the DISCONNECT claim, read inside the
// enqueue's own c.mu critical section (mqttEnqueueProtoLocked). A displacement notifies
// through that claim, and the watch's eviction worker holds no lock of ours — so any check
// made and released BEFORE the enqueue is a check-then-act, and an acknowledgment
// appended after the 0x8E would violate [MQTT-3.14.4-1]. Only a check made WITH the
// write closes that window.
//
// The ownership sample below (`sess.c == c` under sess.mu, read that way rather than as
// c.isClosed(): a foreign disposition detaches synchronously and closes asynchronously,
// so the close flag lags the fact) is DEFENSIVE, deliberately. Every displacement that
// can reach this tail has a written CONNACK, so it consumes the claim, and the gate
// above refuses on its own; a mutation of the sample therefore SURVIVES, which is the
// honest signal that it guards nothing reachable today rather than something untested.
// It is kept because the cost is one comparison and it states a different fact — "this
// session is no longer mine" — that a future detach-without-notifying path would need,
// where the alternative is failing open.
//
// Runs from the client's readLoop. No lock held on entry.
func (c *client) mqttAckUnsubscribe(pi uint16, filters []*mqttFilter, restore *mqttUnsubAckRestore) bool {
	restore.applyIfLanded(filters)
	c.mu.Lock()
	sess := c.mqtt.sess
	c.mu.Unlock()
	if sess != nil {
		sess.mu.Lock()
		bound := sess.c == c
		sess.mu.Unlock()
		if !bound {
			return false
		}
	}
	// Test seam, fired in exactly the window the second gate exists for: after the
	// ownership sample, before the enqueue's critical section.
	if h := mqttTestUnsubAckWindowHookV.Load(); h != nil {
		(*h)(c)
	}
	return c.mqttEnqueueUnsubAck(pi, filters)
}

// mqttTestUnsubAckWindowHookV fires between the acknowledgment's ownership sample and
// its enqueue. See mqttAckUnsubscribe.
var mqttTestUnsubAckWindowHookV atomic.Pointer[func(c *client)]

func setMQTTTestUnsubAckWindowHook(h func(c *client)) func() {
	prev := mqttTestUnsubAckWindowHookV.Load()
	if h == nil {
		mqttTestUnsubAckWindowHookV.Store(nil)
	} else {
		mqttTestUnsubAckWindowHookV.Store(&h)
	}
	return func() { mqttTestUnsubAckWindowHookV.Store(prev) }
}

// removeUnsubConsumer releases one sid's consumer state for an UNSUBSCRIBE that is
// known durable: the shared group release, the cancellation tombstones, the register
// deletes and the consumer delete.
//
// A METHOD rather than a closure inside mqttProcessUnsubs, deliberately. It is reached
// from commitUnsub, which also runs as a LANDED CONTINUATION under the resolver's
// transaction long after the packet's has closed — and every write here is refused at
// entry under a closed token, silently, since none of these calls is checked for an
// outcome. As a closure it captured the packet's transaction and kept using it after
// its caller's signature changed to take one; with no enclosing scope there is nothing
// to capture, so the token can only arrive as an argument. Passing the WRONG one
// explicitly is still possible and is NOT covered by any test: the end-to-end
// unsubscribe test releases a consumer with no pending registers, so the token-gated
// record fallback below is never reached and the mutation survives. Stated as a known
// gap in docs/ROADMAP.md rather than implied to be guarded.
//
// The caller holds the session-manager lock AND an open record transaction — the
// packet's on the confirmed path, the resolver's on the landed continuation — and tok
// is that transaction's token, which the cancellation fallback writes under. sess.mu
// must NOT be held: this takes it.
func (sess *mqttSession) removeUnsubConsumer(c *client, tok recordTx, sid string, heldShared map[string]bool) {
	cc, ok := sess.cons[sid]
	if !ok {
		// A shared filter's group durable must be released even without a
		// local consumer entry (e.g. a per-filter restore failure left the
		// membership persisted with no live join): derive the durable(s) from
		// the session key — the persisted authority — like clear() does, and
		// delete each that has no remaining holder. The membership was already
		// removed and persisted above, so the scan is correct. Spec5 [4.8.2].
		if strings.HasPrefix(sid, string(mqttSharedSubPrefix)) && !strings.HasSuffix(sid, mqttMultiLevelSidSuffix) {
			for _, dur := range mqttSharedDurablesForKey(sid) {
				if heldShared[dur] {
					continue
				}
				if _, err := sess.jsa.deleteConsumer(mqttStreamName, dur, true); isErrorOtherThan(err, JSConsumerNotFoundErr) {
					c.Errorf("Unable to delete shared consumer %q: %v", dur, err)
				}
			}
		}
		return
	}
	delete(sess.cons, sid)
	// A shared group consumer is released (deleted only if no other member
	// remains); the leaving filter has already been removed from sharedSubs
	// and persisted below, so the last-holder scan is correct. Spec5 [4.8.2].
	if cc.DeliverGroup != _EMPTY_ {
		sess.releaseSharedConsumer(c, tok, cc, heldShared)
		return
	}

	c.mu.Lock()
	sub := c.subs[cc.DeliverSubject]
	c.mu.Unlock()

	// Need lock here since these are accessed by callbacks
	sess.mu.Lock()
	// Mark the delivery sub closed so an in-flight QoS 1/2 callback stops
	// tracking new messages after the purge (deleteConsumer is async);
	// same barrier as the QoS 0 downgrade path in processJSConsumer.
	if sub != nil && sub.mqtt != nil {
		sess.subsMu.Lock()
		sub.mqtt.closed = true
		sess.subsMu.Unlock()
	}
	regs, cancels := sess.purgeConsumerPendingLocked(cc.Durable)
	sess.mu.Unlock()
	// The consumer delete moved BELOW the purge, which is also what the barrier
	// above always wanted: the closed flag is now set before the durable goes, and
	// a cancelled registration is durably named before the consumer that would
	// classify its source disappears.
	if !sess.secureRegCancellations(c, tok, cancels) {
		c.Errorf("MQTT: the cancelled in-flight registration(s) of consumer %q for client %q "+
			"are not durably recorded yet", cc.Durable, sess.id)
	}
	// Before the consumer, for the reason given in deleteInflightRegisters.
	sess.deleteInflightRegisters(c, tok, regs)
	sess.deleteConsumer(cc)
}

// mqttUnsubSids returns the sid(s) one UNSUBSCRIBE filter covers: its own, plus
// the level-up companion a "foo/#" filter carries, which drains (and tears down)
// independently.
func mqttUnsubSids(sid string) []string {
	if !mqttNeedSubForLevelUp(sid) {
		return []string{sid}
	}
	return []string{sid, sid[:len(sid)-2] + mqttMultiLevelSidSuffix}
}

// mqttEnqueueUnsubAck enqueues an UNSUBACK and reports whether it was sent. A v5
// UNSUBACK whose reason-code payload alone exceeds the client's Maximum Packet
// Size is suppressed (returns false), and BOTH protocol paths refuse to append the
// packet behind a server-initiated DISCONNECT — checked in the same c.mu critical
// section as the write. See mqttEnqueueProtoLocked and mqttAckUnsubscribe.
func (c *client) mqttEnqueueUnsubAck(pi uint16, filters []*mqttFilter) bool {
	if c.mqtt.proto == mqttProtoLevel5 {
		// v5 UNSUBACK adds a properties block and one reason code per filter.
		// Spec5 [3.11].
		w := newMQTTWriter(7 + len(filters))
		// Carry the first failed filter's detail as the packet-level Reason
		// String, when the client permits it. Spec5 [3.11.2.1.2].
		var reasonStr string
		for _, f := range filters {
			if f.reason != 0 && f.reasonStr != _EMPTY_ {
				reasonStr = f.reasonStr
				break
			}
		}
		props := c.mqttReasonProps(mqttPacketUnsubAck, reasonStr)
		frame := func() []byte {
			var vh mqttWriter
			vh.WriteUint16(pi)
			vh.writeProperties(props)
			for _, f := range filters {
				vh.WriteByte(f.reason)
			}
			w.Reset()
			w.WriteByte(mqttPacketUnsubAck)
			w.WriteVarInt(vh.Len())
			w.Write(vh.Bytes())
			return w.Bytes()
		}
		buf := frame()
		// Drop the Reason String (only) if it overflows the client's max.
		if props != nil && !c.mqttFitsClientMax(len(buf)) {
			props = nil
			buf = frame()
		}
		// If the reason-code payload alone still overflows, the UNSUBACK cannot be
		// sent [MQTT-3.1.2-24]; the unsubscribe side effects stand.
		return c.mqttEnqueueFittingProtoUnlessDisconnected(buf)
	}
	w := newMQTTWriter(4)
	w.WriteByte(mqttPacketUnsubAck)
	w.WriteVarInt(2)
	w.WriteUint16(pi)
	return c.mqttEnqueueProtoLocked(w.Bytes(), true)
}

func mqttUnsubscribeTrace(pi uint16, filters []*mqttFilter) string {
	var sep string
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, f := range filters {
		sb.WriteString(sep)
		sb.Write(f.ttopic)
		sb.WriteString(" (")
		sb.WriteString(f.filter)
		sb.WriteString(")")
		if i == 0 {
			sep = ", "
		}
	}
	sb.WriteString(fmt.Sprintf("] pi=%v", pi))
	return sb.String()
}

//////////////////////////////////////////////////////////////////////////////
//
// PINGREQ/PINGRESP related functions
//
//////////////////////////////////////////////////////////////////////////////

func (c *client) mqttEnqueuePingResp() {
	// 2 bytes; always fits a connected client's Maximum Packet Size, checked for
	// uniformity. [MQTT-3.1.2-24].
	c.mqttEnqueueFittingProto(mqttPingResponse)
}

//////////////////////////////////////////////////////////////////////////////
//
// Trace functions
//
//////////////////////////////////////////////////////////////////////////////

func errOrTrace(err error, trace string) []byte {
	if err != nil {
		return []byte(err.Error())
	}
	return []byte(trace)
}

//////////////////////////////////////////////////////////////////////////////
//
// Subject/Topic conversion functions
//
//////////////////////////////////////////////////////////////////////////////

// Converts an MQTT Topic Name to a NATS Subject (used by PUBLISH)
// See mqttToNATSSubjectConversion() for details.
func mqttTopicToNATSPubSubject(mt []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(mt, false)
}

// Converts an MQTT Topic Filter to a NATS Subject (used by SUBSCRIBE)
// See mqttToNATSSubjectConversion() for details.
func mqttFilterToNATSSubject(filter []byte) ([]byte, error) {
	return mqttToNATSSubjectConversion(filter, true)
}

// Converts an MQTT Topic Name or Filter to a NATS Subject.
// In MQTT:
// - a Topic Name does not have wildcard (PUBLISH uses only topic names).
// - a Topic Filter can include wildcards (SUBSCRIBE uses those).
// - '+' and '#' are wildcard characters (single and multiple levels respectively)
// - '/' is the topic level separator.
//
// Conversion that occurs:
//   - '/' is replaced with '/.' if it is the first character in mt
//   - '/' is replaced with './' if the last or next character in mt is '/'
//     For instance, foo//bar would become foo./.bar
//   - '/' is replaced with '.' for all other conditions (foo/bar -> foo.bar)
//   - '.' is replaced with '//'.
//   - ' ' cause an error to be returned.
//
// If there is no need to convert anything (say "foo" remains "foo"), then
// the no memory is allocated and the returned slice is the original `mt`.
func mqttToNATSSubjectConversion(mt []byte, wcOk bool) ([]byte, error) {
	var cp bool
	var j int
	res := mt

	makeCopy := func(i int) {
		cp = true
		res = make([]byte, 0, len(mt)+10)
		if i > 0 {
			res = append(res, mt[:i]...)
		}
	}

	end := len(mt) - 1
	for i := 0; i < len(mt); i++ {
		switch mt[i] {
		case mqttTopicLevelSep:
			if i == 0 || res[j-1] == btsep {
				if !cp {
					makeCopy(0)
				}
				res = append(res, mqttTopicLevelSep, btsep)
				j++
			} else if i == end || mt[i+1] == mqttTopicLevelSep {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep, mqttTopicLevelSep)
				j++
			} else {
				if !cp {
					makeCopy(i)
				}
				res = append(res, btsep)
			}
		case ' ', '\t', '\n', '\r', '\f':
			// We cannot support whitespace in the MQTT topic/filter — these
			// characters would also corrupt the NATS wire protocol when the
			// subject is forwarded to other connection types (e.g. leaf
			// nodes) where the resulting control line could be split.
			return nil, errMQTTUnsupportedCharacters
		case 0x7f:
			// SubjectTree uses DEL as an internal pivot marker, so retained
			// subjects containing it cannot be indexed safely, including
			// legacy retained messages recovered from the retained-message
			// stream.
			return nil, errMQTTUnsupportedCharacters
		case btsep:
			if !cp {
				makeCopy(i)
			}
			res = append(res, mqttTopicLevelSep, mqttTopicLevelSep)
			j++
		case mqttSingleLevelWC, mqttMultiLevelWC:
			if !wcOk {
				// Spec [MQTT-3.3.2-2] and [MQTT-4.7.1-1]
				// The wildcard characters can be used in Topic Filters, but MUST NOT be used within a Topic Name
				return nil, fmt.Errorf("%w: wildcards not allowed in publish's topic: %q", errMQTTMalformedPacket, mt)
			}
			// Spec [MQTT-4.7.1-3]: '#' must be on its own or follow a level
			// separator, and in either case must be the LAST character of the
			// filter (no further levels after it). Checked here — rather than
			// relying on the embedded nats-server's internal subject validator
			// the way upstream implicitly does — because in the standalone
			// broker a malformed subject reaches nats.go's Subscribe() over a
			// real connection (nats.go's own client-side check does not catch
			// this), and letting it through disrupts that shared connection's
			// other in-flight requests rather than failing this one filter.
			if mt[i] == mqttMultiLevelWC && (i != end || (i > 0 && mt[i-1] != mqttTopicLevelSep)) {
				return nil, fmt.Errorf("%w: multi-level wildcard must be the last character in filter: %q", errMQTTMalformedPacket, mt)
			}
			if !cp {
				makeCopy(i)
			}
			if mt[i] == mqttSingleLevelWC {
				res = append(res, pwc)
			} else {
				res = append(res, fwc)
			}
		default:
			if cp {
				res = append(res, mt[i])
			}
		}
		j++
	}
	if cp && res[j-1] == btsep {
		res = append(res, mqttTopicLevelSep)
		j++
	}
	return res[:j], nil
}

// Converts a NATS subject to MQTT topic. This is for publish
// messages only, so there is no checking for wildcards.
// Rules are reversed of mqttToNATSSubjectConversion.
func natsSubjectStrToMQTTTopic(subject string) []byte {
	return natsSubjectToMQTTTopic(stringToBytes(subject))
}

func natsSubjectToMQTTTopic(subject []byte) []byte {
	topic := make([]byte, len(subject))
	end := len(subject) - 1
	var j int
	for i := 0; i < len(subject); i++ {
		switch subject[i] {
		case mqttTopicLevelSep:
			if i < end {
				switch c := subject[i+1]; c {
				case btsep, mqttTopicLevelSep:
					if c == btsep {
						topic[j] = mqttTopicLevelSep
					} else {
						topic[j] = btsep
					}
					j++
					i++
				default:
				}
			}
		case btsep:
			topic[j] = mqttTopicLevelSep
			j++
		default:
			topic[j] = subject[i]
			j++
		}
	}
	return topic[:j]
}

// Returns true if the subject has more than 1 token and ends with ".>"
func mqttNeedSubForLevelUp(subject string) bool {
	if len(subject) < 3 {
		return false
	}
	end := len(subject)
	if subject[end-2] == '.' && subject[end-1] == fwc {
		return true
	}
	return false
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT Reader functions
//
//////////////////////////////////////////////////////////////////////////////

func (r *mqttReader) reset(buf []byte) {
	if l := len(r.pbuf); l > 0 {
		tmp := make([]byte, l+len(buf))
		copy(tmp, r.pbuf)
		copy(tmp[l:], buf)
		buf = tmp
		r.pbuf = nil
	}
	r.buf = buf
	r.pos = 0
	r.pstart = 0
}

func (r *mqttReader) hasMore() bool {
	return r.pos != len(r.buf)
}

func (r *mqttReader) readByte(field string) (byte, error) {
	if r.pos == len(r.buf) {
		return 0, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.EOF)
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *mqttReader) readPacketLen(maxLen int32) (int, bool, error) {
	v, complete, err := r.readVarInt()
	if err != nil {
		return 0, false, err
	}
	if complete {
		packetEnd := r.pos + v
		packetLen := packetEnd - r.pstart
		if maxLen != jwt.NoLimit && int64(packetLen) > int64(maxLen) {
			return packetLen, false, ErrMaxPayload
		}
		if packetEnd <= len(r.buf) {
			return v, true, nil
		}
	}
	r.pbuf = make([]byte, len(r.buf)-r.pstart)
	copy(r.pbuf, r.buf[r.pstart:])
	return 0, false, nil
}

func (r *mqttReader) readVarInt() (int, bool, error) {
	m := 1
	v := 0
	for {
		var b byte
		if r.pos != len(r.buf) {
			b = r.buf[r.pos]
			r.pos++
		} else {
			break
		}
		v += int(b&0x7f) * m
		if (b & 0x80) == 0 {
			return v, true, nil
		}
		m *= 0x80
		if m > 0x200000 {
			return 0, false, errMQTTMalformedVarInt
		}
	}
	return 0, false, nil
}

func (r *mqttReader) readString(field string) (string, error) {
	var s string
	bs, err := r.readBytes(field, false)
	if err == nil {
		s = string(bs)
	}
	return s, err
}

func (r *mqttReader) readBytes(field string, cp bool) ([]byte, error) {
	luint, err := r.readUint16(field)
	if err != nil {
		return nil, err
	}
	l := int(luint)
	if l == 0 {
		return nil, nil
	}
	start := r.pos
	if start+l > len(r.buf) {
		return nil, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.ErrUnexpectedEOF)
	}
	r.pos += l
	b := r.buf[start:r.pos]
	if cp {
		b = copyBytes(b)
	}
	return b, nil
}

func (r *mqttReader) readUint16(field string) (uint16, error) {
	if len(r.buf)-r.pos < 2 {
		return 0, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.ErrUnexpectedEOF)
	}
	start := r.pos
	r.pos += 2
	return binary.BigEndian.Uint16(r.buf[start:r.pos]), nil
}

func (r *mqttReader) readUint32(field string) (uint32, error) {
	if len(r.buf)-r.pos < 4 {
		return 0, fmt.Errorf("%w: error reading %s: %v", errMQTTMalformedPacket, field, io.ErrUnexpectedEOF)
	}
	start := r.pos
	r.pos += 4
	return binary.BigEndian.Uint32(r.buf[start:r.pos]), nil
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT Writer functions
//
//////////////////////////////////////////////////////////////////////////////

func (w *mqttWriter) WriteUint16(i uint16) {
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func (w *mqttWriter) WriteUint32(i uint32) {
	w.WriteByte(byte(i >> 24))
	w.WriteByte(byte(i >> 16))
	w.WriteByte(byte(i >> 8))
	w.WriteByte(byte(i))
}

func (w *mqttWriter) WriteString(s string) {
	w.WriteBytes([]byte(s))
}

func (w *mqttWriter) WriteBytes(bs []byte) {
	w.WriteUint16(uint16(len(bs)))
	w.Write(bs)
}

func (w *mqttWriter) WriteVarInt(value int) {
	for {
		b := byte(value & 0x7f)
		value >>= 7
		if value > 0 {
			b |= 0x80
		}
		w.WriteByte(b)
		if value == 0 {
			break
		}
	}
}

func newMQTTWriter(cap int) *mqttWriter {
	w := &mqttWriter{}
	w.Grow(cap)
	return w
}

//////////////////////////////////////////////////////////////////////////////
//
// MQTT 5.0 properties codec
//
// References to "spec5" here are from
// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
//
//////////////////////////////////////////////////////////////////////////////

// mqttPropertyKnown reports whether prop is a property identifier defined by
// MQTT 5.0. Used to give a clearer error for genuinely unknown identifiers.
func mqttPropertyKnown(prop byte) bool {
	switch prop {
	case mqttPropPayloadFormat, mqttPropMessageExpiry, mqttPropContentType,
		mqttPropResponseTopic, mqttPropCorrelationData, mqttPropSubscriptionID,
		mqttPropSessionExpiry, mqttPropAssignedClientID, mqttPropServerKeepAlive,
		mqttPropAuthMethod, mqttPropAuthData, mqttPropRequestProblemInfo,
		mqttPropWillDelay, mqttPropRequestResponseInfo, mqttPropResponseInfo,
		mqttPropServerReference, mqttPropReasonString, mqttPropReceiveMaximum,
		mqttPropTopicAliasMax, mqttPropTopicAlias, mqttPropMaxQoS,
		mqttPropRetainAvailable, mqttPropUserProperty, mqttPropMaxPacketSize,
		mqttPropWildcardSubAvailable, mqttPropSubIDAvailable, mqttPropSharedSubAvailable:
		return true
	}
	return false
}

// mqttPropsContextWill is a pseudo "packet type" used to validate Will
// properties, which travel inside a CONNECT payload but have their own allowed
// property set. It cannot collide with real packet types (multiples of 0x10).
const mqttPropsContextWill = byte(0x01)

// mqttPropsContextPubOut is a pseudo "packet type" for a server-to-client
// (outbound) PUBLISH, which unlike an inbound PUBLISH may carry a server-injected
// Subscription Identifier and a server-assigned Topic Alias. Used to
// parse/validate delivered PUBLISH blocks (e.g. in tests). It cannot collide
// with real packet types (multiples of 0x10).
const mqttPropsContextPubOut = byte(0x02)

// mqttPropertyAllowed reports whether property prop may appear in the given
// context (a packet type, or mqttPropsContextWill / mqttPropsContextPubOut).
// Spec5 [2.2.2.2] table of which properties belong to which packets.
func mqttPropertyAllowed(prop, ctx byte) bool {
	switch prop {
	case mqttPropPayloadFormat, mqttPropMessageExpiry, mqttPropContentType,
		mqttPropResponseTopic, mqttPropCorrelationData:
		return ctx == mqttPacketPub || ctx == mqttPropsContextWill || ctx == mqttPropsContextPubOut
	case mqttPropSubscriptionID:
		// A Subscription Identifier is valid on a SUBSCRIBE and, from the server,
		// on a delivered PUBLISH (mqttPropsContextPubOut). A client-to-server
		// PUBLISH MUST NOT carry one (spec5 [MQTT-3.3.4-6]); inbound PUBLISH
		// properties and forwarded blocks are validated with the mqttPacketPub
		// context, so a client-supplied identifier is still rejected.
		return ctx == mqttPacketSub || ctx == mqttPropsContextPubOut
	case mqttPropSessionExpiry:
		return ctx == mqttPacketConnect || ctx == mqttPacketConnectAck || ctx == mqttPacketDisconnect
	case mqttPropAssignedClientID, mqttPropServerKeepAlive, mqttPropResponseInfo,
		mqttPropMaxQoS, mqttPropRetainAvailable, mqttPropWildcardSubAvailable,
		mqttPropSubIDAvailable, mqttPropSharedSubAvailable:
		return ctx == mqttPacketConnectAck
	case mqttPropAuthMethod, mqttPropAuthData:
		return ctx == mqttPacketConnect || ctx == mqttPacketConnectAck || ctx == mqttPacketAuth
	case mqttPropRequestProblemInfo, mqttPropRequestResponseInfo:
		return ctx == mqttPacketConnect
	case mqttPropWillDelay:
		return ctx == mqttPropsContextWill
	case mqttPropServerReference:
		return ctx == mqttPacketConnectAck || ctx == mqttPacketDisconnect
	case mqttPropReasonString:
		switch ctx {
		case mqttPacketConnectAck, mqttPacketPubAck, mqttPacketPubRec, mqttPacketPubRel,
			mqttPacketPubComp, mqttPacketSubAck, mqttPacketUnsubAck, mqttPacketDisconnect,
			mqttPacketAuth:
			return true
		}
		return false
	case mqttPropReceiveMaximum, mqttPropTopicAliasMax, mqttPropMaxPacketSize:
		return ctx == mqttPacketConnect || ctx == mqttPacketConnectAck
	case mqttPropTopicAlias:
		// Valid on a client PUBLISH and, from the server, on a delivered PUBLISH
		// (mqttPropsContextPubOut). mqttValidateForwardProps still rejects a
		// publisher-smuggled alias via the mqttPacketPub context.
		return ctx == mqttPacketPub || ctx == mqttPropsContextPubOut
	case mqttPropUserProperty:
		// Allowed in every packet that carries a properties section.
		return true
	}
	return false
}

// readProperties parses an MQTT 5.0 properties block from the reader for the
// given context (packet type or mqttPropsContextWill). The whole packet is
// already buffered by the dispatch loop, so the var-int length must complete.
// It validates length, identifiers, packet-context, and illegal duplicates.
// Returns nil (no error) when the block is present but empty.
// readPropString reads a UTF-8 Encoded String property value and enforces the
// well-formedness rules of Spec5 [MQTT-1.5.4-1]/[MQTT-1.5.4-2] (valid UTF-8, no
// U+0000). Ill-formed values are a Malformed Packet; without this check the
// server would relay protocol-violating bytes on to subscribers.
func (r *mqttReader) readPropString(field string) (string, error) {
	s, err := r.readString(field)
	if err != nil {
		return _EMPTY_, err
	}
	if err = mqttValidateString(s, field); err != nil {
		return _EMPTY_, fmt.Errorf("%w: %v", errMQTTMalformedProperties, err)
	}
	return s, nil
}

func (r *mqttReader) readProperties(ctx byte) (*mqttProperties, error) {
	plen, complete, err := r.readVarInt()
	if err != nil {
		return nil, err
	}
	if !complete {
		return nil, errMQTTMalformedProperties
	}
	if plen == 0 {
		return nil, nil
	}
	end := r.pos + plen
	if end > len(r.buf) {
		return nil, errMQTTMalformedProperties
	}
	props := &mqttProperties{present: make(map[byte]bool)}
	for r.pos < end {
		prop, err := r.readByte("property id")
		if err != nil {
			return nil, err
		}
		if !mqttPropertyAllowed(prop, ctx) {
			if !mqttPropertyKnown(prop) {
				return nil, fmt.Errorf("%w: 0x%x", errMQTTUnknownProperty, prop)
			}
			return nil, fmt.Errorf("%w: 0x%x", errMQTTPropertyNotAllowed, prop)
		}
		// All properties are non-repeatable except User Property and
		// Subscription Identifier. Spec5 [2.2.2.1].
		if prop != mqttPropUserProperty && prop != mqttPropSubscriptionID {
			if props.present[prop] {
				return nil, fmt.Errorf("%w: 0x%x", errMQTTDuplicateProperty, prop)
			}
			props.present[prop] = true
		}
		switch prop {
		case mqttPropPayloadFormat:
			props.payloadFormat, err = r.readByte("payload format indicator")
			if err == nil && props.payloadFormat > 1 {
				// Spec5 [3.3.2.3.2]: only 0 or 1 are defined.
				err = fmt.Errorf("%w: payload format indicator %d", errMQTTProtocolError, props.payloadFormat)
			}
		case mqttPropRequestProblemInfo:
			props.requestProblem, err = r.readByte("request problem information")
			if err == nil && props.requestProblem > 1 {
				// Spec5 [3.1.2.11.7]: a value other than 0 or 1 is a Protocol Error.
				err = fmt.Errorf("%w: request problem information %d", errMQTTProtocolError, props.requestProblem)
			}
		case mqttPropRequestResponseInfo:
			props.requestResponse, err = r.readByte("request response information")
			if err == nil && props.requestResponse > 1 {
				// Spec5 [3.1.2.11.6]: a value other than 0 or 1 is a Protocol Error.
				err = fmt.Errorf("%w: request response information %d", errMQTTProtocolError, props.requestResponse)
			}
		case mqttPropMaxQoS:
			props.maxQoS, err = r.readByte("maximum qos")
		case mqttPropRetainAvailable:
			props.retainAvailable, err = r.readByte("retain available")
		case mqttPropWildcardSubAvailable:
			props.wildcardSubAvail, err = r.readByte("wildcard subscription available")
		case mqttPropSubIDAvailable:
			props.subIDAvail, err = r.readByte("subscription identifier available")
		case mqttPropSharedSubAvailable:
			props.sharedSubAvail, err = r.readByte("shared subscription available")
		case mqttPropServerKeepAlive:
			props.serverKeepAlive, err = r.readUint16("server keep alive")
		case mqttPropReceiveMaximum:
			if props.receiveMax, err = r.readUint16("receive maximum"); err == nil && props.receiveMax == 0 {
				// Spec5 [3.1.2.11.3]: a Receive Maximum value of 0 is a Protocol
				// Error. (Absent means no property, i.e. the default, which we
				// treat as no client-imposed limit.)
				err = fmt.Errorf("%w: receive maximum of 0", errMQTTProtocolError)
			}
		case mqttPropTopicAliasMax:
			props.topicAliasMax, err = r.readUint16("topic alias maximum")
		case mqttPropTopicAlias:
			props.topicAlias, err = r.readUint16("topic alias")
		case mqttPropMessageExpiry:
			props.messageExpiry, err = r.readUint32("message expiry interval")
		case mqttPropSessionExpiry:
			props.sessionExpiry, err = r.readUint32("session expiry interval")
		case mqttPropWillDelay:
			props.willDelay, err = r.readUint32("will delay interval")
		case mqttPropMaxPacketSize:
			// The client is telling us the largest packet it will accept. We honor
			// this on every server-generated packet: an outbound PUBLISH exceeding
			// it is discarded (mqttEnqueuePublishMsgTo, serializeRetainedMsgsForSub),
			// and any other packet drops its Reason String or, failing that, is not
			// sent (mqttEnqueueFittingProto). Stored as c.mqtt.maxPacketSize. Spec5
			// [3.1.2.11.4].
			if props.maxPacketSize, err = r.readUint32("maximum packet size"); err == nil && props.maxPacketSize == 0 {
				// Spec5 [3.1.2.11.4]: a Maximum Packet Size of 0 is a Protocol
				// Error. (Absent means no property, i.e. no client-imposed limit.)
				err = fmt.Errorf("%w: maximum packet size of 0", errMQTTProtocolError)
			}
		case mqttPropContentType:
			props.contentType, err = r.readPropString("content type")
		case mqttPropResponseTopic:
			props.responseTopic, err = r.readPropString("response topic")
			if err == nil && strings.ContainsAny(props.responseTopic, "+#") {
				// Spec5 [3.3.2.3.5]: the Response Topic must not contain
				// wildcard characters; treat it as a Protocol Error.
				err = fmt.Errorf("%w: wildcard in response topic %q", errMQTTProtocolError, props.responseTopic)
			}
		case mqttPropAssignedClientID:
			props.assignedClientID, err = r.readPropString("assigned client identifier")
		case mqttPropAuthMethod:
			props.authMethod, err = r.readPropString("authentication method")
		case mqttPropResponseInfo:
			props.responseInfo, err = r.readPropString("response information")
		case mqttPropServerReference:
			_, err = r.readPropString("server reference")
		case mqttPropReasonString:
			props.reasonString, err = r.readPropString("reason string")
		case mqttPropCorrelationData:
			props.correlationData, err = r.readBytes("correlation data", true)
		case mqttPropAuthData:
			props.authData, err = r.readBytes("authentication data", true)
		case mqttPropSubscriptionID:
			var v int
			var complete bool
			v, complete, err = r.readVarInt()
			if err == nil && !complete {
				err = errMQTTMalformedProperties
			}
			if err == nil {
				// Spec5 [3.3.2.3.8]/[3.8.2.1.2]: a value of 0 is a Protocol Error.
				if v == 0 {
					return nil, fmt.Errorf("%w: subscription identifier of 0", errMQTTProtocolError)
				}
				props.subIDs = append(props.subIDs, v)
			}
		case mqttPropUserProperty:
			var k, val string
			if k, err = r.readPropString("user property key"); err == nil {
				val, err = r.readPropString("user property value")
			}
			if err == nil {
				props.user = append(props.user, mqttUserProperty{key: k, value: val})
			}
		}
		if err != nil {
			return nil, err
		}
	}
	// A property that lied about its length will have advanced past the
	// declared end; reject such packets as malformed. Spec5 [2.2.2.1].
	if r.pos != end {
		return nil, errMQTTMalformedProperties
	}
	return props, nil
}

// encode writes the set properties as a properties body (without the leading
// length). Scalar/byte/int properties are written only when flagged present;
// strings/slices when non-empty. Used by the server to emit properties.
func (p *mqttProperties) encode(w *mqttWriter) {
	if p == nil {
		return
	}
	wb := func(id, v byte) {
		w.WriteByte(id)
		w.WriteByte(v)
	}
	w16 := func(id byte, v uint16) {
		w.WriteByte(id)
		w.WriteUint16(v)
	}
	w32 := func(id byte, v uint32) {
		w.WriteByte(id)
		w.WriteUint32(v)
	}
	if p.present[mqttPropPayloadFormat] {
		wb(mqttPropPayloadFormat, p.payloadFormat)
	}
	if p.present[mqttPropMessageExpiry] {
		w32(mqttPropMessageExpiry, p.messageExpiry)
	}
	if p.present[mqttPropContentType] {
		w.WriteByte(mqttPropContentType)
		w.WriteString(p.contentType)
	}
	if p.present[mqttPropResponseTopic] {
		w.WriteByte(mqttPropResponseTopic)
		w.WriteString(p.responseTopic)
	}
	if p.present[mqttPropCorrelationData] {
		w.WriteByte(mqttPropCorrelationData)
		w.WriteBytes(p.correlationData)
	}
	if p.present[mqttPropSessionExpiry] {
		w32(mqttPropSessionExpiry, p.sessionExpiry)
	}
	if p.present[mqttPropReceiveMaximum] {
		w16(mqttPropReceiveMaximum, p.receiveMax)
	}
	if p.present[mqttPropMaxQoS] {
		wb(mqttPropMaxQoS, p.maxQoS)
	}
	if p.present[mqttPropRetainAvailable] {
		wb(mqttPropRetainAvailable, p.retainAvailable)
	}
	if p.present[mqttPropMaxPacketSize] {
		w32(mqttPropMaxPacketSize, p.maxPacketSize)
	}
	if p.assignedClientID != _EMPTY_ {
		w.WriteByte(mqttPropAssignedClientID)
		w.WriteString(p.assignedClientID)
	}
	if p.responseInfo != _EMPTY_ {
		w.WriteByte(mqttPropResponseInfo)
		w.WriteString(p.responseInfo)
	}
	if p.present[mqttPropTopicAliasMax] {
		w16(mqttPropTopicAliasMax, p.topicAliasMax)
	}
	if p.present[mqttPropServerKeepAlive] {
		w16(mqttPropServerKeepAlive, p.serverKeepAlive)
	}
	if p.reasonString != _EMPTY_ {
		w.WriteByte(mqttPropReasonString)
		w.WriteString(p.reasonString)
	}
	if p.present[mqttPropWildcardSubAvailable] {
		wb(mqttPropWildcardSubAvailable, p.wildcardSubAvail)
	}
	if p.present[mqttPropSubIDAvailable] {
		wb(mqttPropSubIDAvailable, p.subIDAvail)
	}
	if p.present[mqttPropSharedSubAvailable] {
		wb(mqttPropSharedSubAvailable, p.sharedSubAvail)
	}
	for _, up := range p.user {
		w.WriteByte(mqttPropUserProperty)
		w.WriteString(up.key)
		w.WriteString(up.value)
	}
}

// writeProperties writes a length-prefixed MQTT 5.0 properties block. A nil
// (or empty) set is encoded as a single 0 length byte, which is mandatory in
// every v5 packet that has a properties section. Spec5 [2.2.2.1].
func (w *mqttWriter) writeProperties(p *mqttProperties) {
	if p == nil {
		w.WriteVarInt(0)
		return
	}
	body := newMQTTWriter(0)
	p.encode(body)
	b := body.Bytes()
	w.WriteVarInt(len(b))
	w.Write(b)
}
