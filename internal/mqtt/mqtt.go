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

// The package vocabulary: MQTT wire constants, the broker-owned stream and subject names,
// configuration defaults, the shared error values, and the per-server (srvMQTT) and
// per-connection (mqtt) state every other file of the protocol core works on.

package mqtt

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
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

	// What a connected MQTT 3.1.1 packet may carry beyond max_payload: the fixed
	// header (5), the largest topic or filter with its length prefix (2+65535) and
	// a packet identifier (2). max_payload bounds the NATS message, and a topic or
	// filter is not payload, so a 3.1.1 packet bounded by max_payload alone would
	// refuse a PUBLISH whose payload fits. Same value as upstream nats-server's
	// mqttMaxPacketOverhead (nats-io/nats-server#8692).
	mqttConnectedPacketOverhead = 5 + 2 + 65535 + 2

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

	// Subject prefix for the permanent per-CONNECT acceptance facts, carried on
	// the SAME stream as the session records so an accepted CONNECT can commit
	// both in one atomic batch. It is a separate TOP-LEVEL prefix, never a nesting
	// under "sess.", because every session walk in this package filters
	// "sess.<domainTk>>" and decodes what it gets as a session record.
	// One subject per acceptance: <prefix><domainTk><cidHash>.<epoch>.<acceptID>.
	// See sess_accept_fact.go.
	mqttAcceptSubjectPrefix = mqttPrefix + "accept."

	// Stream name and subject prefix for the delayed-Will durable outbox
	// (Layer 2). One subject per Will event: <prefix><domainTk><cidHash>.<willID>,
	// MaxMsgsPer:1, so each generation is an independent CAS target and an
	// in-flight publication survives a fresh reconnect+disconnect. See
	// will_outbox.go.
	mqttWillQStreamName    = "$MQTT_willq"
	mqttWillQSubjectPrefix = mqttPrefix + "willq."

	// Stream name and subject prefix for the durable per-client GENERATION
	// RETIREMENT watermark (one subject per client, MaxMsgsPer:1, permanent
	// retention). A teardown that has WON its claim records the retirement here
	// before it deletes anything, so the fact survives the one thing the session
	// record cannot: an unconditional foreign write evicting the marker off the
	// record's own subject. (Two broker-authored CONDITIONAL writes off one base
	// cannot both apply — see the same-base boundary in docs/CONFORMANCE.md — but a
	// write carrying no expectation is not excluded by any CAS.) Nothing ever reaps a
	// watermark. See sess_teardown.go.
	mqttTeardownStreamName    = "$MQTT_teardown"
	mqttTeardownSubjectPrefix = mqttPrefix + "teardown."

	// $MQTT_origin was the durable per-client session-interval register, whose sequence
	// named the interval a client's record subject was in and supplied the identity a
	// parentless create inherited. The write-lineage audit deleted it: every verdict it
	// enabled was a false fork. The broker no longer defines it, so an account still
	// holding one is an unsupported persisted shape and startup REFUSES rather than
	// carrying it — the name survives here for exactly that preflight.
	// See mqttRefuseRetiredOriginStream and docs/ARCHITECTURE.md "Compatibility boundary".
	mqttOriginStreamName = "$MQTT_origin"

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

	// The shared-group lifecycle records (F-105 transfer): the per-group group
	// record and the per-generation acquisition ledger. Kept on their OWN stream
	// rather than on $MQTT_inflight, even though both are MaxMsgsPer:1 CAS slots
	// like the in-flight registers. The registers are rewritten and then deleted
	// on ack, and an evicted register revives on a raw-block recovery whenever a
	// neighbour keeps its storage block non-empty (the upstream defect recorded in
	// docs/known-upstream-issues.md, MAXMSGSPER-REVIVAL). A permanent lifecycle
	// record co-located on $MQTT_inflight would be exactly such a neighbour, so a
	// plain session that leaves no live register would still not let its block
	// collapse. These records are not in-flight registers and do not belong there;
	// their own stream keeps $MQTT_inflight collapsible. See shared_lifecycle.go.
	mqttSharedLifecycleStreamName          = "$MQTT_shared"
	mqttSharedLifecycleStreamSubjectPrefix = mqttPrefix + "shared."

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

	// The kind tokens of a JS API reply subject, which is always
	// "$MQTT.JSA.<node id>.<kind>.<nuid>": "$MQTT.JSA.<node id>.SC.<nuid>" answers
	// a stream create, "$MQTT.JSA.<node id>.SL.<nuid>" a stream lookup, and so on.
	// processJSAPIReplies reads only the kind, at mqttJSATokenPos, and correlates
	// on the whole subject; the nuid alone makes it unique.
	mqttJSATokenPos       = 4
	mqttJSAStreamCreate   = "SC"
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

	// Default bound (in seconds) on the server-side TLS handshake for an MQTT
	// connection when MQTTOpts.TLSTimeout is unset. Matches nats-server's default
	// so a client that opens a socket and then stalls cannot pin a connection
	// slot indefinitely.
	mqttDefaultTLSTimeout = 2.0

	// Backoff between retries of a failed Accept while the server runs: doubling from
	// the minimum, capped at the maximum, reset by the next accepted connection. The
	// values are upstream's (ACCEPT_MIN_SLEEP / ACCEPT_MAX_SLEEP).
	mqttAcceptMinBackoff = 10 * time.Millisecond
	mqttAcceptMaxBackoff = time.Second

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
	mqttSessJailDur      = newMQTTKnob(mqttSessFlappingJailDur)
	mqttFlapCleanItvl    = newMQTTKnob(mqttSessFlappingCleanupInterval)
	mqttRetainedCacheTTL = newMQTTKnob(mqttDefaultRetainedCacheTTL)
	// How old a consumer must be before the orphan sweep may delete it: a plain
	// session durable is created before the record save that references it, so a
	// young unreferenced consumer may just be mid-SUBSCRIBE. A var so tests can
	// shorten it; the orphan QoS 2 sweep additionally supports an instance-scoped
	// override, because it is read by a goroutine a test cannot synchronize with.
	mqttOrphanSweepGrace = newMQTTKnob(5 * time.Minute)
	// Ceiling on how long a delivery waits for a gate to release its subscription:
	// the reconciliation phase's per-subscription gate, and the connection-scoped
	// restore verdict. The bound exists so a CONNECT that dies without releasing —
	// every ordinary exit releases, so that means a panic — cannot park a delivery
	// goroutine indefinitely. A var so tests can shorten it.
	//
	// **It is a CAP, not the AckWait.** The effective bound is
	// min(AckWait, this), and an earlier comment here claimed waiting past it buys
	// nothing "because JetStream re-pushes the message at exactly that point
	// anyway" — true only while AckWait is inside the cap. With a configured
	// AckWait above it the wait ends long before any redelivery, which is why a
	// timed-out wait is a DISTINCT verdict from a refusal and why the two classes
	// carrying no withheld marker (PUBREL, affine copies) NAK on it rather than
	// returning. See mqttTransmissionTimedOut and mqttSub.phaseGate.
	mqttPhaseGateWaitMax = newMQTTKnob(30 * time.Second)
)

var (
	errMQTTHTTPRequestOnMQTTListener  = errors.New("an HTTP request reached the MQTT listener: this listener serves MQTT over TCP; MQTT over WebSocket is served on its own listener (-ws-port)")
	errMQTTTopicFilterCannotBeEmpty   = errors.New("topic filter cannot be empty")
	errMQTTMalformedVarInt            = errors.New("malformed variable int")
	errMQTTSecondConnectPacket        = errors.New("received a second CONNECT packet")
	errMQTTAckWaitMustBePositive      = errors.New("ack wait must be a positive value")
	errMQTTJSAPITimeoutMustBePositive = errors.New("JS API timeout must be a positive value")
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
	errMQTTMalformedSubOption           = errors.New("malformed MQTT 5.0 subscription option")
	errMQTTMalformedSharedSub           = errors.New("malformed shared subscription")
	errMQTTTopicAliasInvalid            = errors.New("topic alias invalid")
	errMQTTReceiveMaxExceeded           = errors.New("receive maximum exceeded")
	errMQTTPayloadFormatInvalid         = errors.New("payload format invalid")
	errMQTTNotAuthorizedToSubscribe     = errors.New("not authorized to subscribe")
	// errMQTTPublishNotAuthorized ends the connection. Only a 3.1.1 publisher ever
	// sees it: MQTT 5 gets reason 0x87 on its PUBACK/PUBREC and stays connected,
	// while 3.1.1 has no per-publish refusal code and [3.3.5.2] offers the close as
	// the alternative. See mqttUnauthorizedPublishAnswer.
	errMQTTPublishNotAuthorized = errors.New("not authorized to publish")
	// errMQTTServiceEpochMoved says the service connection's account epoch changed
	// between authorizing a publication and writing it. The write is abandoned
	// rather than handed to an account it was never authorized against.
	errMQTTServiceEpochMoved = errors.New("the service connection's account epoch moved before the write")
	// errMQTTWillFanOutFailed says a QoS 0 publication's fan-out did not leave the
	// connection. It exists so the delayed-Will worker leaves its outbox record
	// RETRYABLE instead of finalizing a Will nobody received.
	errMQTTWillFanOutFailed = errors.New("qos 0 fan-out failed")
	// errMQTTRetainedStoreUnconfirmed says the store did not confirm a publication's
	// retained update: the old value may still be served, a deletion may not have
	// happened, or the write may have landed with its reply lost. An accepted,
	// non-expired publication whose retained update is required is never given final
	// success -- PUBACK, PUBCOMP, or a nil return to a Will worker -- while this
	// stands. The CLIENT re-sends; the broker does not re-attempt on its own, because
	// a retry lands at a newer sequence and can overwrite another publisher's newer
	// value. F-060.
	errMQTTRetainedStoreUnconfirmed = errors.New("the retained update was not confirmed by the store")
	errMQTTMalformedPacket          = errors.New("malformed packet")
)

type srvMQTT struct {
	listener net.Listener
	// wsListener is the MQTT-over-WebSocket listener; nil when it is disabled.
	wsListener net.Listener
	// plainListener is the plaintext MQTT listener beside a TLS one; nil when it is disabled.
	plainListener net.Listener
	listenerErr   error
	sessmgr       mqttSessionManager
	// originKey is a per-server-run random secret keying the HMACs that
	// authenticate No Local: the stamp's per-filter tags on delivered and stored
	// messages (nolocal_stamp.go) and the origin marker on retained messages. It
	// is never sent to clients, so a client that can write
	// the MQTT stream subjects still cannot forge a marker for a victim's session;
	// an unauthenticated or unverifiable origin is simply not suppressed (No Local
	// fails open, never dropping a legitimate message). Because the key is not
	// persisted or shared across the cluster, a self-published message that
	// outlives its stamping server-run — a retained/QoS message replayed after a
	// restart, or delivered after the persistent session reconnects to another
	// node — no longer verifies and is delivered rather than suppressed. That is
	// an accepted, safe degradation of No Local for persisted messages across a
	// restart/failover, not a security gap, stated in docs/CONFORMANCE.md.
	// Spec5 [3.8.3.1].
	originKey []byte
	// ephemeralID NAMES this server run's per-account ephemeral consumers, and
	// ephemeralOwner WITNESSES its ownership of them. Two INDEPENDENT draws, minted
	// lazily by ephemeralIdentity, which is where the reason for each lives. See
	// ephemeral_identity.go.
	ephemeralID    string
	ephemeralOwner string
	// rmObsGen numbers the retained observer's delivery generations for the whole run,
	// across initialization attempts and resets, so no two of them ever share a deliver
	// subject or a sid. See retained_index.go.
	rmObsGen atomic.Uint64
}

type mqtt struct {
	r    *mqttReader
	cp   *mqttConnectProto
	pp   *mqttPublish
	asm  *mqttAccountSessionManager // quick reference to account session manager, immutable after processConnect()
	sess *mqttSession               // quick reference to session, immutable after processConnect()
	cid  string                     // client ID

	// tracker is the incarnation this connection was BOUND to (F-036 unit 1; see
	// sess_lease.go). Set at each bind, in the same sess.mu section as sess.c = c,
	// and deliberately NEVER cleared: a displaced packet arriving after the detach
	// must still acquire the OLD, retired tracker and be refused there — clearing it
	// would leave nothing to discriminate against, and reading sess.leases instead
	// would let a displaced route acquire the FRESH incarnation it must not serve.
	tracker *mqttIncarnationTracker

	// outEnt is the outbound entitlement this connection was ACCEPTED under: the generation and
	// tenure its CONNECT's ownership drive established as serving. Written once, under c.mu, when
	// the accept loop reaches a servable outcome, and never rewritten. Every ordinary acquisition
	// names THIS pair rather than whatever the store serves when it asks, so a connection whose
	// tenure a takeover ended can never acquire output under its successor's (F-105 row 2). Zero
	// means no accepted entitlement, and an ordinary acquisition then refuses.
	outEnt mqttOutboundEntitlement

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
	// exactly once by mqttTakeWillLocked. See mqttHandleClosedClient,
	// mqttHandleWill.
	willDisposition mqttWillDisposition

	// lifetimeOverride is the Session Expiry Interval a v5 DISCONNECT on this
	// connection instructed the session to end on, or nil when no DISCONNECT
	// carried the property. Spec5 [3.14.2.2.2].
	//
	// It is an OBLIGATION of this accepted connection, and the whole point of its
	// existing separately is that it is neither of the two facts it would
	// otherwise be confused with. Not the session's CONFIRMED terms
	// (sess.expiryInterval / sess.clean), which move only under the ownership
	// gate. And not a CONNECT's REQUESTED terms, which sessionExpiry above also
	// carries and which the broker may still refuse -- those must never reach the
	// close, which is what mqttAbortAcceptedConnect exists to guarantee. Because
	// this is written only by the DISCONNECT packet handler, a connection whose
	// CONNECT was refused never has one. F-041.
	//
	// Written and read only under c.mu, never marshalled, never crossing a
	// broker.
	lifetimeOverride *uint32

	// closeObservedAt is when this connection's close was observed, in unix
	// nanoseconds, recorded by the same transition that makes the Will owed. It is
	// the origin the Will Delay Interval runs from: Spec5 [3.1.3.2.2] measures the
	// delay from the Network Connection ending, not from whenever a teardown path
	// gets round to consuming the Will -- and between the two sit the session gate,
	// a clean-session discard and two JetStream round trips.
	//
	// Nonzero exactly while a Will is owed. Zero means no close has been separately
	// observed, so a route consuming a still-ARMED Will is itself the close
	// observation and its own instant is the anchor.
	//
	// It lives here rather than on the session for the reason sessionExpiry above
	// does: a takeover rebinds the session before the displaced connection has
	// finished closing, so a session-held value would pair one connection's close
	// with another connection's Will.
	//
	// Written, restored and cleared only under c.mu, and consumed with the Will by
	// mqttTakeWillLocked -- the Will, the disposition it was consumed in, and the
	// close it is measured from are ONE fact, so nothing may read one without the
	// others.
	closeObservedAt int64

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
	// the lock and attempting its capped write after releasing it. The guarantee
	// is at-most-one ATTEMPT: the winner's write stays best-effort (a concurrent
	// close can fail it, and the shutdown drain can spend the claim and then
	// write nothing at all), never exactly-once delivery.
	disconnectSent bool

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

	// inPipe is this connection's inbound acknowledgement pipeline (inbound_pipeline.go):
	// the FIFO of inbound PUBLISH and PUBREL work, and the Receive Maximum ledger that both
	// the direct and the pipelined path share. Created by the readLoop on the first
	// PUBLISH or PUBREL and never replaced. Read by the readLoop without a lock; every
	// other access goes through the pipe's own mutex.
	inPipe *mqttInboundPipe

	// inAbort is the connection's ABORT LATCH: closed exactly once (inAbortOnce) when the
	// connection's transport is closed (closeTransport), when an inbound job's failure
	// reaches the emission head, or when the readLoop exits. It belongs to the connection
	// rather than to the pipe because a close can come first: a pipe created afterwards
	// reads an already-closed latch and starts aborted. Every inbound-pipeline wait, the
	// direct path's subs barrier included, selects on it, so nothing a readLoop or a pipe
	// goroutine waits on can outlive the socket.
	//
	// inAbortMu is a leaf lock (nothing is acquired under it), so the latch can be fired by
	// a caller holding c.mu, as queueOutbound's close does. The channel is made by
	// createMQTTClient; abortLatch makes it on first use for a client built any other way.
	inAbortMu    sync.Mutex
	inAbort      chan struct{}
	inAbortFired bool

	// experimentalSharedQoS2 enables the parked session-affine shared QoS 2
	// path. Production leaves this false, capping only shared subscriptions at
	// QoS 1. Tests of the preserved experimental implementation opt in.
	experimentalSharedQoS2 bool
}
