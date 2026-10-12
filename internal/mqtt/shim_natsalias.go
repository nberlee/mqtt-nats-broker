// Code generated shim: aliases to nats-server exported symbols. DO NOT hand-edit lightly.
package mqtt

import server "github.com/nats-io/nats-server/v2/server"

type (
	closedStateAlias            = server.ClosedState
	ApiError                    = server.ApiError
	ApiPagedRequest             = server.ApiPagedRequest
	ApiResponse                 = server.ApiResponse
	ClientInfo                  = server.ClientInfo
	ConsumerConfig              = server.ConsumerConfig
	ConsumerInfo                = server.ConsumerInfo
	CreateConsumerRequest       = server.CreateConsumerRequest
	ErrorIdentifier             = server.ErrorIdentifier
	PubAck                      = server.PubAck
	JSApiConsumerCreateResponse = server.JSApiConsumerCreateResponse
	JSApiConsumerDeleteResponse = server.JSApiConsumerDeleteResponse
	JSApiConsumerInfoResponse   = server.JSApiConsumerInfoResponse
	JSApiConsumerListResponse   = server.JSApiConsumerListResponse
	JSApiConsumersRequest       = server.JSApiConsumersRequest
	JSApiMsgDeleteRequest       = server.JSApiMsgDeleteRequest
	JSApiMsgDeleteResponse      = server.JSApiMsgDeleteResponse
	JSApiMsgGetRequest          = server.JSApiMsgGetRequest
	JSApiMsgGetResponse         = server.JSApiMsgGetResponse
	JSApiStreamCreateResponse   = server.JSApiStreamCreateResponse
	JSApiStreamDeleteResponse   = server.JSApiStreamDeleteResponse
	JSApiStreamInfoRequest      = server.JSApiStreamInfoRequest
	JSApiStreamInfoResponse     = server.JSApiStreamInfoResponse
	JSApiStreamNamesResponse    = server.JSApiStreamNamesResponse
	JSApiStreamPurgeRequest     = server.JSApiStreamPurgeRequest
	JSApiStreamPurgeResponse    = server.JSApiStreamPurgeResponse
	JSApiStreamUpdateResponse   = server.JSApiStreamUpdateResponse
	JSPubAckResponse            = server.JSPubAckResponse
	NkeyUser                    = server.NkeyUser
	Permissions                 = server.Permissions
	Placement                   = server.Placement
	RePublish                   = server.RePublish
	SubjectPermission           = server.SubjectPermission
	SubjectTransformConfig      = server.SubjectTransformConfig
	StoredMsg                   = server.StoredMsg
	StreamConfig                = server.StreamConfig
	StreamInfo                  = server.StreamInfo
	StreamState                 = server.StreamState
	StreamSource                = server.StreamSource
	User                        = server.User
)

const (
	// ActionCreate makes a named consumer create REFUSE an existing consumer whose
	// config differs, instead of adopting it at its current position as the zero
	// value (create-or-update) does. See ephemeral_identity.go.
	ActionCreate              = server.ActionCreate
	AckExplicit               = server.AckExplicit
	AckNone                   = server.AckNone
	AsyncPersistMode          = server.AsyncPersistMode
	ClientClosed              = server.ClientClosed
	ClientInfoHdr             = server.ClientInfoHdr
	CR_LF                     = server.CR_LF
	MAX_CONTROL_LINE_SIZE     = server.MAX_CONTROL_LINE_SIZE
	DefaultPersistMode        = server.DefaultPersistMode
	DeliverNew                = server.DeliverNew
	DiscardNew                = server.DiscardNew
	DiscardOld                = server.DiscardOld
	DuplicateClientID         = server.DuplicateClientID
	FileStorage               = server.FileStorage
	InterestPolicy            = server.InterestPolicy
	JETSTREAM                 = server.JETSTREAM
	JSApiConsumerCreateT      = server.JSApiConsumerCreateT
	JSApiConsumerDeleteT      = server.JSApiConsumerDeleteT
	JSApiConsumerInfoT        = server.JSApiConsumerInfoT
	JSApiConsumerListT        = server.JSApiConsumerListT
	JSApiDurableCreateT       = server.JSApiDurableCreateT
	JSApiMsgDeleteT           = server.JSApiMsgDeleteT
	JSApiMsgGetT              = server.JSApiMsgGetT
	JSApiPrefix               = server.JSApiPrefix
	JSApiStreamCreateT        = server.JSApiStreamCreateT
	JSApiStreamDeleteT        = server.JSApiStreamDeleteT
	JSApiStreamInfoT          = server.JSApiStreamInfoT
	JSApiStreamPurgeT         = server.JSApiStreamPurgeT
	JSApiStreams              = server.JSApiStreams
	JSApiStreamUpdateT        = server.JSApiStreamUpdateT
	JSBatchCommit             = server.JSBatchCommit
	JSBatchId                 = server.JSBatchId
	JSBatchSeq                = server.JSBatchSeq
	JSConsumerCreateErrF      = server.JSConsumerCreateErrF
	JSConsumerNotFoundErr     = server.JSConsumerNotFoundErr
	JSExpectedLastSubjSeq     = server.JSExpectedLastSubjSeq
	JSMessageTTL              = server.JSMessageTTL
	JSMsgId                   = server.JSMsgId
	JSNoMessageFoundErr       = server.JSNoMessageFoundErr
	JSSequenceNotFoundErrF    = server.JSSequenceNotFoundErrF
	JSStreamMsgDeleteFailedF  = server.JSStreamMsgDeleteFailedF
	JSStreamNameExistErr      = server.JSStreamNameExistErr
	JSStreamNotFoundErr       = server.JSStreamNotFoundErr
	JSBadRequestErr           = server.JSBadRequestErr
	JSNotEnabledErr           = server.JSNotEnabledErr
	JSNotEnabledForAccountErr = server.JSNotEnabledForAccountErr
	JSStreamStoreFailedF      = server.JSStreamStoreFailedF
	// The two structural outcomes of a rejected expected-last-subject-sequence
	// CAS: 10071 from the single-writer check, 10164 from the clustered inflight
	// contention branches. Both mean "the subject is not at the expected
	// sequence", never "the write landed".
	JSStreamWrongLastSequenceConstantErr = server.JSStreamWrongLastSequenceConstantErr
	JSStreamWrongLastSequenceErrF        = server.JSStreamWrongLastSequenceErrF
	LEN_CR_LF                            = server.LEN_CR_LF
	LimitsPolicy                         = server.LimitsPolicy
	MemoryStorage                        = server.MemoryStorage
	ReplayInstant                        = server.ReplayInstant
	S2Compression                        = server.S2Compression
	ServerShutdown                       = server.ServerShutdown
	StaleConnection                      = server.StaleConnection
	TLSHandshakeError                    = server.TLSHandshakeError
	WriteError                           = server.WriteError

	// The retained observer's resume policy, the idle-heartbeat header naming the last
	// consumer delivery sequence, and the header every JetStream subject delete marker
	// carries. See retained_index.go.
	DeliverByStartSequence = server.DeliverByStartSequence
	JSLastConsumerSeq      = server.JSLastConsumerSeq
	JSMarkerReason         = server.JSMarkerReason
)

var (
	AckAck                = server.AckAck
	AckNak                = server.AckNak
	ApiErrors             = server.ApiErrors
	ErrAuthentication     = server.ErrAuthentication
	ErrConnectionClosed   = server.ErrConnectionClosed
	ErrMaxControlLine     = server.ErrMaxControlLine
	ErrMaxMsgsPerSubject  = server.ErrMaxMsgsPerSubject
	ErrMaxPayload         = server.ErrMaxPayload
	ErrServerNotRunning   = server.ErrServerNotRunning
	ErrStoreMsgNotFound   = server.ErrStoreMsgNotFound
	IsNatsErr             = server.IsNatsErr
	NewJSInvalidJSONError = server.NewJSInvalidJSONError
)

// The metadata keys nats-server sets on an asset of its own accord.
//
// A separate block, deliberately: these names are longer than every alias above, and
// folding them into that block makes gofmt re-align ninety lines of it for three
// additions — churn that buries the real change in a file whose header asks for light
// edits.
//
// Named rather than matched by their shared "_nats." prefix, so a key a future server
// adds fails a test and forces a decision instead of being silently absorbed. See
// sharedGroupMetadataIsServerOnly.
const (
	JSRequiredLevelMetadataKey = server.JSRequiredLevelMetadataKey
	JSServerVersionMetadataKey = server.JSServerVersionMetadataKey
	JSServerLevelMetadataKey   = server.JSServerLevelMetadataKey
)
