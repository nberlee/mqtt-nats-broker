// Code generated shim: aliases to nats-server exported symbols. DO NOT hand-edit lightly.
package mqtt

import server "github.com/nats-io/nats-server/v2/server"

type (
	closedStateAlias            = server.ClosedState
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
	JSApiConsumerListResponse   = server.JSApiConsumerListResponse
	JSApiConsumersRequest       = server.JSApiConsumersRequest
	JSApiMsgDeleteRequest       = server.JSApiMsgDeleteRequest
	JSApiMsgDeleteResponse      = server.JSApiMsgDeleteResponse
	JSApiMsgGetRequest          = server.JSApiMsgGetRequest
	JSApiMsgGetResponse         = server.JSApiMsgGetResponse
	JSApiStreamCreateResponse   = server.JSApiStreamCreateResponse
	JSApiStreamDeleteResponse   = server.JSApiStreamDeleteResponse
	JSApiStreamInfoResponse     = server.JSApiStreamInfoResponse
	JSApiStreamNamesResponse    = server.JSApiStreamNamesResponse
	JSApiStreamPurgeRequest     = server.JSApiStreamPurgeRequest
	JSApiStreamPurgeResponse    = server.JSApiStreamPurgeResponse
	JSApiStreamUpdateResponse   = server.JSApiStreamUpdateResponse
	JSPubAckResponse            = server.JSPubAckResponse
	NkeyUser                    = server.NkeyUser
	Permissions                 = server.Permissions
	SubjectPermission           = server.SubjectPermission
	StoredMsg                   = server.StoredMsg
	StreamConfig                = server.StreamConfig
	StreamInfo                  = server.StreamInfo
	StreamSource                = server.StreamSource
	User                        = server.User
)

const (
	AckExplicit              = server.AckExplicit
	AckNone                  = server.AckNone
	ClientClosed             = server.ClientClosed
	ClientInfoHdr            = server.ClientInfoHdr
	CR_LF                    = server.CR_LF
	MAX_CONTROL_LINE_SIZE    = server.MAX_CONTROL_LINE_SIZE
	DeliverNew               = server.DeliverNew
	DiscardNew               = server.DiscardNew
	DiscardOld               = server.DiscardOld
	DuplicateClientID        = server.DuplicateClientID
	FileStorage              = server.FileStorage
	InterestPolicy           = server.InterestPolicy
	JETSTREAM                = server.JETSTREAM
	JSApiConsumerCreateT     = server.JSApiConsumerCreateT
	JSApiConsumerDeleteT     = server.JSApiConsumerDeleteT
	JSApiConsumerListT       = server.JSApiConsumerListT
	JSApiDurableCreateT      = server.JSApiDurableCreateT
	JSApiMsgDeleteT          = server.JSApiMsgDeleteT
	JSApiMsgGetT             = server.JSApiMsgGetT
	JSApiPrefix              = server.JSApiPrefix
	JSApiStreamCreateT       = server.JSApiStreamCreateT
	JSApiStreamDeleteT       = server.JSApiStreamDeleteT
	JSApiStreamInfoT         = server.JSApiStreamInfoT
	JSApiStreamPurgeT        = server.JSApiStreamPurgeT
	JSApiStreams             = server.JSApiStreams
	JSApiStreamUpdateT       = server.JSApiStreamUpdateT
	JSConsumerNotFoundErr    = server.JSConsumerNotFoundErr
	JSExpectedLastSubjSeq    = server.JSExpectedLastSubjSeq
	JSMessageTTL             = server.JSMessageTTL
	JSNoMessageFoundErr      = server.JSNoMessageFoundErr
	JSSequenceNotFoundErrF   = server.JSSequenceNotFoundErrF
	JSStreamMsgDeleteFailedF = server.JSStreamMsgDeleteFailedF
	JSStreamNameExistErr     = server.JSStreamNameExistErr
	JSStreamNotFoundErr      = server.JSStreamNotFoundErr
	JSStreamStoreFailedF     = server.JSStreamStoreFailedF
	LEN_CR_LF                = server.LEN_CR_LF
	LimitsPolicy             = server.LimitsPolicy
	MemoryStorage            = server.MemoryStorage
	ReplayInstant            = server.ReplayInstant
	ServerShutdown           = server.ServerShutdown
	TLSHandshakeError        = server.TLSHandshakeError
	WriteError               = server.WriteError
)

var (
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
