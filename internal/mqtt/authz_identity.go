package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// The authenticated identity's NATS account and permissions, which the broker
// cannot otherwise learn.
//
// Auth passthrough dials a per-client NATS connection so NATS can enforce that
// client's rights, and for SUBSCRIBE admission it does. Everything else — the
// $MQTT_msgs store, the retained store, the QoS 2 hold, QoS 1/2 delivery,
// retained replay, and an internally replayed Will — rides the SERVICE
// connection, which holds full rights by design. So the broker has to enforce
// those itself, and to enforce anything it first has to know who the client is.
//
// $SYS.REQ.USER.INFO answers exactly that, on the client's own connection: the
// bound account (F-080) and both permission sets (F-079 publish, F-081
// subscribe). One request per CONNECT, before the CONNECT is accepted.
//
// Every unknown refuses. A snapshot is only ever a POSITIVE statement about an
// identity; an absent, malformed, expired or unverifiable one is not a weaker
// permission set, it is no identity at all.
const mqttUserInfoSubject = "$SYS.REQ.USER.INFO"

// mqttUserInfoInboxPrefix keeps the identity reply inside the reserved namespace,
// where a client can neither subscribe to it nor receive it through a wildcard.
// See mqttFetchIdentity for why the default "_INBOX" prefix is not usable here.
//
// IT DOES NOT MAKE THE REPLY UNFORGEABLE, and nothing here can. The reply is an
// unsigned message on an account subject, and a request to $SYS.REQ.USER.INFO is
// observable to an account subscriber (measured), so a principal able to publish
// into this namespace can race the server and answer for another client.
//
// This CANNOT be checked from the reply, and an earlier revision that tried was
// circular: any test applied to the claimed permissions is a test the forger also
// controls -- a reply claiming "allow >, deny <this exact inbox>" satisfies "the
// identity could not have published this" while granting everything else. The
// check was removed rather than weakened, because a test an attacker can satisfy
// is worse than none: it invites trust it cannot carry.
//
// What makes it safe is the deployment, and the requirement is not new. MQTT
// identities must be denied publish on "$MQTT.>", which docs/ARCHITECTURE.md has
// recommended since the reserved namespace existed -- and for a strictly stronger
// reason than this one: a principal who can publish there can already forge
// session records and JSA traffic directly, so identity spoofing gains it nothing
// it did not already have. The prerequisite is stated in docs/ARCHITECTURE.md and
// docs/CONFORMANCE.md as a security requirement rather than pretended to be
// enforced.
const mqttUserInfoInboxPrefix = mqttPrefix + "uinfo."

// mqttUserInfoTimeout bounds the identity request. It sits on the CONNECT path,
// which the CONNECT deadline already bounds, so it must be comfortably shorter
// than that: a request that outlives the deadline turns a refusable CONNECT into
// a closed socket with no CONNACK.
const mqttUserInfoTimeout = 2 * time.Second

// mqttIdentity is one connection's authenticated identity, taken once and never
// refreshed. Its validity is the per-client NATS connection's validity, not a
// clock: see mqttBindClientIdentity.
type mqttIdentity struct {
	user    string
	account string
	pub     *mqttSubjectPermissions
	sub     *mqttSubjectPermissions
	// expires is the remaining lifetime the server reported, or 0. ZERO IS
	// AMBIGUOUS — nats-server truncates the remaining duration to seconds
	// (server/client.go), so 0 means both "no expiry" and "under a second left".
	// It is therefore never read as "unlimited": a nonzero value bounds the
	// snapshot, and zero bounds nothing, with the connection lifecycle carrying
	// the guarantee instead.
	expires time.Duration
}

// mqttSubjectPermissions is one direction's allow/deny lists, already validated.
//
// Deny wins over allow, as in NATS. A nil receiver means "no permissions were
// configured for this identity", which the server reports by omitting the object
// entirely and which means UNRESTRICTED — reading it as "deny everything" would
// refuse every publish on a server with no ACLs, i.e. the default deployment.
type mqttSubjectPermissions struct {
	allow []mqttPermFilter
	deny  []mqttPermFilter
}

// mqttPermFilter is one permission entry. A subscribe deny may be queue-scoped,
// which USER.INFO serialises as "<subject> <queue>"; publish entries never are.
type mqttPermFilter struct {
	subject string
	queue   string // empty when the entry is not queue-scoped
}

// mqttUserInfoResponse is the envelope $SYS.REQ.USER.INFO answers with. Only the
// two fields this broker acts on are decoded.
type mqttUserInfoResponse struct {
	Data  *server.UserInfo `json:"data"`
	Error *ApiError        `json:"error"`
}

// mqttFetchIdentity asks nc who it is and returns a validated snapshot.
//
// Every failure is an error, never a permissive default. The caller refuses the
// CONNECT on any of them.
//
// replyOn is the connection the ANSWER is received on, and it is deliberately not
// nc. The server decides whose identity to report from who PUBLISHED the request,
// so the reply can be collected anywhere -- and it must not be collected on nc,
// because nc also carries this client's own MQTT subscriptions. An MQTT client
// subscribed to "#" maps to the NATS wildcard ">", which matches "_INBOX.>" and
// therefore matches EVERY client's reply inbox in the account: the default
// request/reply inbox would hand one client another client's identity and
// permission lists as an ordinary MQTT message.
//
// Collecting on the service connection under "$MQTT." fixes that for two
// independent reasons, either of which suffices: the recommended client ACL denies
// the whole reserved prefix, and the broker drops "$"-leading subjects from
// wildcard subscriptions anyway ([MQTT-4.7.2-1]).
func mqttFetchIdentity(nc, replyOn *nats.Conn) (*mqttIdentity, error) {
	if nc == nil || replyOn == nil {
		return nil, fmt.Errorf("no NATS connection to identify")
	}
	inbox := mqttUserInfoInboxPrefix + nuid.Next()
	sub, err := replyOn.SubscribeSync(inbox)
	if err != nil {
		return nil, fmt.Errorf("subscribing to the identity reply inbox: %w", err)
	}
	defer sub.Unsubscribe() //nolint:errcheck // best effort; the subscription is short-lived
	// The interest must exist before the request, or a fast reply is lost and the
	// CONNECT is refused for a reason that is not the client's.
	if ferr := replyOn.Flush(); ferr != nil {
		return nil, fmt.Errorf("registering the identity reply inbox: %w", ferr)
	}
	if perr := nc.PublishRequest(mqttUserInfoSubject, inbox, nil); perr != nil {
		return nil, fmt.Errorf("%s: %w", mqttUserInfoSubject, perr)
	}
	msg, err := sub.NextMsg(mqttUserInfoTimeout)
	if err != nil {
		// A client denied publish on the subject gets a timeout here with the
		// violation in LastError, so naming both is what lets the refusal say
		// which of the two it was.
		if le := nc.LastError(); le != nil {
			return nil, fmt.Errorf("%s: %w (connection reported: %v)", mqttUserInfoSubject, err, le)
		}
		return nil, fmt.Errorf("%s: %w", mqttUserInfoSubject, err)
	}
	body := msg.Data
	// The seam substitutes the RAW RESPONSE, not the parsed identity, so every
	// validation below still runs against it. Substituting the parsed result would
	// bypass the checks the seam exists to exercise -- which it did, until a run of
	// the paired test caught the empty-user row passing.
	if h := mqttTestUserInfoHookV.Load(); h != nil {
		sub, herr := (*h)()
		if herr != nil {
			return nil, herr
		}
		body = sub
	}
	var resp mqttUserInfoResponse
	if uerr := json.Unmarshal(body, &resp); uerr != nil {
		return nil, fmt.Errorf("%s returned an undecodable body: %w", mqttUserInfoSubject, uerr)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("%s returned an error: %v", mqttUserInfoSubject, resp.Error)
	}
	ui := resp.Data
	if ui == nil {
		return nil, fmt.Errorf("%s returned no identity", mqttUserInfoSubject)
	}
	// An empty user id is refused rather than accepted as an anonymous
	// principal: account equality alone would then admit an identity the server
	// could not name.
	if ui.UserID == _EMPTY_ {
		return nil, fmt.Errorf("%s named no user", mqttUserInfoSubject)
	}
	if ui.Account == _EMPTY_ {
		return nil, fmt.Errorf("%s named no account", mqttUserInfoSubject)
	}
	// A NEGATIVE remaining lifetime is an identity the server already considers
	// expired, and it is refused rather than clamped. Zero is NOT that: nats-server
	// truncates the remainder to seconds, so zero means both "no expiry" and "under
	// a second left", and the connection lifecycle is what bounds that case.
	if ui.Expires < 0 {
		return nil, fmt.Errorf("%s reported an identity whose lifetime has already elapsed (%v)", mqttUserInfoSubject, ui.Expires)
	}
	id := &mqttIdentity{user: ui.UserID, account: ui.Account, expires: ui.Expires}
	// A nil Permissions is the shape a server with no ACLs emits and means
	// unrestricted. A NON-nil Permissions missing a child is NOT a shape the
	// server emits, so it is a malformed body and is refused rather than read as
	// unrestricted in that direction.
	if ui.Permissions != nil {
		if ui.Permissions.Publish == nil || ui.Permissions.Subscribe == nil {
			return nil, fmt.Errorf("%s returned a permissions object missing a direction", mqttUserInfoSubject)
		}
		if id.pub, err = mqttCompilePermissions(ui.Permissions.Publish, false); err != nil {
			return nil, fmt.Errorf("publish permissions: %w", err)
		}
		if id.sub, err = mqttCompilePermissions(ui.Permissions.Subscribe, true); err != nil {
			return nil, fmt.Errorf("subscribe permissions: %w", err)
		}
	}
	return id, nil
}

// mqttCompilePermissions validates and compiles one direction.
//
// Validation is not decoration: an unparseable deny that merely "matches
// nothing" is a fail-open, so a malformed entry refuses the identity outright.
func mqttCompilePermissions(sp *SubjectPermission, queueScoped bool) (*mqttSubjectPermissions, error) {
	if sp == nil {
		return nil, nil
	}
	out := &mqttSubjectPermissions{}
	var err error
	if out.allow, err = mqttCompilePermFilters(sp.Allow, queueScoped); err != nil {
		return nil, fmt.Errorf("allow: %w", err)
	}
	if out.deny, err = mqttCompilePermFilters(sp.Deny, queueScoped); err != nil {
		return nil, fmt.Errorf("deny: %w", err)
	}
	return out, nil
}

func mqttCompilePermFilters(in []string, queueScoped bool) ([]mqttPermFilter, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]mqttPermFilter, 0, len(in))
	for _, e := range in {
		f := mqttPermFilter{subject: e}
		if queueScoped {
			// "<subject> <queue>" is how a queue-scoped subscribe permission is
			// serialised. Publish permissions are never queue-scoped, so the split
			// is not attempted there and a space would be a malformed subject.
			if i := strings.IndexByte(e, ' '); i >= 0 {
				f.subject, f.queue = e[:i], strings.TrimSpace(e[i+1:])
			}
		}
		if !mqttIsValidSubjectFilter(f.subject) {
			return nil, fmt.Errorf("entry %q is not a valid subject filter", e)
		}
		out = append(out, f)
	}
	return out, nil
}

// mqttIsValidSubjectFilter rejects what SubjectMatchesFilter would silently
// never match: an empty filter, an empty token, whitespace, or a ">" anywhere
// but the final token.
func mqttIsValidSubjectFilter(f string) bool {
	if f == _EMPTY_ || strings.ContainsAny(f, " \t\r\n") {
		return false
	}
	toks := strings.Split(f, ".")
	for i, t := range toks {
		if t == _EMPTY_ {
			return false
		}
		if t == ">" && i != len(toks)-1 {
			return false
		}
	}
	return true
}

// allows reports whether subject is permitted in this direction. Deny wins.
//
// A nil receiver is unrestricted (see mqttSubjectPermissions).
func (p *mqttSubjectPermissions) allows(subject string) bool {
	if p == nil {
		return true
	}
	for _, d := range p.deny {
		// A queue-scoped deny does not deny the subject outright; it denies it
		// for one queue group, which only the delivery side can evaluate.
		if d.queue == _EMPTY_ && server.SubjectMatchesFilter(subject, d.subject) {
			return false
		}
	}
	if len(p.allow) == 0 {
		return true
	}
	for _, a := range p.allow {
		if server.SubjectMatchesFilter(subject, a.subject) {
			return true
		}
	}
	return false
}

// deniesSub reports whether a delivery of subject to a subscription in queue
// group queue is denied. Only the DENY side is evaluated: the allow side was
// already enforced by NATS when it accepted the subscription, and re-checking it
// here would refuse deliveries NATS admitted.
//
// A queue name of exactly "*" or ">" is compared LITERALLY before being treated
// as a wildcard, matching nats-server's own queue matching.
func (p *mqttSubjectPermissions) deniesSub(subject, queue string) bool {
	if p == nil {
		return false
	}
	for _, d := range p.deny {
		if !server.SubjectMatchesFilter(subject, d.subject) {
			continue
		}
		if d.queue == _EMPTY_ {
			return true // denies the subject for every queue, and for no queue
		}
		if queue == _EMPTY_ {
			continue // a queue-scoped deny says nothing about a plain subscription
		}
		if mqttQueueMatches(queue, d.queue) {
			return true
		}
	}
	return false
}

// mqttQueueMatches compares a subscription's queue group against a permission's
// queue pattern. An exact match wins first, so a group literally named "*" or
// ">" is not treated as a wildcard against itself.
func mqttQueueMatches(queue, pattern string) bool {
	if queue == pattern {
		return true
	}
	return server.SubjectMatchesFilter(queue, pattern)
}

// ---------------------------------------------------------------------------
// The service connection's own identity
// ---------------------------------------------------------------------------

// serviceIdentity is what the broker knows about the connection its streams ride.
//
// It is an epoch, not a cached string, and for the same reason the per-client one
// is: a disconnected service connection cannot tell us where a buffered write
// would land. nats.go resends subscriptions and FLUSHES ITS RECONNECT BUFFER
// before it queues the reconnect callback, so a callback that discovers an
// account change discovers it AFTER the effects have already been written into
// the new account. Only invalidating on the disconnect gets ahead of that.
type serviceIdentity struct {
	mu      sync.Mutex
	account string
	valid   bool
}

// serviceAccount returns the account the service connection is currently, and
// provably, bound to. A false second result means the broker does not know --
// which is refused, never assumed.
// serviceAccount returns the account the service connection is currently, and
// provably, bound to. A false second result means the broker does not know --
// which is refused, never assumed.
//
// The ACCOUNT is the whole answer. An earlier revision also carried a generation
// that advanced on every disconnect, so a snapshot could be pinned to one
// incarnation of the account; review showed that to be worse than the hazard it
// guarded, because an ordinary same-account reconnect then permanently stranded
// every live client -- their publications refused and their deliveries
// indeterminate forever, with no event that would ever restore them. What the
// invariant actually names is the ACCOUNT an identity was discovered against, so
// that is what is compared, and a reconnect into the same account is a non-event.
func (s *Server) serviceAccount() (string, bool) {
	// The LIVE connection state is authoritative and the flag is only a cache.
	//
	// Checking the flag alone loses to a check-then-set race that no ordering of
	// the handler can win: NATS serialises connection callbacks, so a transition
	// that happens while the reconnect handler is still running has its disconnect
	// handler QUEUED BEHIND it, and any "is it still connected?" test the handler
	// makes can go stale between the test and the assignment. Asking the connection
	// here instead means a reconnecting connection is never reported valid,
	// whatever the flag says and whichever callback is mid-flight.
	if s.nrc == nil || !s.nrc.IsConnected() {
		return _EMPTY_, false
	}
	s.svcID.mu.Lock()
	defer s.svcID.mu.Unlock()
	if !s.svcID.valid {
		return _EMPTY_, false
	}
	return s.svcID.account, true
}

// establishServiceIdentity discovers the service connection's account and arms
// the handlers that keep that knowledge honest. It runs at Start, BEFORE the MQTT
// listener opens, so no connection is ever accepted that the broker could not
// judge.
//
// Only meaningful under auth passthrough: with no dialer there is no per-client
// identity to compare against, so there is nothing to establish.
func (s *Server) establishServiceIdentity() error {
	// Armed before the request, so a drop while it is in flight is observed
	// rather than missed; the IsConnected checks close the window neither setter
	// covers.
	s.armServiceIdentityHandlers()
	if !s.nrc.IsConnected() {
		return fmt.Errorf("the service connection is not connected")
	}
	id, err := mqttFetchIdentity(s.nrc, s.nrc)
	if err != nil {
		return fmt.Errorf("identifying the service connection: %w", err)
	}
	if !s.nrc.IsConnected() {
		return fmt.Errorf("the service connection dropped while being identified")
	}
	s.svcID.mu.Lock()
	s.svcID.account, s.svcID.valid = id.account, true
	s.svcID.mu.Unlock()
	return nil
}

func (s *Server) armServiceIdentityHandlers() {
	s.nrc.SetDisconnectErrHandler(func(*nats.Conn, error) {
		// A disconnect IS the indeterminate case. Every publication gate and every
		// delivery guard consults the epoch, so this is what stops an effect being
		// produced at all -- rather than discovering on reconnect that it was
		// buffered into the wrong account.
		s.svcID.mu.Lock()
		s.svcID.valid = false
		s.svcID.mu.Unlock()
	})
	s.nrc.SetReconnectHandler(func(nc *nats.Conn) {
		id, err := mqttFetchIdentity(nc, nc)
		if err != nil {
			s.Warnf("MQTT: the service connection reconnected but could not be identified, so no CONNECT is accepted: %v", err)
			return
		}
		// Re-checked before believing the answer. NATS serialises connection
		// callbacks, so a disconnect that happens while this one is still running
		// -- including during the request above -- has its handler QUEUED behind
		// us: setting valid here without asking would revalidate a connection that
		// is already gone, and publication gates would accept the stale account
		// until the queued handler finally ran.
		if !nc.IsConnected() {
			s.Warnf("MQTT: the service connection dropped again while it was being re-identified; leaving the account unknown")
			return
		}
		s.svcID.mu.Lock()
		was := s.svcID.account
		same := was == id.account
		if same {
			// The same account is the same destination, so nothing a live client
			// was authorized against has changed and none of them is disturbed.
			s.svcID.valid = true
		}
		s.svcID.mu.Unlock()
		if same {
			// The destination is back and unchanged, so any Will held across the
			// outage may now be published. It takes its own admission token rather
			// than running inline here, because this callback can complete long
			// after shutdown closed the dispatch gate. See will_blip_retry.go.
			s.kickOwedImmediateWills()
			s.kickOwedDelayedWills()
			return
		}
		// Not a routine event: the broker's streams are account-scoped, so a
		// different account is a different broker. Existing clients are cut loose
		// rather than left writing into it.
		s.Errorf("MQTT: the service connection reconnected into account %q, not %q; disconnecting every auth-passthrough client", id.account, was)
		s.invalidateAuthPassthroughClients()
	})
}

// invalidateAuthPassthroughClients revokes every per-client snapshot and closes
// those sockets. Refusing only NEW connects would leave the already-connected
// ones publishing into streams that are no longer the ones they were authorized
// against.
func (s *Server) invalidateAuthPassthroughClients() {
	s.mu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for _, c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()
	for _, c := range clients {
		c.mu.Lock()
		affected := c.mqttAuthzApplies()
		if affected {
			c.authzValid = false
		}
		c.mu.Unlock()
		if affected {
			c.closeTransport()
		}
	}
}

// ---------------------------------------------------------------------------
// The publication predicate
// ---------------------------------------------------------------------------

// mqttEffectivePublishSubjects returns EVERY NATS subject a publication will
// actually write to, which is the set the identity must be authorized for before
// any of it happens.
//
// It is a set and not a subject because this finding's history is that each
// review round produced another route: a Sparkplug birth publication also derives
// and stores a "$sparkplug/certificates/..." subject on the SERVICE JSA, and a
// Will whose topic happens to be a birth topic is one publication with two
// effective subjects. Authorizing the one the client named would leave the other
// unpoliced -- which is the whole defect, one level down.
//
// Written over the publication rather than per producer, so a new producer
// inherits the rule instead of needing a new entry in a list.
func mqttEffectivePublishSubjects(pp *mqttPublish) []string {
	if pp == nil || len(pp.subject) == 0 {
		return nil
	}
	subjects := []string{string(pp.subject)}
	// The retained hook keys off the TOPIC SHAPE, not off any flag the publisher
	// set, so the derived subject appears for anything that looks like a birth --
	// an ordinary PUBLISH, an immediate Will, a replayed Will. Mirrors the
	// condition in mqttHandlePubRetain.
	if isBirth, _, isCertificate := sparkbParseBirthDeathTopic(pp.topic); isBirth && !isCertificate {
		derived := make([]byte, 0, len(sparkbCertificatesTopicPrefix)+len(pp.topic))
		derived = append(derived, sparkbCertificatesTopicPrefix...)
		derived = append(derived, pp.topic...)
		if subj, err := mqttTopicToNATSPubSubject(derived); err == nil {
			subjects = append(subjects, string(subj))
		}
		// A derived topic that will not convert cannot be published either, so
		// there is nothing to authorize and nothing to refuse for it.
	}
	return subjects
}

// mqttPublicationAuthorized reports whether c may create EVERY effect this
// publication would create.
//
// All or nothing: a publication is one act, and authorizing part of it would
// leave the client's message half-applied with no way to say so.
func (c *client) mqttPublicationAuthorized(pp *mqttPublish) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mqttPublicationAuthorizedLocked(pp)
}

func (c *client) mqttPublicationAuthorizedLocked(pp *mqttPublish) bool {
	for _, subj := range mqttEffectivePublishSubjects(pp) {
		if !c.pubAllowedLocked(subj) {
			return false
		}
	}
	return true
}

// mqttUnauthorizedPublishAnswer drops an unauthorized publication and decides what the
// publisher is told: the acknowledgement it returns takes the publication's place in the
// connection's response order, and a 3.1.1 client gets the error that closes the connection.
//
// MQTT 5 has a reason code for exactly this and uses it. MQTT 3.1.1 has none, and
// [3.3.5.2] offers two lawful outcomes: a positive acknowledgement, or closing the
// network connection. This takes the close. The acknowledgement is legal but it
// is the defect restated -- F-079's measured symptom was precisely that a denied
// QoS 1 publish came back PUBACK and a denied QoS 2 came back PUBREC/PUBCOMP, so
// answering "success" to a message we refused would leave the publisher exactly as
// misinformed as before.
//
// This deliberately DIVERGES from the reserved-"$MQTT.>" refusal a few lines
// above, which answers a 3.1.1 client with a success-shaped ack. Both are lawful;
// harmonising them would change behaviour this item was not asked to change.
func (c *client) mqttUnauthorizedPublishAnswer(pp *mqttPublish, qos byte) (mqttInboundResp, error) {
	subject := string(pp.subject)
	c.Warnf("Rejecting unauthorized PUBLISH to %q", subject)
	if c.mqtt.proto != mqttProtoLevel5 {
		return mqttInboundResp{}, fmt.Errorf("%w: publish to %q", errMQTTPublishNotAuthorized, subject)
	}
	if qos == 0 {
		// QoS 0 has no acknowledgement to be wrong about. Dropping it silently is
		// the whole remedy; a DISCONNECT is optional and buys nothing here.
		c.Debugf("Discarding unauthorized QoS0 PUBLISH to %q", subject)
	}
	return mqttPubAnswer(qos, pp.pi, mqttReasonNotAuthorized, "not authorized to publish to this topic"), nil
}

// mqttSharedQueueFromGroupDurable recovers a shared group's NATS queue name from
// its consumer durable.
//
// The durable is "$MQTT_SHARED_<qkey>_<inc>": the group key plus the PHYSICAL incarnation
// the group record mounted. The queue is the LOGICAL identity and depends on the key alone,
// so the recovery parses the tail and drops the incarnation — every incarnation of one group
// answers to one queue, which is what an authorization decision is about. The affine
// delivery path needs it because its own subscription carries no queue: it is created with
// a nil queue, so by delivery time sub.queue no longer names the group the copy belongs to,
// and a queue-scoped subscribe deny evaluated against it would silently degrade to a
// subject-only check.
//
// The second result is false when the durable is not one this broker generated — the
// pre-transfer shape with no incarnation included. That is not a denial and must not be
// treated as one -- see the caller.
func mqttSharedQueueFromGroupDurable(durable string) (string, bool) {
	tail, ok := strings.CutPrefix(durable, mqttSharedConsumerDurablePrefix)
	if !ok {
		return _EMPTY_, false
	}
	qkey, _, ok := mqttParseSharedIncName(tail)
	if !ok {
		return _EMPTY_, false
	}
	return mqttSharedQueuePrefix + qkey, true
}

// mqttTestUserInfoHookV substitutes the raw $SYS.REQ.USER.INFO response, so a
// test can present shapes a real server will not produce on demand: an identity
// in another account, an empty user, a permissions object missing a direction, a
// malformed deny entry, an undecodable body.
//
// It exists because those cases are otherwise untestable in isolation. A
// cross-account client, for instance, cannot even be identified -- its reply is
// published into ITS account while the broker collects replies in its own -- so
// it is refused by timeout, and an end-to-end fixture for it passes with the
// account comparison deleted. A mutation run showed exactly that. The hook is what
// makes the comparison itself falsifiable.
//
// Nil in production, and reached only after a real reply has arrived.
var mqttTestUserInfoHookV atomic.Pointer[func() ([]byte, error)]

// mqttTestPublishGatePauseV parks a parsed-but-unauthorized PUBLISH so a test can
// revoke the snapshot underneath it. See mqttProcessPub. Nil in production.
var mqttTestPublishGatePauseV atomic.Pointer[func()]

// serviceEpochStillOurs reports whether the service account is currently valid
// AND is the same incarnation this client's snapshot was taken against.
//
// Called immediately before a durable write, because the authorization gate ran
// earlier and the epoch can move in between. It does not make the write atomic
// with the check -- nats.go may buffer a publish across a reconnect, and nothing
// on this side can unsend it -- so the residual is stated rather than implied.
func (c *client) serviceEpochStillOurs() bool {
	if !c.mqttAuthzApplies() {
		return true
	}
	// BOTH facts, read together. The account alone says the destination has not
	// moved; it says nothing about whether the identity that was authorized for it
	// still exists. A publication can pass the gate and then have its per-client
	// connection revoked while a subs barrier or a CAS round trip is in flight, and
	// checking only the account would let that write land on behalf of an identity
	// that had already gone -- which is exactly what invalidating-before-closing
	// exists to prevent.
	// No Will exemption is needed here, and an earlier revision had one. A Will
	// whose snapshot is no longer usable no longer takes this path at all: it is
	// handed to the client-free route, whose internal client short-circuits above.
	// The exemption was a second mechanism saying what the routing already says, so
	// it was deleted rather than kept in step.
	c.mu.Lock()
	valid, want := c.snapshotUsableLocked(), c.authzSvcAccount
	c.mu.Unlock()
	if !valid {
		return false
	}
	acc, ok := c.srv.serviceAccount()
	return ok && acc == want
}

// serviceAccountForWill is serviceAccount for the delayed-Will outbox, which runs
// with no per-client identity in scope.
//
// With auth passthrough off the broker never discovers a service account at all,
// and there is nothing to bind a Will to: every client already acts as the
// service credential, so the outbox behaves as it always did.
func (s *Server) serviceAccountForWill() (string, bool) {
	if s.dialClient == nil {
		return _EMPTY_, true
	}
	return s.serviceAccount()
}

// mqttTestDurableWritePauseV parks a publication between the authorization gate
// and its durable write, so a test can revoke the snapshot in the one window the
// final fence exists for. See mqttInitiateMsgDeliveryJSA. Nil in production.
var mqttTestDurableWritePauseV atomic.Pointer[func()]

// mqttTestAffineGateV runs immediately before the AFFINE delivery guard reads its
// verdict, so a test can make that verdict INDETERMINATE for the delivering client.
//
// The affine route is the one guard whose indeterminate arm no end-to-end fixture
// can otherwise reach: by the time a moved copy is redelivered, the resuming client
// holds a freshly established snapshot, and every way of invalidating it from
// outside also closes the connection the copy would have arrived on. Without this
// seam a mutation replacing the indeterminate arm's paced refusal with the deny
// arm's terminal disposal survives the whole suite -- and that mutation destroys a
// session-owned QoS 2 exchange on a transient identity blip. See the affine guard
// in qos2_shared_affine.go. Nil in production.
var mqttTestAffineGateV atomic.Pointer[func(*client)]

// mqttTestDerivedSendPauseV parks a Sparkplug birth between the authorization
// gate and its DERIVED certificate publication, which is the only window that
// send's own fence covers. See mqttHandlePubRetain. Nil in production.
var mqttTestDerivedSendPauseV atomic.Pointer[func()]

// mqttTestRetainedFencePauseV parks a retained publication between the authorization
// gate and the retained store's own fence, which is the only window that fence covers.
// See mqttHandlePubRetain. Nil in production.
var mqttTestRetainedFencePauseV atomic.Pointer[func()]

// mqttTestDeliveryResultV observes the error mqttInitiateMsgDeliveryJSA returns for a
// publication. A fence exit's REPORTING half is otherwise invisible: the revocation that
// trips the fence closes the socket whether or not the error is returned. The callback
// must copy any pp field it needs before returning, since pp is reused. Nil in production.
var mqttTestDeliveryResultV atomic.Pointer[func(*mqttPublish, error)]
