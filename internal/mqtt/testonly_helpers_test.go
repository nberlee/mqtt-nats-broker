package mqtt

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
)

// ---- from handoff_outcome.go ----

// mqttIsHandoffSubject reports whether a subject is a handoff outcome. The session walks
// filter on the session prefix and would never see one, but the accept-fact readers and the
// record-absent paths take subjects from enumerations, so the classification is available
// rather than assumed.
func mqttIsHandoffSubject(subject string) bool {
	return strings.HasPrefix(subject, mqttHandoffSubjectPrefix)
}

// ---- from inflight_marker.go ----

// mqttInflightMarkerSubject is the marker's subject for one register.
func mqttInflightMarkerSubject(idHash, gen string, pi uint16) string {
	return mqttInflightSubject(idHash, gen, pi) + "." + mqttInflightMarkerSuffix
}

// mqttInflightMarkerSubjectFor derives the marker subject from a register subject that has
// already been parsed, so a caller holding a register's name never rebuilds it from parts.
func mqttInflightMarkerSubjectFor(reg *mqttInflightName) string {
	return reg.subject + "." + mqttInflightMarkerSuffix
}

// ---- from inflight_register.go ----

// endSend ends a send section and executes whatever continuation a removal path parked
// while the write was out.
//
// Guarded by the TOKEN, not by map residency (a condemned entry's disposition is
// completed here) and not by register identity (a rollback restores the same pointer).
//
// Session lock NOT held on entry: the parked continuation is extracted under the lock,
// and the durable work it owes runs after the unlock.
func (sess *mqttSession) endSend(c *client, pi uint16, token string) {
	sess.endSendWith(c, pi, token, nil)
}

// ---- from inflight_slot_state.go ----

// discharged reports whether the CALLER's own delete obligation is over, which is true for
// both established forms. It is NOT "the identifier is free" — only
// mqttReleaseEstablishedAbsent says that.
func (r mqttReleaseConfirmed) discharged() bool {
	return r == mqttReleaseEstablishedAbsent || r == mqttReleaseEstablishedOther
}

// ---- from outbound_authority.go ----

// mqttGenClosureWalkFilter enumerates one client's closure records, and only those.
func mqttGenClosureWalkFilter(idHash string) string {
	return mqttF105SubjectPrefix() + mqttClosureClassTk + "." + idHash + ".*"
}

// mqttTransitionOutcomeWalkFilter enumerates one client's transition outcomes, and only
// those.
func mqttTransitionOutcomeWalkFilter(idHash string) string {
	return mqttF105SubjectPrefix() + mqttOutcomeClassTk + "." + idHash + ".*"
}

// ---- from outbound_exchange.go ----

// mqttSharedAcqRefEqual compares two optional acquisition references by VALUE.
func mqttSharedAcqRefEqual(a, b *mqttSharedAcqRef) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// ---- from outbound_ops.go ----

// opPrepareHandoff freezes admission and reserves a handoff from the serving authority.
//
// The INTENT is the caller's, because it describes what the accepting connection proposes —
// its generation, its tenure, and the retirement it requests. Everything derived from the
// PREDECESSOR is not: the intent's `Prev*` members are filled in from the standing authority
// and the session record the caller names, so a caller cannot propose a handoff from a
// predecessor that is not there.
func (jsa *mqttJSA) opPrepareHandoff(id, epoch, txn string, i *mqttHandoffIntent) (mqttOpOutcome, error) {
	return jsa.opPrepareHandoffFenced(id, epoch, txn, i, 0)
}

// ---- from outbound_source_completion.go ----

// Arming is the creation boundary: no bare retry token can escape without its
// timer, recovery-scope handoff, or shutdown entry. All three carry the captured
// epoch, acquisition and ACK authority. sess.mu -> owedTerminalsMu is the order.
func (sess *mqttSession) armTerminalRetry(ref mqttInflightSlotRef, acknowledged bool) *mqttTerminalRetry {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.armTerminalRetryLocked(nil, ref, acknowledged)
}

func (sess *mqttSession) disarmTerminalRetry(w *mqttTerminalRetry) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	key := w.key
	if sess.terminalRetries[key] == w {
		delete(sess.terminalRetries, key)
	}
}

// ---- from outbound_withdraw_unsent.go ----

// mqttEntitlementLicence is the licence of an entitlement captured from the deciding connection
// earlier (a parked close, a resume walk). Test-only (F-054): production never mints a licence
// this way, and outbound_licence_provenance_test.go fails if a production call appears.
func mqttEntitlementLicence(ent mqttOutboundEntitlement, ok bool) mqttDestroyLicence {
	return mqttDestroyLicence{ent: ent, entOK: ok}
}

// ---- from publish_inbound.go ----

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

// ---- from pubrel_action.go ----

// completed reports mqttTerminalDone specifically: settled AND not a deliberately retained
// slot, so cleanup ran to a confirmed delete or the slot already belonged to another exchange.
// A retained slot is settled but NOT completed. (In PUBCOMP's own path Done is also returned
// for a `pubrel_terminal_draining` slot, where the live drain — not this predicate — owns
// finalization.)
func (o mqttTerminalOutcome) completed() bool { return o == mqttTerminalDone }

// ---- from retained_index.go ----

// retainedIndexedLive reports whether subject is a replay candidate: indexed, and not a
// placeholder.
func (as *mqttAccountSessionManager) retainedIndexedLive(subject string) bool {
	as.mu.RLock()
	defer as.mu.RUnlock()
	if as.retmsgs == nil {
		return false
	}
	ref, ok := as.retmsgs.Find(stringToBytes(subject))
	return ok && !ref.deleted
}

// ---- from sess_expiry.go ----

// expireSession removes and clears a session whose Session Expiry Interval has
// elapsed, unless it was cancelled (reconnect/takeover) or the account manager is
// shutting down. Reuses the same removeSession + clear path as a clean disconnect.
// A session not in the in-memory maps (timer re-armed by the startup sweep) is
// re-verified against the persisted record first. Spec5 [3.1.2.11.2].
//
// No lock held on entry.
// expireSession runs the expiry for idHash with no timer-instance proof: it is the
// form a direct caller (a sweep decision, a test) uses, where no timer fired.
func (as *mqttAccountSessionManager) expireSession(s *Server, idHash string) {
	as.expireSessionFired(s, idHash, 0)
}

// ---- from sess_incarnation.go ----

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

// ---- from sess_lease.go ----

// retireBoundIncarnation is retireBoundIncarnationChecked with no pre-detach proof. A test
// entry point (F-054): every production caller - the CONNECT clean-start arm and the gone
// retirement - supplies a proof and calls retireBoundIncarnationChecked directly.
func (asm *mqttAccountSessionManager) retireBoundIncarnation(sess *mqttSession) (
	*mqttRetiredBinding, *mqttPreparedClear, error) {

	return asm.retireBoundIncarnationChecked(sess, nil)
}

// retirementPendingArmed reports whether a discard obligation is still parked on this
// session's CURRENT incarnation. A test observation (F-054): the drain-triggered finisher
// (pendingDiscardFinisher) polls the tracker it CAPTURED, not this, so a later
// incarnation's mark cannot stop it.
func (sess *mqttSession) retirementPendingArmed() bool {
	sess.mu.Lock()
	tr := sess.leases
	sess.mu.Unlock()
	return tr.isRetirementPending()
}

// ---- from sess_manager.go ----

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
	return as.createOrRestoreSessionWithin(clientID, opts, mqttNewRecordReDrive())
}

// ---- from shared_group_verify.go ----

// sharedGroupFieldKind is what a ConsumerConfig field IS, for the purposes of judging a
// group durable this broker did not necessarily create.
//
// Every field takes exactly one kind, enforced by reflection in
// TestSharedGroupConfigFieldCoverage. A LIST of fields worth checking is what this
// deliberately is not: a list cannot close a struct a dependency bump can extend, and the
// affine path already recorded that every list an earlier revision of it wrote turned out
// to be missing one. The comparison below is whole-struct; this table exists to justify the
// only two categories of field the comparison may not take literally.
type sharedGroupFieldKind int

const (
	// sharedGroupPinned: compared byte-for-byte against buildSharedGroupConsumerConfig.
	// The default, and the reason a new field fails closed rather than being waved through.
	sharedGroupPinned sharedGroupFieldKind = iota
	// sharedGroupServerNormalized: the SERVER sets or defaults it, so the request never
	// carries it. Compared against an EXPECTED NORMALIZED VALUE, never copied from the live
	// consumer — copying makes the field unconstrained, which would accept a MaxDeliver of 1
	// on a group durable and discard a delivery after one attempt.
	sharedGroupServerNormalized
	// sharedGroupMemberVariable: another member's broker may legitimately have provisioned a
	// different value, so equality would refuse a heterogeneous but working deployment.
	// BOUNDED rather than ignored: the bound is what stops "variable" meaning "anything".
	sharedGroupMemberVariable
	// sharedGroupServerOwned: the server populates it with content this broker cannot
	// predict across versions, and which carries no behavior the broker depends on.
	//
	// Deliberately distinct from sharedGroupServerNormalized, and the distinction is the
	// one a review already forced on MaxDeliver: a normalized field has ONE expected value
	// and gets compared against it, because accepting whatever the consumer reports makes a
	// behaviour-bearing field unconstrained. This kind is for a field where no such value
	// exists — but it is still not a free pass. What is checked is the part the broker
	// could have written: see sharedGroupMetadataIsServerOnly.
	sharedGroupServerOwned
)

// sharedGroupConfigRules is the audited disposition of every ConsumerConfig field.
// TestSharedGroupConfigFieldCoverage enforces the bijection by reflection, and every row
// carries a reason for the same purpose the stream validators' waiver prose serves: a
// disposition without a reason is an omission that looks like a decision.
var sharedGroupConfigRules = map[string]struct {
	kind   sharedGroupFieldKind
	reason string
}{
	// The identity of the group's live incarnation. Derived from (group, subject,
	// incarnation), which is what makes every member of that incarnation converge on one
	// consumer; a mismatch means the name is occupied by something that is not this group's.
	"Durable":        {sharedGroupPinned, "the group durable's name, derived from (group, subject)"},
	"DeliverSubject": {sharedGroupPinned, "the subject this group's deliveries arrive on; a wrong one delivers nowhere"},
	"DeliverGroup":   {sharedGroupPinned, "the queue group that makes JetStream pick exactly one member"},
	"FilterSubject":  {sharedGroupPinned, "the subject space the group consumes; a wrong one silently changes what the client receives"},

	// Delivery semantics the whole group depends on. None is a tuning knob: each changes
	// what the members observe, and a member joining a group configured otherwise would
	// mis-account or lose deliveries.
	"AckPolicy":         {sharedGroupPinned, "AckExplicit is what makes QoS 1/2 accounting possible at all; AckNone would silently complete every delivery"},
	"DeliverPolicy":     {sharedGroupPinned, "DeliverNew; a different policy replays history into a live group"},
	"ReplayPolicy":      {sharedGroupPinned, "instant; a replay pacing this broker never sets"},
	"MaxDeliver":        {sharedGroupServerNormalized, "the request leaves it zero and nats-server normalizes that to -1 (unlimited). A positive value would DISCARD a group delivery after that many attempts, so it is compared against the expected normalized value rather than accepted from the live consumer"},
	"InactiveThreshold": {sharedGroupPinned, "must be zero: a non-zero one gives the group durable a delete timer and reaps it while every member is offline, which is the shape the join path refuses to create"},
	"HeadersOnly":       {sharedGroupPinned, "the broker needs bodies"},
	"FlowControl":       {sharedGroupPinned, "not used; a flow-control consumer sends frames the delivery callback does not expect"},
	"Heartbeat":         {sharedGroupPinned, "not used, for the same reason as FlowControl"},
	"RateLimit":         {sharedGroupPinned, "not used; pacing the group is not this broker's contract"},
	"BackOff":           {sharedGroupPinned, "not used; it would also override AckWait, so a non-empty one silently changes redelivery timing for every member"},
	"SampleFrequency":   {sharedGroupPinned, "monitoring only, and not something this broker sets"},

	// Legitimately variable across members, and each BOUNDED in
	// sharedGroupVariableFieldsInBounds.
	"AckWait":       {sharedGroupMemberVariable, "another member's broker may run a different ack_wait; bounded positive"},
	"MaxAckPending": {sharedGroupMemberVariable, "another member's broker may run a different cap; bounded to the provisionable range and to the live stream's consumer limit"},
	"MemoryStorage": {sharedGroupMemberVariable, "another member's broker may run consumer_memory_storage; both values are ones this broker produces, so it is unbounded"},

	// Server-set identity.
	"Name": {sharedGroupServerNormalized, "the server reports the consumer's name; for a durable it equals Durable, so that is the expected value"},

	// Pull-consumer and priority machinery this broker never uses. Pinned at their zero
	// values rather than ignored: a group durable carrying any of them was not provisioned
	// by this broker, and the whole-struct comparison is what makes that observable.
	"OptStartSeq":        {sharedGroupPinned, "unset; a start sequence would change what a recreated group replays"},
	"OptStartTime":       {sharedGroupPinned, "unset, as OptStartSeq"},
	"MaxWaiting":         {sharedGroupPinned, "pull-consumer bound; this is a push consumer, and the server leaves it zero when DeliverSubject is set"},
	"MaxRequestBatch":    {sharedGroupPinned, "pull-consumer bound"},
	"MaxRequestExpires":  {sharedGroupPinned, "pull-consumer bound"},
	"MaxRequestMaxBytes": {sharedGroupPinned, "pull-consumer bound"},
	"PauseUntil":         {sharedGroupPinned, "a paused group durable delivers nothing; this broker never pauses one"},
	"PriorityGroups":     {sharedGroupPinned, "unused"},
	"PriorityPolicy":     {sharedGroupPinned, "unused"},
	"PinnedTTL":          {sharedGroupPinned, "unused"},

	// Descriptive and server-informational.
	"Description":    {sharedGroupPinned, "unset; cosmetic, but a whole-struct comparison is only closed if every field has a verdict"},
	"FilterSubjects": {sharedGroupPinned, "unset: this broker uses the single FilterSubject form, and a multi-filter group would consume a different subject space"},
	"Metadata":       {sharedGroupServerOwned, "nats-server populates it with its own version and API-level keys, whose values move with the dependency, so no single expected value exists. This broker writes NO consumer metadata, and sharedGroupMetadataIsServerOnly re-checks exactly that: anything beyond the three server keys was written by something else"},
	"Replicas":       {sharedGroupPinned, "unset in the request; the group durable inherits the stream's replication rather than asking for its own"},
	"Direct":         {sharedGroupPinned, "server-set for direct-get consumers; this broker reads through the JS API"},
	"Sourcing":       {sharedGroupPinned, "server-set marker for a sourcing consumer; a group durable is never one"},
}

// ---- from testhooks.go ----

// mqttTestRestoreOnce wraps a test hook's restore closure so it runs AT MOST ONCE.
//
// Every hook setter returns a closure that puts back whatever it displaced, and tests
// legitimately call it EARLY — mid-test, to observe behaviour with the hook gone — and
// then again from a defer or t.Cleanup. Without this guard the second call restores an
// OBSOLETE predecessor over whatever the currently-running code installed afterwards,
// which is one half of how a leaked hook contaminates an entire suite run: a test fails
// inside its injected window, its own restore never runs, and a later test's nested
// restoration puts the contaminated hook back rather than clearing it.
//
// Idempotence here is not tidiness. These hooks are PROCESS-GLOBAL, so one leak turns
// every subsequent test's unrelated failure into noise — 840 of one run's 876 failures
// were downstream of exactly two such leaks, and the real defects were unreadable behind
// them.
func mqttTestRestoreOnce(restore func()) func() {
	var once sync.Once
	return func() { once.Do(restore) }
}

// ---- from unsub_drain.go ----

// claimedCount reports how many sequences this drain still claims. A test observation
// (F-054): whether the drain is finished is retirable()/retirableExcept()'s decision, which
// asks more than whether the count is zero.
//
// sess.mu must be held.
func (d *mqttDrain) claimedCount() int {
	n := 0
	for _, st := range d.seqs {
		if st.claimed {
			n++
		}
	}
	return n
}
