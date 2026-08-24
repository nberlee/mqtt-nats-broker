// Verifying a shared group durable a resume is about to restore membership in.
//
// THE FINDING (F-076). processSharedJSConsumer's resume fall-through subscribes to the
// recorded deliver subject and stops. Every proof that the group durable still exists sits
// behind its `!exists` branch, and the post-subscribe repair sits behind `charged != 0`,
// which is false on that path by construction. So a client whose group durable was deleted
// out of band — a concurrent leaver's last-holder race, the orphan sweep's grace window, an
// operator, an account reset — is told Session Present = 1 and handed a live subscription on
// a subject nothing publishes to. No error, no reason code, and it recurs on every reconnect
// to the same broker while the condition persists.
//
// It is NOT the F-050 shape: F-050 refuses a CONNECT whose restoration REPORTS a failure,
// and here every step returns success, so the per-filter aggregation cannot see it.
//
// WHAT THIS FILE ESTABLISHES, AND WHAT IT DELIBERATELY DOES NOT. It establishes
// CONFIGURATION EVIDENCE: the durable exists and carries a configuration this broker would
// recognise. It establishes NOTHING about ownership — a present canonical consumer does not
// prove this broker still owns the membership the restore is keyed on, and a stale resident
// broker can validate one and serve from it. That is F-077, and it is a defect that exists
// independently of this file: no group-durable create on any path is followed by a
// revalidation of the membership's record ownership.
//
// The verification arm issues NO group-durable write. It reads, and where it cannot prove
// what it needs, it refuses the filter — which reaches the pre-CONNACK refusal through the
// existing per-filter aggregation. It does not recreate: recreating is a provisioning act,
// it belongs with the ownership arbitration that must accompany it, and both are F-077.
package mqtt

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// The three verdicts a lookup can produce, beyond "usable". They are distinct values and
// not one error string because the whole point of the classification is that ABSENCE and
// INDETERMINACY are different facts, and a caller (or a test) that cannot tell them apart
// is the defect this design exists to avoid — an absence is only a fact when something
// established it.
var (
	// errSharedGroupAbsent: the lookup COMPLETED and reported the consumer not there.
	// Authoritative. Nothing here acts on it beyond refusing, because recreating under a
	// membership whose ownership is unrevalidated is F-077's problem to solve.
	errSharedGroupAbsent = errors.New("the shared group consumer does not exist")
	// errSharedGroupIndeterminate: nothing was established. A failed read, a response
	// carrying no consumer, or a consumer carrying no configuration. Fails closed.
	errSharedGroupIndeterminate = errors.New("the shared group consumer could not be verified")
)

// mqttSharedGroupVerdict is what the batched classification concluded for one durable.
type mqttSharedGroupVerdict struct {
	// cfg is the LIVE configuration, cloned, and is set only when err is nil. The caller
	// adopts it in place of whatever it remembered: the remembered copy was captured on an
	// earlier connection, possibly under different broker options, and a member on another
	// broker may legitimately have moved a variable field since.
	cfg *ConsumerConfig
	err error
}

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
	// sharedGroupServerOwned: the server populates it with content this bridge cannot
	// predict across versions, and which carries no behavior the bridge depends on.
	//
	// Deliberately distinct from sharedGroupServerNormalized, and the distinction is the
	// one a review already forced on MaxDeliver: a normalized field has ONE expected value
	// and gets compared against it, because accepting whatever the consumer reports makes a
	// behaviour-bearing field unconstrained. This kind is for a field where no such value
	// exists — but it is still not a free pass. What is checked is the part the bridge
	// could have written: see sharedGroupMetadataIsServerOnly.
	sharedGroupServerOwned
)

// mqttServerOwnedConsumerMetaKeys are the metadata keys nats-server sets on a consumer of
// its own accord. Named constants rather than a "_nats." prefix test on purpose: a prefix
// would silently absorb a key a future server adds, while naming the three makes a fourth
// fail TestSharedGroupServerNormalization and forces a decision about it.
var mqttServerOwnedConsumerMetaKeys = []string{
	JSRequiredLevelMetadataKey,
	JSServerVersionMetadataKey,
	JSServerLevelMetadataKey,
}

// sharedGroupMetadataIsServerOnly reports whether a live group durable's metadata contains
// nothing but the server's own keys.
//
// This bridge sets no consumer metadata. That is an assertion, not an absence of interest:
// consumer metadata is a CARRIER — it is how $MQTT_teardown's epoch coupling is expressed
// at the stream level — so "nobody writes it here" is a fact worth re-checking rather than
// assuming. A group durable carrying a key this bridge did not write was provisioned by
// something else, and the whole-struct comparison is what makes that observable.
func sharedGroupMetadataIsServerOnly(md map[string]string) error {
	for k := range md {
		server := false
		for _, known := range mqttServerOwnedConsumerMetaKeys {
			if k == known {
				server = true
				break
			}
		}
		if !server {
			return fmt.Errorf("shared group consumer carries metadata key %q, which this broker never writes", k)
		}
	}
	return nil
}

// sharedGroupConfigRules is the audited disposition of every ConsumerConfig field.
// TestSharedGroupConfigFieldCoverage enforces the bijection by reflection, and every row
// carries a reason for the same purpose the stream validators' waiver prose serves: a
// disposition without a reason is an omission that looks like a decision.
var sharedGroupConfigRules = map[string]struct {
	kind   sharedGroupFieldKind
	reason string
}{
	// The identity of the group. Deterministic in (group, subject), which is what makes
	// every member converge on one consumer; a mismatch means the deterministic name is
	// occupied by something that is not this group's.
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
	"HeadersOnly":       {sharedGroupPinned, "the bridge needs bodies"},
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
	"Metadata":       {sharedGroupServerOwned, "nats-server populates it with its own version and API-level keys, whose values move with the dependency, so no single expected value exists. This bridge writes NO consumer metadata, and sharedGroupMetadataIsServerOnly re-checks exactly that: anything beyond the three server keys was written by something else"},
	"Replicas":       {sharedGroupPinned, "unset in the request; the group durable inherits the stream's replication rather than asking for its own"},
	"Direct":         {sharedGroupPinned, "server-set for direct-get consumers; this bridge reads through the JS API"},
	"Sourcing":       {sharedGroupPinned, "server-set marker for a sourcing consumer; a group durable is never one"},
}

// sharedGroupVariableFieldsInBounds bounds the three fields the comparison accepts from the
// live consumer, at the domain a create could actually have been provisioned in rather than
// merely at "non-zero". Without this, "variable" would mean "unchecked", which is how a
// short allowlist fails.
//
// streamMaxAckPending is the live $MQTT_msgs ConsumerLimits.MaxAckPending; zero means the
// stream imposes none.
func sharedGroupVariableFieldsInBounds(cfg *ConsumerConfig, streamMaxAckPending int) error {
	if cfg.AckWait <= 0 {
		// Not "the default": the join path substitutes mqttDefaultAckWait before building,
		// so a live consumer reporting zero was not provisioned by a broker running this
		// code.
		return fmt.Errorf("shared group consumer has a non-positive ack_wait (%v)", cfg.AckWait)
	}
	if cfg.MaxAckPending <= 0 || cfg.MaxAckPending > mqttMaxAckTotalLimit {
		return fmt.Errorf("shared group consumer has a max_ack_pending of %d, outside the provisionable range 1..%d",
			cfg.MaxAckPending, mqttMaxAckTotalLimit)
	}
	if streamMaxAckPending > 0 && cfg.MaxAckPending > streamMaxAckPending {
		return fmt.Errorf("shared group consumer has a max_ack_pending of %d, above the stream's consumer limit of %d",
			cfg.MaxAckPending, streamMaxAckPending)
	}
	return nil
}

// sharedGroupExpectedConfig is what a present group durable for (group, subject) must
// carry, given the live consumer's own variable fields.
//
// The two server-normalized rows are applied as EXPECTED VALUES here, in one place, so the
// comparison stays whole-struct. Substituting the live values instead would make exactly
// those fields unconstrained, which is the hole this shape closes.
func sharedGroupExpectedConfig(group, subject string, live *ConsumerConfig) ConsumerConfig {
	want := buildSharedGroupConsumerConfig(group, subject, live.AckWait, live.MaxAckPending, live.MemoryStorage)
	_, _, durable := mqttSharedNames(group, subject)
	want.Name = durable // server-set; for a durable it equals Durable
	want.MaxDeliver = -1
	// Server-OWNED, not normalized: its content moves with the dependency, so it is taken
	// from the live consumer — but only after sharedGroupMetadataIsServerOnly has proved it
	// holds nothing this bridge could have written. That check is the constraint; this
	// assignment only keeps the whole-struct comparison from failing on the server's own
	// version keys.
	want.Metadata = live.Metadata
	return want
}

// sharedGroupConfigUsable reports whether a present group durable is one this broker may
// join a resuming member to, and returns the reason when it is not.
//
// Whole-struct, because a list of fields worth checking cannot close a struct that a
// dependency bump can extend. A field neither side sets is zero on both and passes; any
// writer that sets one deviates and is refused. reflect.DeepEqual rather than ==, because
// ConsumerConfig carries slices, maps and a time pointer.
//
// It never repairs and never deletes. The durable may be serving other members, and a
// configuration this broker would not have built is not one it may silently normalize
// on their behalf.
func sharedGroupConfigUsable(cfg *ConsumerConfig, group, subject string, streamMaxAckPending int) error {
	if cfg == nil {
		return errSharedGroupIndeterminate
	}
	if err := sharedGroupVariableFieldsInBounds(cfg, streamMaxAckPending); err != nil {
		return err
	}
	if err := sharedGroupMetadataIsServerOnly(cfg.Metadata); err != nil {
		return err
	}
	want := sharedGroupExpectedConfig(group, subject, cfg)
	if !reflect.DeepEqual(*cfg, want) {
		return fmt.Errorf("the shared group consumer for %q is not the configuration this broker provisions: got %+v, want %+v",
			subject, *cfg, want)
	}
	return nil
}

// classifySharedGroupConsumer turns one batched lookup outcome into a verdict.
//
// settled is mqttClassifyConsumerInfoResponse's second result, threaded through
// lookupConsumersMulti: false means the reply established nothing, which is NOT an absence.
func classifySharedGroupConsumer(info *ConsumerInfo, settled bool, group, subject string,
	streamMaxAckPending int) mqttSharedGroupVerdict {

	if !settled {
		return mqttSharedGroupVerdict{err: errSharedGroupIndeterminate}
	}
	if info == nil {
		return mqttSharedGroupVerdict{err: errSharedGroupAbsent}
	}
	if info.Config == nil {
		// The consumer was read but says nothing about itself. Settled at the transport
		// layer, useless here: refusing is the only honest outcome.
		return mqttSharedGroupVerdict{err: errSharedGroupIndeterminate}
	}
	if err := sharedGroupConfigUsable(info.Config, group, subject, streamMaxAckPending); err != nil {
		return mqttSharedGroupVerdict{err: err}
	}
	cfg := mqttCloneConsumerConfig(info.Config)
	return mqttSharedGroupVerdict{cfg: cfg}
}

// mqttSharedConsInRecord reports the first sid whose persisted consumer config is a SHARED
// group consumer in ANY of the three ways one can be recognised, which is a shape this
// broker never writes.
//
// buildPersistedSessionLocked skips every entry whose DeliverGroup is non-empty, because
// SharedSubs is the sole authority for shared membership and a config in Cons would be
// redundant with it. So a record carrying one did not come from this code, and under the
// compatibility boundary an unsupported persisted shape is REFUSED rather than
// accommodated — the same rule the reserved-namespace and affine-pair refusals apply.
//
// Refusing it is not bookkeeping. Such a record reaches `exists == true` in the resume arm
// with no resident join behind it, which falsifies the invariant the charge accounting
// rests on; and the expiry paths would CLAIM, rewrite and delete it, normalizing a shape
// nothing here understands.
func mqttSharedConsInRecord(ps *mqttPersistedSession) (string, bool) {
	for sid, cc := range ps.Cons {
		if cc == nil {
			continue
		}
		// THREE independent marks, because any ONE of them makes the entry a shared
		// consumer this broker never persists — and keying only on DeliverGroup is a hole
		// with a destructive exit.
		//
		// clear() decides what is session-private by DeliverGroup alone: an entry with an
		// empty one goes into the direct deletion list. So a record carrying a shared SID,
		// or a group durable, with DeliverGroup empty would pass a DeliverGroup-only guard
		// and then have teardown delete a group consumer that is still serving other
		// members, discarding its backlog with it. That is the exact destruction the
		// refusal exists to prevent, reached through the one field the guard was reading.
		if cc.DeliverGroup != _EMPTY_ {
			return sid, true
		}
		// The SID is the session's own key for the subscription, and only a shared
		// membership produces one in this shape. Covers the level-up companion too, whose
		// key is the shared key with the multi-level suffix.
		if _, _, ok := mqttParseSharedKey(strings.TrimSuffix(sid, mqttMultiLevelSidSuffix)); ok {
			return sid, true
		}
		// And the durable itself: mqttSharedNames is the single source of that prefix, and
		// no other consumer class can collide with it.
		if strings.HasPrefix(cc.Durable, mqttSharedConsumerDurablePrefix) {
			return sid, true
		}
	}
	return _EMPTY_, false
}

// mqttSharedConsRecordErr is the one refusal message, so every ingress says the same thing.
func mqttSharedConsRecordErr(clientID, sid string) error {
	return fmt.Errorf("the record of %q carries a shared group consumer configuration for %q in Cons, "+
		"a format this broker never writes: shared membership is held in SharedSubs alone. Stop this "+
		"account's brokers and recreate the bridge-owned JetStream state (the $MQTT_* streams); this is "+
		"not a rolling upgrade", clientID, sid)
}

// mqttSharedGroupTarget is one group durable a resume is about to restore membership in,
// and the sid the restore will look it up under.
//
// The collection is keyed by SID rather than by durable because that is what the resume arm
// has in hand, and because a "foo.>" filter owns TWO distinct durables — its own and the
// level-up companion `mqttNeedSubForLevelUp` derives — each under its own sid. Keying by
// durable alone would let one of the pair silently answer for the other.
type mqttSharedGroupTarget struct {
	sid     string
	group   string
	subject string
	durable string
}

// collectResidentSharedGroups is the pre-pass's population: exactly the shared filters
// whose sid already has a RESIDENT cons entry.
//
// That is the resume arm's population and no more. A filter with no resident entry takes
// the untouched join path, which creates — this verification arm neither helps nor hinders
// it, and including it here would verify a durable that is about to be created anyway.
//
// Derivable before processSubs runs, which is what makes the batch possible: the sid is
// f.sessionKey(), and the level-up companion's sid and subject follow the same rule
// processSubs itself applies.
//
// sess.mu is taken internally.
func (sess *mqttSession) collectResidentSharedGroups(filters []*mqttFilter) []mqttSharedGroupTarget {
	var want []mqttSharedGroupTarget
	add := func(sid, group, subject string) {
		_, _, durable := mqttSharedNames(group, subject)
		want = append(want, mqttSharedGroupTarget{sid: sid, group: group, subject: subject, durable: durable})
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()
	for _, f := range filters {
		if !f.shared || f.qos >= mqttSubAckFailure {
			continue
		}
		sid := f.sessionKey()
		if _, resident := sess.cons[sid]; resident {
			add(sid, f.group, f.filter)
		}
		// The level-up companion is an independent consumer with its own durable, so it is
		// verified on its own account. A pass that checked only the main half would leave a
		// dead companion restored and delivering nothing, which is this finding in
		// miniature.
		if mqttNeedSubForLevelUp(f.filter) {
			fwcsid := sid[:len(sid)-2] + mqttMultiLevelSidSuffix
			if _, resident := sess.cons[fwcsid]; resident {
				add(fwcsid, f.group, f.filter[:len(f.filter)-2])
			}
		}
	}
	return want
}

// classifySharedGroups runs ONE batched lookup over the collected durables and classifies
// each, returning verdicts keyed by sid.
//
// Batched rather than one lookup per filter: a session may hold thousands of filters, and
// lookupConsumersMulti exists precisely because thousands of serial CONNECT-time round
// trips are not acceptable on a reconnect.
//
// A failure to read the stream's consumer limits is itself indeterminate for every target:
// the MaxAckPending bound cannot be applied, so nothing can be declared canonical. Fails
// closed rather than skipping the bound.
//
// No lock held on entry.
func (as *mqttAccountSessionManager) classifySharedGroups(targets []mqttSharedGroupTarget) map[string]mqttSharedGroupVerdict {
	out := make(map[string]mqttSharedGroupVerdict, len(targets))
	if len(targets) == 0 {
		return out
	}

	var streamMaxAckPending int
	si, err := as.jsa.lookupStream(mqttStreamName)
	if err != nil || si == nil {
		for _, t := range targets {
			out[t.sid] = mqttSharedGroupVerdict{err: errSharedGroupIndeterminate}
		}
		return out
	}
	streamMaxAckPending = si.Config.ConsumerLimits.MaxAckPending

	as.mu.RLock()
	failHook := as.testSharedGroupLookupFail
	batchHook := as.testSharedGroupLookupBatch
	errHook := as.testSharedGroupLookupErr
	as.mu.RUnlock()
	call := 0

	// BOUNDED, and it STOPS at the first refusal. One rule, because three rounds of
	// review each found the same seam re-cut a different way.
	//
	// Two properties, and they are the same property:
	//
	//   - Chunked at mqttInflightLoadBatch, as every other resume-time batch here is. A
	//     session's shared filter count is client-controlled and MaxSubs defaults to
	//     unlimited, so one unbounded request would let a single reconnect build an
	//     arbitrary request set.
	//
	//   - Abandoned the moment any target's verdict is a refusal. A refusal — for ANY
	//     reason: a transport failure, an unreadable reply, an absence, a non-canonical
	//     configuration — already refuses the whole CONNECT through the per-filter
	//     aggregation. Every lookup after that point is work whose result cannot change
	//     the outcome, and each carries its own timeout, so continuing lets the client
	//     choose how long the CONNECT stalls. In the POST-pass that is not merely latency:
	//     this connection's shared queue interest is already installed, so QoS 0 messages
	//     selected for it are dropped behind the restore gate for the whole walk.
	//
	// Earlier revisions special-cased only the transport error, which is why a
	// response-level refusal kept walking. There is no special case now: a transport
	// failure is simply one way a verdict comes back refusing.
	for start := 0; start < len(targets); start += mqttInflightLoadBatch {
		end := min(start+mqttInflightLoadBatch, len(targets))
		chunk := targets[start:end]

		durables := make([]string, 0, len(chunk))
		for _, t := range chunk {
			durables = append(durables, t.durable)
		}
		if batchHook != nil {
			batchHook(len(durables))
		}
		infos, settled, lerr := as.jsa.lookupConsumersMulti(mqttStreamName, durables)
		// Beside the real lookup, never instead of it: the request above has already been
		// issued, so a test proves the production route ran and then failed.
		if errHook != nil {
			if herr := errHook(call); herr != nil {
				lerr = herr
			}
		}
		call++

		for i, t := range chunk {
			var v mqttSharedGroupVerdict
			switch {
			case lerr != nil:
				// The whole batch established nothing about any of its durables.
				v = mqttSharedGroupVerdict{err: errSharedGroupIndeterminate}
			default:
				var info *ConsumerInfo
				var ok bool
				if i < len(infos) {
					info = infos[i]
				}
				if i < len(settled) {
					ok = settled[i]
				}
				// The seam stands BESIDE the real classification: the lookup already ran.
				if failHook != nil {
					if herr := failHook(t.durable); herr != nil {
						ok, info = false, nil
					}
				}
				v = classifySharedGroupConsumer(info, ok, t.group, t.subject, streamMaxAckPending)
			}
			out[t.sid] = v
			if v.err == nil {
				continue
			}
			// Terminal. Keep what this chunk established, judge everything still
			// unexamined as indeterminate — a caller reading a missing entry as "nothing
			// to check" is the failure this whole change exists to prevent — and issue no
			// further lookup.
			for _, rest := range targets[start+i+1:] {
				out[rest.sid] = mqttSharedGroupVerdict{err: errSharedGroupIndeterminate}
			}
			return out
		}
	}
	return out
}

// applySharedGroupPostPass is what the SECOND classification does with its verdicts:
// adopt every success, fail every refusal.
//
// Adoption is not optional and is easy to leave out — the first implementation did, and a
// holistic review caught it. The post-pass exists partly because a member-variable field
// (AckWait, MaxAckPending, MemoryStorage) may legitimately move between the two readings;
// keeping the pre-pass snapshot would finish the CONNECT holding state the account no
// longer agrees with, while every document describing this mechanism claims the second
// reading is the one adopted. A refusal-only post-pass makes those claims false.
//
// sess.mu is taken internally, per the discipline every other cons writer follows.
func applySharedGroupPostPass(sess *mqttSession, filters []*mqttFilter,
	verdicts map[string]mqttSharedGroupVerdict) {

	if len(verdicts) == 0 {
		return
	}
	sess.mu.Lock()
	for sid, v := range verdicts {
		if v.err != nil || v.cfg == nil {
			continue
		}
		// Only where the resume arm actually adopted one: a sid with no entry was never
		// this pass's to install, and creating one here would invent a membership.
		if _, resident := sess.cons[sid]; resident {
			sess.cons[sid] = v.cfg
		}
	}
	sess.mu.Unlock()
	failSharedGroupFilters(filters, verdicts)
}

// failSharedGroupFilters marks every filter whose group durable did not verify, so the
// CONNECT restore's existing per-filter aggregation refuses before the CONNACK.
//
// It reuses that aggregation rather than adding a refusal path of its own: the restore
// already turns an unrestored filter into a pre-CONNACK refusal that names the filter, and
// a second mechanism saying the same thing is one more place for the two to disagree.
//
// A filter is failed by its OWN sid or by its level-up companion's, because either dead
// half makes the subscription the client is being told about incomplete.
func failSharedGroupFilters(filters []*mqttFilter, verdicts map[string]mqttSharedGroupVerdict) {
	if len(verdicts) == 0 {
		return
	}
	for _, f := range filters {
		if !f.shared || f.qos >= mqttSubAckFailure {
			continue
		}
		sid := f.sessionKey()
		sids := []string{sid}
		if mqttNeedSubForLevelUp(f.filter) {
			sids = append(sids, sid[:len(sid)-2]+mqttMultiLevelSidSuffix)
		}
		for _, s := range sids {
			v, seen := verdicts[s]
			if !seen || v.err == nil {
				continue
			}
			f.qos = mqttSubAckFailure
			f.reason = mqttSubAckFailure
			f.reasonStr = v.err.Error()
			break
		}
	}
}
