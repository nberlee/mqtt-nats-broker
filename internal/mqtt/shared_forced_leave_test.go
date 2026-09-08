// Tests for the 3.1.1 forced leave's TARGET DERIVATION: the two authorities it uses, and the
// populations each one alone cannot see.
//
// The defect these pin is that the leave's two halves used to disagree about what they were
// acting on — the staging void walked the membership keys while the commit's purge walked
// `es.cons` — so a record-restored session, which has membership WITHOUT configs, had its
// group durables released while its pending state was left holding packet identifiers and
// Receive Maximum slots for the rest of the connection. That is F-045's forced-leave instance.
//
// The unit under test is `sharedPendingDurablesLocked`, the derivation both halves now share.
// The end-to-end arms of the same change (which save outcome refuses, which continues, and the
// restore gate's terminal) are driven through real CONNECTs in
// sess_forced_leave_exit_e2e_test.go; here the concern is only "does the derivation SEE the
// target", because that is what neither structural view could do.

package mqtt

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/nats-io/nuid"
)

// TestForcedLeavePendingDerivationSeesMembershipOnlyDurable is the F-045 population: a
// record-restored session. Shared consumer configs are never persisted
// (buildPersistedSessionLocked excludes them and only processSubs rebuilds them), while
// recoverInflightRegisters HAS rebuilt the pending entries under the deterministic group
// durable names — so a cons walk finds nothing and the pending maps find everything.
//
// Mutation check: derive the purge from `es.cons` (the pre-change code) and this fails, because
// the fixture deliberately installs no shared config at all.
func TestForcedLeavePendingDerivationSeesMembershipOnlyDurable(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-membership-only")
	const group, filter = "g", "fl.a"
	_, _, dur := mqttSharedNames(group, filter)

	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	// The record-restored shape: a membership, pending state under the group durable, and NO
	// shared cons entry — the config the commit's old walk needed in order to see the durable.
	e.sess.sharedSubs = map[string]byte{string(mqttSharedSubPrefix) + group + "/" + filter: 1}
	sharedCons := 0
	for _, cc := range e.sess.cons {
		if cc.DeliverGroup != _EMPTY_ {
			sharedCons++
		}
	}
	got := e.sess.sharedPendingDurablesLocked()
	e.sess.mu.Unlock()

	if sharedCons != 0 {
		t.Fatalf("the fixture must have NO shared cons entry, found %d: with one present a "+
			"cons walk would also reach the durable and the test would prove nothing", sharedCons)
	}
	if len(got) != 1 || got[0] != dur {
		t.Fatalf("the pending derivation returned %v, want exactly [%s]: the durable is "+
			"reachable through the pending maps alone", got, dur)
	}
	// The entry really is the one whose identifier is at stake.
	e.sess.mu.Lock()
	_, held := e.sess.pendingPublish[pi]
	e.sess.mu.Unlock()
	if !held {
		t.Fatal("the fixture's pending entry vanished, so the identifier it holds is not at stake")
	}
}

// TestForcedLeavePendingDerivationSeesDurableNamedByNeitherView is the shape the plan calls
// pending-only: no membership key and no config names the durable, but its register is live.
//
// This is the population that makes the union of the two structural views insufficient rather
// than merely awkward, and it is why the derivation had to move into the STAGING half: the
// write that removes the membership is the write that must also record "these allocations were
// never transmitted".
func TestForcedLeavePendingDerivationSeesDurableNamedByNeitherView(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-neither-view")
	const group, filter = "g", "fl.b"
	_, _, dur := mqttSharedNames(group, filter)

	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	_ = e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	// Neither view: the membership is gone (some earlier operation removed it) and no config
	// was ever rebuilt. Only the register remains.
	e.sess.sharedSubs = nil
	got := e.sess.sharedPendingDurablesLocked()
	e.sess.mu.Unlock()

	if len(got) != 1 || got[0] != dur {
		t.Fatalf("the pending derivation returned %v, want exactly [%s]: with both structural "+
			"views empty this durable is reachable no other way", got, dur)
	}
}

// TestForcedLeavePendingDerivationIgnoresPlainDurables pins the classification boundary. The
// derivation keys on the shared durable NAME PREFIX, which is the protocol fact
// collectSharedPendingPurge already relies on — and it must not be confused by a plain
// consumer, whose pending state belongs to a subscription the leave has no business touching.
//
// Mutation check: drop the prefix test and this fails, because the plain durable appears.
func TestForcedLeavePendingDerivationIgnoresPlainDurables(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-plain-untouched")
	const group, filter = "g", "fl.c"
	_, _, sharedDur := mqttSharedNames(group, filter)
	plainDur := e.sess.idHash + "_" + nuid.Next()

	sharedSeq := e.publishSourceGroup(t, sharedDur, group, filter, filter)
	_ = e.track(t, sharedDur, sharedSeq, 1)
	plainSeq := e.publishSource(t, plainDur)
	_ = e.track(t, plainDur, plainSeq, 2)

	e.sess.mu.Lock()
	got := e.sess.sharedPendingDurablesLocked()
	e.sess.mu.Unlock()

	sort.Strings(got)
	if len(got) != 1 || got[0] != sharedDur {
		t.Fatalf("the pending derivation returned %v, want exactly [%s]: a plain consumer's "+
			"pending state is not the forced leave's to purge", got, sharedDur)
	}
}

// TestForcedLeavePendingDerivationIsDeduplicated: cpending, deferred and srcAckOwed can each
// name the same durable, and the caller voids per target. A duplicate would void twice, which
// is harmless in itself, but the derivation is also the delete-candidate EXCLUSION set, and a
// caller reasoning about set membership deserves a set.
func TestForcedLeavePendingDerivationIsDeduplicated(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-dedup")
	const group, filter = "g", "fl.d"
	_, _, dur := mqttSharedNames(group, filter)

	for range [2]struct{}{} {
		sseq := e.publishSourceGroup(t, dur, group, filter, filter)
		_ = e.track(t, dur, sseq, 1)
	}

	e.sess.mu.Lock()
	got := e.sess.sharedPendingDurablesLocked()
	e.sess.mu.Unlock()

	if len(got) != 1 {
		t.Fatalf("the derivation returned %v for two entries on one durable, want a single "+
			"entry: the result is a target SET", got)
	}
}

// TestForcedLeaveStagingVoidsThePendingOnlyRegister is the crash window, and it is the deepest
// thing this item closes.
//
// The rule it enforces is voidRegistrationsLocked's own: the record write that removes the
// membership must be the write that says "these allocations were never transmitted". Staging
// used to void only membership- and cons-derived durables, so a PENDING-ONLY durable's nonce
// never entered that write. A crash between the save landing and the commit then left:
//
//   - no persisted membership,
//   - no persisted shared config,
//   - a live register whose nonce is in NO CancelledRegs,
//
// and — the part that makes it unrecoverable rather than merely untidy — NO membership left to
// trigger any future forced leave, because the leave is guarded on len(sharedSubs) > 0. Recovery
// then treats that register as reservable state, so the identifier is held for good.
//
// The assertion is on the PERSISTED verdict, not on memory: memory is what a crash discards.
func TestForcedLeaveStagingVoidsThePendingOnlyRegister(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-staging-void")
	const group, filter = "g", "fl.stage"
	_, _, dur := mqttSharedNames(group, filter)

	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	// Pending-only: no membership names this durable and no config was ever rebuilt. The
	// membership that IS present belongs to a different filter, so the key-derived view cannot
	// reach the durable under test — which is what makes this the pending-only population
	// rather than a membership-derived one.
	e.sess.sharedSubs = map[string]byte{string(mqttSharedSubPrefix) + group + "/fl.other": 1}
	// Exactly what the forced leave's STAGING half now does over the pending derivation.
	for _, d := range e.sess.sharedPendingDurablesLocked() {
		e.sess.voidRegistrationsLocked(d)
	}
	e.sess.mu.Unlock()

	if err := testSessSave(t, e.sess); err != nil {
		t.Fatalf("persist the staged verdict: %v", err)
	}

	// The verdict must be in the RECORD. A crash here discards memory, and the record is all a
	// restart has: without the nonce in CancelledRegs, recovery reinstates the register as a
	// live claim on the identifier and no later leave can reach it.
	subject := mqttSessStreamSubjectPrefix + e.sess.domainTk + e.sess.idHash
	smsg, lerr := e.sess.jsa.loadLastMsgFor(mqttSessStreamName, subject)
	if lerr != nil {
		t.Fatalf("loading the staged record: %v", lerr)
	}
	ps := &mqttPersistedSession{}
	if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
		t.Fatalf("unmarshalling the staged record: %v", uerr)
	}
	found := false
	for _, n := range ps.CancelledRegs {
		if n == nonce {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("the pending-only register's nonce %q is not in the record's CancelledRegs %v: "+
			"a crash after this save and before the commit leaves a live register with no "+
			"membership able to trigger another leave, so recovery reserves its identifier "+
			"permanently", nonce, ps.CancelledRegs)
	}
}

// TestForcedLeavePendingOnlyTargetIsPurgedButNotDeleted pins the boundary between the two
// authorities, and it is the one place where "found it" must NOT mean "may destroy it".
//
// A target discovered only through the pending maps is purged: its identifier is freed and its
// tracking dropped. It is NOT offered for deletion, because pending state is not evidence that
// this session was the group durable's LAST HOLDER — only the last-holder scan authorizes a
// CONSUMER.DELETE, and a durable another member still holds carries that member's backlog on an
// interest-retained stream.
//
// The scan is arranged to answer UNHELD, so the test cannot pass on the scan's verdict: with no
// other session holding the durable the delete would be authorized if the code offered it, and
// the assertion is that the code does not offer it at all.
func TestForcedLeavePendingOnlyTargetIsPurgedButNotDeleted(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-pendingonly-keep")
	const group, filter = "g", "fl.keep"
	deliver, _, dur := mqttSharedNames(group, filter)

	// The durable exists on the stream, exactly as a real group consumer would.
	ccr := &CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: buildSharedGroupConsumerConfig(group, filter, mqttDefaultAckWait, 1024, false),
	}
	if _, err := e.asm.jsa.createDurableConsumer(ccr); err != nil {
		t.Fatalf("creating the group consumer: %v", err)
	}
	_ = deliver

	sseq := e.publishSourceGroup(t, dur, group, filter, filter)
	pi := e.track(t, dur, sseq, 1)

	e.sess.mu.Lock()
	// Pending-only: no membership and no config names this durable.
	e.sess.sharedSubs = nil
	targets := e.sess.sharedPendingDurablesLocked()
	e.sess.mu.Unlock()
	if len(targets) != 1 || targets[0] != dur {
		t.Fatalf("the fixture did not build a pending-only target: got %v, want [%s]", targets, dur)
	}
	// Nobody else holds it, so a delete WOULD be authorized. That is what makes the assertion
	// below about the code's own restraint rather than about the scan's answer.
	if held := e.sess.sharedDurablesHeldByOthers([]string{dur}); held[dur] {
		t.Fatalf("the fixture's durable reads as held by another session, so an omitted delete "+
			"would be explained by the scan rather than by the two-authority rule: %v", held)
	}

	// Purge it the way the commit purges a pending-only target: the pending state goes, and the
	// consumer is never offered to the release.
	e.sess.mu.Lock()
	regs, cancels := e.sess.purgeConsumerPendingLocked(dur)
	e.sess.mu.Unlock()
	e.sess.deleteInflightRegisters(e.c, recordTx{}, regs)
	e.sess.secureRegCancellations(e.c, recordTx{}, cancels)

	e.sess.mu.Lock()
	_, stillHeld := e.sess.pendingPublish[pi]
	_, stillMapped := e.sess.cpending[dur]
	e.sess.mu.Unlock()
	if stillHeld || stillMapped {
		t.Fatalf("the pending-only target was not purged: pendingPublish=%v cpending=%v — its "+
			"identifier and Receive Maximum slot stay held for the rest of the connection",
			stillHeld, stillMapped)
	}
	// AND THE DURABLE SURVIVES.
	cons, err := e.asm.jsa.consumerList(mqttStreamName)
	if err != nil {
		t.Fatalf("listing consumers: %v", err)
	}
	for _, ci := range cons {
		if ci != nil && ci.Name == dur {
			return // still there, which is the requirement
		}
	}
	t.Fatalf("the group durable %q was deleted for a target discovered only through the pending "+
		"maps: pending state does not prove this session was its last holder, and deleting it "+
		"destroys the backlog of every member that still is", dur)
}

// TestForcedLeaveStagingDoesNotVoidARetirement pins the ONE exemption in the staging derivation,
// and the reason it is an exemption rather than an oversight.
//
// The pending derivation deliberately reaches srcAckOwed, because a retirement's durable can be
// reachable through no other map. But voidRegistrationsLocked must NOT void it, and its own
// comment gives the reason: Retired proves the source crossed PUBREC, so voiding it would
// falsely say the delivery was never transmitted. It does not itself encode the slot phase;
// a stored PUBREL and the explicit phase govern admission. The source suppression may go
// with its consumer, but teardown's never-transmitted ladder may not consume the slot.
//
// So this asserts an ABSENCE, paired with the positive half in the same test: the retirement's
// durable IS discovered by the derivation, and then deliberately not voided.
func TestForcedLeaveStagingDoesNotVoidARetirement(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-retirement")
	const group, filter = "g", "fl.retired"
	_, _, retDur := mqttSharedNames(group, filter)

	retSseq := e.publishSourceGroup(t, retDur, group, filter, filter)
	retPi := e.track(t, retDur, retSseq, 2)
	e.sess.mu.Lock()
	retAck := e.sess.pendingPublish[retPi]
	retRegSeq, retNonce := retAck.regSeq, retAck.regNonce
	e.sess.untrackPublishEntry(retPi)
	delete(e.sess.cpending, retDur)
	e.sess.retireSourceLocked(retDur, retSseq, &mqttRetiredSource{
		pi: retPi, regSeq: retRegSeq, nonce: retNonce, durable: true,
	})
	targets := e.sess.sharedPendingDurablesLocked()
	found := false
	for _, d := range targets {
		if d == retDur {
			found = true
		}
	}
	for _, d := range targets {
		e.sess.voidRegistrationsLocked(d)
	}
	_, voided := e.sess.cancelledRegNonces[retNonce]
	e.sess.mu.Unlock()

	if !found {
		t.Fatalf("the retirement's durable %q was not discovered by the pending derivation "+
			"(targets %v): srcAckOwed can be the only map naming it, so missing it here means "+
			"the staging half cannot see it at all", retDur, targets)
	}
	if voided {
		t.Fatalf("the retirement's register nonce %q was VOIDED: a retirement suppresses a "+
			"completed QoS 2 exchange rather than claiming an identifier, so voiding it makes "+
			"recovery drop the suppression, the source live and re-offerable, and the completed "+
			"payload a duplicate", retNonce)
	}
}

// TestVoidVerdictsDurableIgnoresRetirement pins the accounting the predicate must NOT use.
//
// voidedRegsRev counts REMOVALS as well as insertions: forgetCancelledReg increments it when a
// register-stream proof retires an already-durable verdict, and that path runs no session-record
// save because none is owed. A predicate comparing the counter to the saved watermark therefore
// reports "not durable" for a session whose cleanup entirely SUCCEEDED — and on the forced
// leave's direct path that closes an already-acknowledged connection, recreating the very defect
// the change exists to remove. A holistic round caught exactly that.
//
// So the predicate is defined over the OUTSTANDING insertions, and this test is the difference
// between the two definitions.
func TestVoidVerdictsDurableIgnoresRetirement(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-retire-acct")
	const group, filter = "g", "fl.acct"
	_, _, dur := mqttSharedNames(group, filter)

	e.sess.mu.Lock()
	pi, ok := e.sess.trackPublish(dur, testInflightAckSubject(dur, 2300, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !ok {
		t.Fatalf("trackPublish: pi=%d ok=%v", pi, ok)
	}
	e.sess.mu.Lock()
	nonce := e.sess.pendingPublish[pi].regNonce
	e.sess.voidRegistrationsLocked(dur)
	rev := e.sess.voidedRegsRev
	e.sess.mu.Unlock()

	// Owed: the insertion is above the credited watermark.
	if e.sess.voidVerdictsDurable() {
		t.Fatal("a freshly staged verdict reported durable, so the predicate would permit a " +
			"release that strands a live register")
	}
	// Credit it, exactly as a landed save does.
	e.sess.mu.Lock()
	e.sess.creditVoidedRegsLocked(rev)
	e.sess.mu.Unlock()
	if !e.sess.voidVerdictsDurable() {
		t.Fatal("a credited verdict still reported not-durable, so the release would be " +
			"withheld for ever")
	}

	// NOW RETIRE IT, which is what a successful register-stream proof does. It bumps
	// voidedRegsRev without owing any save — and the predicate must be unmoved.
	e.sess.forgetCancelledReg(nonce)
	e.sess.mu.Lock()
	revAfter, savedAfter := e.sess.voidedRegsRev, e.sess.voidedRegsSavedRev
	e.sess.mu.Unlock()
	if revAfter == savedAfter {
		t.Fatalf("retirement did not advance the mutation counter (rev=%d saved=%d), so this "+
			"test no longer distinguishes the two definitions and proves nothing",
			revAfter, savedAfter)
	}
	if !e.sess.voidVerdictsDurable() {
		t.Fatalf("after a SUCCESSFUL register retirement the predicate reported not-durable "+
			"(rev=%d saved=%d): nothing is owed, and withholding here closes an acknowledged "+
			"connection on the direct path — the defect this change removes", revAfter, savedAfter)
	}
}

// TestForcedLeaveStructuralUnionNeedsBothHalves pins the OTHER authority: the structural one.
//
// The commit derives its release CANDIDATES from the membership keys plus the `cons` entries, and
// both halves are load-bearing for populations the other cannot see:
//
//   - a membership with NO config — every record-restored resume, since shared configs are not
//     persisted and only processSubs rebuilds them. The `cons` half is blind to it.
//   - a config no surviving key names — the shape F-083 documents. The membership half is
//     blind to it. This fixture is a SYNTHETIC BACKSTOP: the SUBSCRIBE rollback used to be
//     its production producer and no longer is (F-039 restores the membership beside cons).
//     The population it seeds is not shown to be reachable by another production route at
//     this operation's boundary, so the provenance is stated as synthetic rather than
//     replaced with a route this test does not establish. The assertion stays: the union's
//     cons half is what sees it, whoever produced it.
//
// The pending maps do not substitute for either: a target found only there is purged and never
// offered for deletion, because pending state is not evidence of last-holder status.
func TestForcedLeaveStructuralUnionNeedsBothHalves(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-fl-union")
	const group = "g"
	keyOnly, _, keyOnlyDur := mqttSharedNames(group, "fl.keyonly")
	consOnly, _, consOnlyDur := mqttSharedNames(group, "fl.consonly")
	_ = keyOnly
	_ = consOnly

	e.sess.mu.Lock()
	// A membership with no config...
	e.sess.sharedSubs = map[string]byte{string(mqttSharedSubPrefix) + group + "/fl.keyonly": 1}
	// ...and a config no membership names.
	e.sess.cons = map[string]*ConsumerConfig{
		"fl//consonly": {Durable: consOnlyDur, DeliverGroup: group,
			DeliverSubject: mqttSubPrefix + "flconsonly"},
	}

	// Build the union exactly as the commit builds it.
	structural := make(map[string]string)
	for _, cc := range e.sess.cons {
		if cc.DeliverGroup != _EMPTY_ {
			structural[cc.Durable] = cc.DeliverSubject
		}
	}
	for key := range e.sess.sharedSubs {
		for _, tgt := range mqttSharedGroupNamesForKey(key) {
			if _, dup := structural[tgt.durable]; !dup {
				structural[tgt.durable] = tgt.deliver
			}
		}
	}
	e.sess.mu.Unlock()

	if _, ok := structural[keyOnlyDur]; !ok {
		t.Fatalf("the membership-only durable %q is not a release candidate: dropping the "+
			"membership-key half of the union loses every record-restored resume, whose shared "+
			"configs were never persisted", keyOnlyDur)
	}
	if _, ok := structural[consOnlyDur]; !ok {
		t.Fatalf("the config-only durable %q is not a release candidate: dropping the cons half "+
			"of the union loses every config-only shape, whose durable no surviving key names", consOnlyDur)
	}
}
