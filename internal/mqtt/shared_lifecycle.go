// Copyright 2026 The mqtt-nats-broker Authors
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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
)

// The shared-group lifecycle: a group durable is HELD BY ARBITRATION, not counted by a scan
// (F-105, the shared-group lifecycle transfer).
//
// # What this replaces
//
// A shared-subscription group consumer on $MQTT_msgs used to be named by a pure function of
// (ShareName, subject) and deleted by whichever session decided it was the last holder — a
// non-snapshot walk of every session record. A walk decides by the ABSENCE of a reference
// across a record set, so it could never fence a concurrent join, and a deterministic name
// let a late delete reach the consumer a new joiner had just created. Both are gone.
//
// # The two records, both on $MQTT_shared
//
//   - the GROUP RECORD, one per logical group: which PHYSICAL incarnation the group currently
//     uses (an ordinal, never reused), how many acquisitions hold it, and which retired
//     incarnations still owe a consumer deletion;
//   - the ACQUISITION LEDGER, one per session generation: exactly the acquisitions that
//     generation holds, whether it is still open to new ones, and which TENURE of the
//     generation may acquire.
//
// A JetStream atomic batch is per stream, so a join or a release commits its group member and
// its ledger member together. The session record on $MQTT_sess cannot be in that batch; what
// joins the two streams is the acquisition NONCE: the record CAS arbitrates every DISPOSITION
// of an acquisition (its commit, its removal, its cancellation), the ledger CAS arbitrates the
// acquisition and its release, and the tenure the resume installs (3.2 of the plan) projects
// the record's ownership decision onto the ledger, where no $MQTT_sess write reaches.
//
// # Nothing here decides from an absence
//
// Retirement of an incarnation is derived from ONE record's contents under the validator's
// state machine (k < next, k not live, k not retiring), never from a consumer not being found.
// Settlement of a release is the ledger, read after the writer's own batch or its refusal, no
// longer listing the acquisition. Neither record is ever deleted within the recovery epoch: a
// sealed ledger is the permanent fence that excludes a stale actor's late write, on the same
// ground the F-105 closure records rest on (docs/ARCHITECTURE.md, "Storage this mechanism
// accepts").

const (
	// mqttSharedGroupClassTk and mqttSharedLedgerClassTk are the two record classes, on the
	// dedicated $MQTT_shared stream (mqttSharedLifecycleStreamSubjectPrefix), NOT on
	// $MQTT_inflight: a permanent record co-located with the in-flight registers keeps their
	// storage block from collapsing, which is the neighbour condition the MaxMsgsPer revival
	// defect needs (docs/known-upstream-issues.md). Their own stream keeps $MQTT_inflight
	// collapsible. Only the shared-lifecycle writer below writes these two classes.
	mqttSharedGroupClassTk  = "grp"
	mqttSharedLedgerClassTk = "shl"

	// mqttSharedGroupKeyLen is the width of the group key, the same 16-character hash of
	// (ShareName, subject) the deterministic names were built from.
	mqttSharedGroupKeyLen = 16

	// mqttSharedRetiringMax bounds the retiring list of one group record, and with it the
	// record's size. A join that would mount a new incarnation against a full list first
	// finishes the entries it can and otherwise FAILS its filter (a retryable SUBACK failure) —
	// a client-visible refusal reachable only after this many consecutive ambiguous deletions
	// of one group's consumers, stated in docs/CONFORMANCE.md.
	mqttSharedRetiringMax = 16

	// mqttSharedIncMaxDigits is the width of an incarnation ordinal in a name: math.MaxUint64.
	mqttSharedIncMaxDigits = 20

	// mqttSharedAcquireAttempts bounds one acquisition's settlement loop, the
	// mqttCloseEpisodeAttempts pattern: exhausting it fails the filter and enters abort
	// settlement, after which no attempt of that nonce is issued again.
	mqttSharedAcquireAttempts = 4
)

// mqttSharedLedgerMaxAcqs bounds one generation's ledger, DERIVED rather than chosen: an
// acquisition is reserved only after the charge ceiling admitted the whole filter, one key
// holds at most two acquisitions (its own group and the level-up parent), a key with a
// membership reuses it, and a release removes its entry in the same batch.
func mqttSharedLedgerMaxAcqs(maxAckPending int) int {
	if maxAckPending <= 0 {
		maxAckPending = mqttDefaultMaxAckPending
	}
	return 2 * ((mqttMaxAckTotalLimit + maxAckPending - 1) / maxAckPending)
}

// ---------------------------------------------------------------------------
// Names and subjects
// ---------------------------------------------------------------------------

// mqttSharedGroupKey is the logical group's key: what every member of a group derives, and
// what the queue group (the LOGICAL identity) and the group record are named by.
func mqttSharedGroupKey(group, subject string) string {
	return getHashSize(group+" "+subject, mqttSharedGroupKeyLen)
}

// mqttValidSharedGroupKey reports whether s is a well-formed group key: exactly
// mqttSharedGroupKeyLen characters of the hash alphabet.
func mqttValidSharedGroupKey(s string) bool {
	if len(s) != mqttSharedGroupKeyLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// mqttSharedGroupSubject is the group record's subject. Per GROUP, not per client: the record
// arbitrates between every session that joins, so no client-scoped address could hold it.
func mqttSharedGroupSubject(qkey string) string {
	return mqttSharedLifecycleStreamSubjectPrefix + mqttSharedGroupClassTk + "." + qkey
}

// mqttSharedGroupWalkFilter enumerates every group record, for the sweep's retiring pass.
func mqttSharedGroupWalkFilter() string {
	return mqttSharedLifecycleStreamSubjectPrefix + mqttSharedGroupClassTk + ".*"
}

// mqttSharedLedgerSubject is one session generation's ledger. Per GENERATION and permanent:
// once sealed it is the fence that refuses a stale actor of that generation for ever.
func mqttSharedLedgerSubject(idHash, gen string) string {
	return mqttSharedLifecycleStreamSubjectPrefix + mqttSharedLedgerClassTk + "." + idHash + "." + gen
}

// validateSharedLifecycleStream verifies $MQTT_shared can carry the group records and the
// acquisition ledgers. The list is derived from what THIS owner does, the doctrine every
// validator here follows: a group record is CAS-arbitrated, an acquisition ledger is sealed,
// a join commits the pair in one atomic batch, and neither record is ever deleted.
func validateSharedLifecycleStream(si *StreamInfo, wantReplicas int, bundleEpoch string) error {
	if si == nil {
		return fmt.Errorf("stream info unavailable")
	}
	cfg := si.Config
	if err := validateBrokerOwnedStreamShape(&cfg, []string{mqttSharedLifecycleStreamSubjectPrefix + ">"}, wantReplicas); err != nil {
		return err
	}
	if !cfg.AllowAtomicPublish {
		// A join commits the group record and the acquisition ledger as one atomic batch,
		// which a stream without this rejects. Refused here, never patched: the broker-owned
		// streams are one recreation unit (F-066).
		return fmt.Errorf("allow_atomic is not set: a shared-group join commits its group record and acquisition ledger in one atomic batch, which the stream would reject")
	}
	if cfg.Retention != LimitsPolicy {
		return fmt.Errorf("retention is %v (want limits): nothing consumes these records, so any other retention drops them immediately", cfg.Retention)
	}
	if cfg.Discard != DiscardOld {
		// A CAS rewrite must replace the subject's record, not be rejected: DiscardNew turns
		// a replacement into a rejection and wedges every arbitration.
		return fmt.Errorf("discard is %v (want old)", cfg.Discard)
	}
	if cfg.MaxMsgsPer != 1 {
		// Each subject is a single-slot CAS register: a group per qkey, a ledger per
		// generation. More than one per subject appends, so a read returns the newest while
		// a stale arbitration survives underneath.
		return fmt.Errorf("max_msgs_per_subject is %d (want 1)", cfg.MaxMsgsPer)
	}
	if cfg.MaxMsgs > 0 || cfg.MaxBytes > 0 || cfg.MaxAge > 0 {
		// These records are permanent within the recovery epoch. Any global limit would evict
		// a live group's arbitration or a generation's fence.
		return fmt.Errorf("global limits set (max_msgs=%d max_bytes=%d max_age=%v; want all unlimited): they would evict a live group record or a generation's ledger",
			cfg.MaxMsgs, cfg.MaxBytes, cfg.MaxAge)
	}
	if len(cfg.Sources) > 0 {
		return fmt.Errorf("stream has sources (want none): a foreign record could occupy an arbitration's CAS slot")
	}
	if cfg.MaxMsgSize > 0 {
		return fmt.Errorf("max_msg_size is %d (want unlimited): it would reject a legitimate group or ledger write", cfg.MaxMsgSize)
	}
	if !cfg.DenyDelete {
		// These records are never deleted within the recovery epoch: a group record is
		// CAS-rewritten and an acquisition ledger is sealed. deny_delete enforces that AND
		// keeps the stream immune to the MaxMsgsPer revival defect (docs/known-upstream-issues.md):
		// with no explicit delete there is no tombstone, so no evicted-then-deleted shape
		// exists for a raw-block recovery to revive.
		return fmt.Errorf("deny_delete is not set: these records are never deleted, and denying it keeps the stream immune to the upstream MaxMsgsPer revival defect")
	}
	if cfg.SubjectDeleteMarkerTTL != 0 {
		return fmt.Errorf("subject_delete_marker_ttl is %v (want 0): marker messages would be parsed as records", cfg.SubjectDeleteMarkerTTL)
	}
	// The bundle stamp: this stream carries the account's recovery epoch, which is what makes
	// the broker-owned streams one recreation unit rather than independently recoverable
	// stores (F-066).
	if err := validateBundleEpochStamp(&cfg, bundleEpoch); err != nil {
		return err
	}
	return nil
}

// mqttSharedIncToken renders an incarnation ordinal for a name.
func mqttSharedIncToken(inc uint64) string { return strconv.FormatUint(inc, 10) }

// mqttParseSharedIncName splits "<qkey>_<inc>" — the tail of an incarnation-qualified durable
// or deliver subject — back into its parts. The incarnation is inside ONE token so that
// "$MQTT.qsub.*" in an ACL still covers the deliver subject.
func mqttParseSharedIncName(tail string) (qkey string, inc uint64, ok bool) {
	i := strings.LastIndexByte(tail, '_')
	if i != mqttSharedGroupKeyLen {
		return _EMPTY_, 0, false
	}
	qkey = tail[:i]
	if !mqttValidSharedGroupKey(qkey) {
		return _EMPTY_, 0, false
	}
	digits := tail[i+1:]
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != digits {
		return _EMPTY_, 0, false
	}
	return qkey, n, true
}

// ---------------------------------------------------------------------------
// The group record
// ---------------------------------------------------------------------------

// mqttSharedLive is the incarnation a group currently serves and the acquisitions holding it.
type mqttSharedLive struct {
	Inc     uint64 `json:"inc"`
	Holders uint64 `json:"holders"`
}

// mqttSharedGroupRecord is one logical group's arbitration record.
//
//   - Live names the incarnation currently served; it is null exactly when nobody holds one,
//     and live.holders >= 1 whenever it is present (the validator refuses anything else).
//   - Retiring lists incarnations whose consumer deletion is OWED. An entry is removed only by
//     the CAS that follows a classified deletion outcome; a crash inside that work leaves the
//     entry standing for the sweep. Bounded by mqttSharedRetiringMax.
//   - Next is a monotone ordinal counter. Incarnations are never reused, which is what makes
//     "k < next, k != live.inc, k not in retiring" a DERIVATION that k passed through
//     live -> retiring -> deleted, rather than an inference from a consumer not being found.
type mqttSharedGroupRecord struct {
	Epoch    string          `json:"epoch"`
	Group    string          `json:"group"`
	Live     *mqttSharedLive `json:"live"`
	Retiring []uint64        `json:"retiring"`
	Next     uint64          `json:"next"`
}

var mqttSharedGroupMembers = map[string]struct{}{
	"epoch": {}, "group": {}, "live": {}, "retiring": {}, "next": {},
}

// mqttSharedGroupNonNullMembers are the group record's members that may never be null. `live`
// is the one member excluded: it is mandatory and legally null (see mqttDecodeSharedGroup).
var mqttSharedGroupNonNullMembers = map[string]struct{}{
	"epoch": {}, "group": {}, "retiring": {}, "next": {},
}

// mqttDecodeSharedGroup decodes and fully validates one group record body.
//
// `live` is a REQUIRED member that may be null — the one place this mechanism's records
// carry a legal null — because "nobody holds an incarnation" is a positive state the record
// has to be able to say, and an absent member would be indistinguishable from a body that
// forgot it.
func mqttDecodeSharedGroup(data []byte, wantEpoch, wantKey string) (*mqttSharedGroupRecord, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttSharedGroupMembers, "a shared group record"); err != nil {
		return nil, err
	}
	// Every member but `live` is mandatory and non-null; `live` is mandatory and MAY be null,
	// which the generic presence rule would refuse, so it is checked for presence alone.
	if err := mqttJSONMembersArePresent(data, mqttSharedGroupNonNullMembers, "a shared group record"); err != nil {
		return nil, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if _, ok := raw["live"]; !ok {
		return nil, mqttShapeErr("a shared group record member live is mandatory and absent; nobody holding an incarnation is written as null")
	}
	g := &mqttSharedGroupRecord{}
	if err := json.Unmarshal(data, g); err != nil {
		return nil, err
	}
	if err := mqttValidateSharedGroup(g, wantEpoch, wantKey); err != nil {
		return nil, err
	}
	return g, nil
}

// mqttValidateSharedGroup refuses a group record this broker cannot act on.
func mqttValidateSharedGroup(g *mqttSharedGroupRecord, wantEpoch, wantKey string) error {
	if g == nil {
		return fmt.Errorf("shared group record is absent")
	}
	if g.Epoch != wantEpoch {
		return fmt.Errorf("shared group record carries recovery epoch %q, not %q", g.Epoch, wantEpoch)
	}
	if !mqttValidSharedGroupKey(g.Group) {
		return fmt.Errorf("shared group record carries a malformed group key %q", g.Group)
	}
	if wantKey != _EMPTY_ && g.Group != wantKey {
		return fmt.Errorf("shared group record names group %q, not %q", g.Group, wantKey)
	}
	if g.Next == 0 {
		return fmt.Errorf("shared group record %q carries ordinal counter 0; incarnations start at 1", g.Group)
	}
	if g.Live != nil {
		if g.Live.Inc == 0 || g.Live.Inc >= g.Next {
			return fmt.Errorf("shared group record %q serves incarnation %d outside its allocated ordinals [1,%d)",
				g.Group, g.Live.Inc, g.Next)
		}
		if g.Live.Holders == 0 {
			return fmt.Errorf("shared group record %q serves incarnation %d with no holder: a live "+
				"incarnation has at least one, or it is not live", g.Group, g.Live.Inc)
		}
	}
	if g.Retiring == nil {
		// A nil list marshals as null, which the decoder refuses; refusing it here keeps a
		// composer from ever sending one.
		return fmt.Errorf("shared group record %q carries no retiring list; an empty list is []", g.Group)
	}
	if len(g.Retiring) > mqttSharedRetiringMax {
		return fmt.Errorf("shared group record %q carries %d retiring incarnations, above the bound %d",
			g.Group, len(g.Retiring), mqttSharedRetiringMax)
	}
	seen := make(map[uint64]struct{}, len(g.Retiring))
	for _, k := range g.Retiring {
		if k == 0 || k >= g.Next {
			return fmt.Errorf("shared group record %q retires incarnation %d outside its allocated ordinals [1,%d)",
				g.Group, k, g.Next)
		}
		if g.Live != nil && k == g.Live.Inc {
			return fmt.Errorf("shared group record %q retires the incarnation it serves, %d", g.Group, k)
		}
		if _, dup := seen[k]; dup {
			return fmt.Errorf("shared group record %q retires incarnation %d twice", g.Group, k)
		}
		seen[k] = struct{}{}
	}
	return nil
}

// isRetiring reports whether inc is owed a deletion.
func (g *mqttSharedGroupRecord) isRetiring(inc uint64) bool {
	for _, k := range g.Retiring {
		if k == inc {
			return true
		}
	}
	return false
}

// mqttSharedIncDisposition classifies one incarnation against a group record it READ.
type mqttSharedIncDisposition uint8

const (
	// mqttSharedIncLive: the incarnation the group serves. Retain.
	mqttSharedIncLive mqttSharedIncDisposition = iota + 1
	// mqttSharedIncRetiring: its deletion is owed and unfinished. Finish it.
	mqttSharedIncRetiring
	// mqttSharedIncRetired: DERIVED from the record — allocated, not live, not retiring — so it
	// passed through live -> retiring -> deleted. A consumer bearing its name is a delayed
	// create that landed after retirement and may be deleted under the record's authority.
	mqttSharedIncRetired
	// mqttSharedIncUnallocated: an ordinal the record has never issued. Impossible for a name
	// this broker minted; retained and reported.
	mqttSharedIncUnallocated
)

// classifyInc is the derived-retirement rule, stated once.
func (g *mqttSharedGroupRecord) classifyInc(inc uint64) mqttSharedIncDisposition {
	switch {
	case inc == 0 || inc >= g.Next:
		return mqttSharedIncUnallocated
	case g.Live != nil && g.Live.Inc == inc:
		return mqttSharedIncLive
	case g.isRetiring(inc):
		return mqttSharedIncRetiring
	default:
		return mqttSharedIncRetired
	}
}

// ---------------------------------------------------------------------------
// The acquisition ledger
// ---------------------------------------------------------------------------

// mqttSharedLedgerState is whether a generation is still open to acquisitions.
//
// From 1, as every F-105 enum is: a missing member must not decode as the permissive value.
type mqttSharedLedgerState uint8

const (
	mqttSharedLedgerOpen   mqttSharedLedgerState = 1
	mqttSharedLedgerSealed mqttSharedLedgerState = 2
)

// mqttSharedTenure is WHICH CONNECT of a generation may acquire, and it is ORDERED.
//
// Seq is the $MQTT_sess sequence at which that CONNECT's accept save landed — immutable for
// the attempt, named by the accept batch's PubAck — and ID a nonce minted with the attempt. A
// resume preserves the generation, so two brokers may legitimately hold one generation; the
// record CAS decides which of them owns the SESSION, and this member carries that decision
// onto $MQTT_shared, where no $MQTT_sess write reaches. Installation never regresses: a
// tenure with a smaller Seq is refused over a larger one, and a sealed ledger is never
// reopened.
type mqttSharedTenure struct {
	Seq uint64 `json:"seq"`
	ID  string `json:"id"`
}

func (t mqttSharedTenure) isZero() bool { return t.Seq == 0 && t.ID == _EMPTY_ }

// mqttSharedAcqRef names one acquisition: the group it holds, the incarnation it holds it on,
// and the nonce minted for it. The nonce is the identity that joins the ledger to the session
// record's membership and to its release verdicts.
type mqttSharedAcqRef struct {
	Group string `json:"group"`
	Inc   uint64 `json:"inc"`
	Acq   string `json:"acq"`
}

// mqttSharedLedger is one session generation's acquisition inventory.
//
// Acqs are exactly the acquisitions the generation currently holds — no history. A join
// appends one and a release removes one, each in the batch that moves the group count, which
// is what makes the group's holder count exact by construction (count discipline).
//
// Closed names the listed acquisitions CLOSED TO NEW DEPENDENCIES (F-105 7e): the first step of
// an open ledger's release, monotone until the release removes the entry, and what a shared
// admission's readiness refuses. A sealed ledger is closed generation-wide and needs no entry.
type mqttSharedLedger struct {
	Epoch  string                `json:"epoch"`
	ID     string                `json:"id"`
	Gen    string                `json:"gen"`
	State  mqttSharedLedgerState `json:"state"`
	Tenure mqttSharedTenure      `json:"tenure"`
	Acqs   []mqttSharedAcqRef    `json:"acqs"`
	Closed []string              `json:"closed"`
}

var mqttSharedLedgerMembers = map[string]struct{}{
	"epoch": {}, "id": {}, "gen": {}, "state": {}, "tenure": {}, "acqs": {}, "closed": {},
}

// mqttDecodeSharedLedger decodes and fully validates one ledger body.
func mqttDecodeSharedLedger(data []byte, wantID, wantEpoch, wantGen string) (*mqttSharedLedger, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttSharedLedgerMembers, "a shared acquisition ledger"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttSharedLedgerMembers, "a shared acquisition ledger"); err != nil {
		return nil, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if string(bytes.TrimSpace(raw["acqs"])) == "null" {
		return nil, mqttShapeErr("a shared acquisition ledger's acquisition list is null; an empty list is written as []")
	}
	if string(bytes.TrimSpace(raw["closed"])) == "null" {
		return nil, mqttShapeErr("a shared acquisition ledger's closed list is null; an empty list is written as []")
	}
	if string(bytes.TrimSpace(raw["tenure"])) == "null" {
		return nil, mqttShapeErr("a shared acquisition ledger's tenure is null")
	}
	l := &mqttSharedLedger{}
	if err := json.Unmarshal(data, l); err != nil {
		return nil, err
	}
	if err := mqttValidateSharedLedger(l, wantID, wantEpoch, wantGen); err != nil {
		return nil, err
	}
	return l, nil
}

// mqttValidateSharedLedger refuses a ledger body this broker cannot act on.
func mqttValidateSharedLedger(l *mqttSharedLedger, wantID, wantEpoch, wantGen string) error {
	if l == nil {
		return fmt.Errorf("shared acquisition ledger is absent")
	}
	if l.ID != wantID {
		return fmt.Errorf("shared acquisition ledger names client %q, not %q", l.ID, wantID)
	}
	if l.Epoch != wantEpoch {
		return fmt.Errorf("shared acquisition ledger carries recovery epoch %q, not %q", l.Epoch, wantEpoch)
	}
	if !mqttValidSessGen(l.Gen) {
		return fmt.Errorf("shared acquisition ledger carries a malformed generation %q", l.Gen)
	}
	if wantGen != _EMPTY_ && l.Gen != wantGen {
		return fmt.Errorf("shared acquisition ledger names generation %q, not %q", l.Gen, wantGen)
	}
	switch l.State {
	case mqttSharedLedgerOpen, mqttSharedLedgerSealed:
	default:
		return fmt.Errorf("shared acquisition ledger of generation %q is in unsupported state %d", l.Gen, l.State)
	}
	// A tenure is zero only on a seal written over absence — a generation that never acquired,
	// closed by its teardown. An OPEN ledger always names who may acquire.
	if l.Tenure.isZero() {
		if l.State != mqttSharedLedgerSealed || len(l.Acqs) != 0 {
			return fmt.Errorf("shared acquisition ledger of generation %q carries no tenure while open or holding acquisitions", l.Gen)
		}
	} else if l.Tenure.Seq == 0 || !mqttValidInflightNonce(l.Tenure.ID) {
		return fmt.Errorf("shared acquisition ledger of generation %q carries a malformed tenure %+v", l.Gen, l.Tenure)
	}
	if l.Acqs == nil {
		// A nil list marshals as null, which the decoder refuses; refusing it here keeps a
		// composer from ever sending one.
		return fmt.Errorf("shared acquisition ledger of generation %q carries no acquisition list; an empty list is []", l.Gen)
	}
	seen := make(map[string]struct{}, len(l.Acqs))
	for _, a := range l.Acqs {
		if !mqttValidSharedGroupKey(a.Group) {
			return fmt.Errorf("shared acquisition ledger of generation %q names a malformed group %q", l.Gen, a.Group)
		}
		if a.Inc == 0 {
			return fmt.Errorf("shared acquisition ledger of generation %q holds group %q on incarnation 0", l.Gen, a.Group)
		}
		if !mqttValidInflightNonce(a.Acq) {
			return fmt.Errorf("shared acquisition ledger of generation %q carries a malformed acquisition nonce %q", l.Gen, a.Acq)
		}
		if _, dup := seen[a.Acq]; dup {
			return fmt.Errorf("shared acquisition ledger of generation %q lists acquisition %q twice", l.Gen, a.Acq)
		}
		seen[a.Acq] = struct{}{}
	}
	if l.Closed == nil {
		return fmt.Errorf("shared acquisition ledger of generation %q carries no closed list; an empty list is []", l.Gen)
	}
	closed := make(map[string]struct{}, len(l.Closed))
	for _, c := range l.Closed {
		if _, listed := seen[c]; !listed {
			return fmt.Errorf("shared acquisition ledger of generation %q closes acquisition %q, which it does not list", l.Gen, c)
		}
		if _, dup := closed[c]; dup {
			return fmt.Errorf("shared acquisition ledger of generation %q closes acquisition %q twice", l.Gen, c)
		}
		closed[c] = struct{}{}
	}
	return nil
}

// isClosed reports whether acquisition acq is closed to new dependencies: listed in Closed, or
// the whole ledger sealed.
func (l *mqttSharedLedger) isClosed(acq string) bool {
	if l == nil {
		return false
	}
	if l.State == mqttSharedLedgerSealed {
		return true
	}
	for _, c := range l.Closed {
		if c == acq {
			return true
		}
	}
	return false
}

// mqttSharedCopyStrings copies a closed list into a NON-NIL slice, for the reason
// mqttSharedCopyAcqs gives.
func mqttSharedCopyStrings(v []string) []string {
	out := make([]string, len(v))
	copy(out, v)
	return out
}

// mqttSharedStringsWithout returns v minus every occurrence of s, as a non-nil slice.
func mqttSharedStringsWithout(v []string, s string) []string {
	out := make([]string, 0, len(v))
	for _, x := range v {
		if x != s {
			out = append(out, x)
		}
	}
	return out
}

// mqttSharedStringsEqual compares two closed lists entry for entry, in order.
func mqttSharedStringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// lists reports whether the ledger lists acquisition acq, and the entry when it does.
func (l *mqttSharedLedger) lists(acq string) (mqttSharedAcqRef, bool) {
	if l == nil {
		return mqttSharedAcqRef{}, false
	}
	for _, a := range l.Acqs {
		if a.Acq == acq {
			return a, true
		}
	}
	return mqttSharedAcqRef{}, false
}

// mqttSharedAcqsEqual compares two acquisition lists entry for entry, in order.
func mqttSharedAcqsEqual(a, b []mqttSharedAcqRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// mqttSharedCopyAcqs copies an acquisition list into a NON-NIL slice. A nil slice marshals as
// JSON null, which the decoder refuses (an empty list is written as []), so every composer that
// carries a list forward copies it through here rather than through append onto nil, which
// returns nil for an empty source.
func mqttSharedCopyAcqs(acqs []mqttSharedAcqRef) []mqttSharedAcqRef {
	out := make([]mqttSharedAcqRef, len(acqs))
	copy(out, acqs)
	return out
}

// mqttSharedCopyUint64s is mqttSharedCopyAcqs for a retiring list.
func mqttSharedCopyUint64s(v []uint64) []uint64 {
	out := make([]uint64, len(v))
	copy(out, v)
	return out
}

// mqttSharedAcqsWithout returns acqs minus exactly the entry named acq, and whether it was there.
func mqttSharedAcqsWithout(acqs []mqttSharedAcqRef, acq string) ([]mqttSharedAcqRef, bool) {
	out := make([]mqttSharedAcqRef, 0, len(acqs))
	found := false
	for _, a := range acqs {
		if a.Acq == acq && !found {
			found = true
			continue
		}
		out = append(out, a)
	}
	return out, found
}

// ---------------------------------------------------------------------------
// The membership, as the session record carries it
// ---------------------------------------------------------------------------

// mqttSharedAcquisition is one acquisition reference the session record carries: which
// incarnation of the group the membership is on, and the nonce that names its ledger entry.
type mqttSharedAcquisition struct {
	Inc uint64 `json:"inc"`
	Acq string `json:"acq"`
}

// mqttSharedMembership is the persisted value of one shared subscription key.
//
// It keeps the session's own configuration (the granted QoS) plus its ACQUISITION REFERENCES,
// and it is no longer an independently interpreted holder registry: a session entry alone
// authorizes neither serving nor recreating; its absence authorizes no release (a release is
// authorized by a VERDICT, mqttPersistedSession.AcqReleases); a ledger entry may exist before
// the session entry commits; and release requires durable settlement of the acquisition,
// never a comparison of two snapshots.
//
// Main is the filter's own group. Up is the level-up companion a "foo.>" filter also joins
// (the "a/#" matches "a" rule), and is absent for every other filter.
//
// The pre-transfer shape — a bare QoS byte per key — does not decode into this struct, so a
// record carrying it is REFUSED at every ingress rather than interpreted: an unsupported
// persisted shape is never migrated (F-056, F-057, F-058).
type mqttSharedMembership struct {
	QoS  byte                   `json:"qos"`
	Main mqttSharedAcquisition  `json:"main"`
	Up   *mqttSharedAcquisition `json:"up,omitempty"`
}

// acquisitions returns the membership's acquisition references, main first.
func (m mqttSharedMembership) acquisitions() []mqttSharedAcquisition {
	if m.Up == nil {
		return []mqttSharedAcquisition{m.Main}
	}
	return []mqttSharedAcquisition{m.Main, *m.Up}
}

// mqttValidateSharedMembership refuses a membership value this broker never writes.
//
// The key decides whether a companion is owed: a multi-level filter joins two groups and a
// single-level one joins one, so a value disagreeing with its key describes a join this
// broker did not make.
func mqttValidateSharedMembership(key string, m mqttSharedMembership) error {
	group, subject, ok := mqttParseSharedKey(key)
	if !ok {
		return fmt.Errorf("shared subscription key %q does not parse", key)
	}
	if m.QoS > 2 {
		return fmt.Errorf("shared subscription %q carries an invalid QoS %d", key, m.QoS)
	}
	if m.Main.Inc == 0 || !mqttValidInflightNonce(m.Main.Acq) {
		return fmt.Errorf("shared subscription %q carries a malformed acquisition reference %+v", key, m.Main)
	}
	needUp := mqttNeedSubForLevelUp(subject)
	switch {
	case needUp && m.Up == nil:
		return fmt.Errorf("shared subscription %q is a multi-level filter with no level-up acquisition", key)
	case !needUp && m.Up != nil:
		return fmt.Errorf("shared subscription %q is a single-level filter carrying a level-up acquisition", key)
	case needUp && (m.Up.Inc == 0 || !mqttValidInflightNonce(m.Up.Acq)):
		return fmt.Errorf("shared subscription %q carries a malformed level-up acquisition %+v", key, *m.Up)
	}
	_ = group
	return nil
}

// ---------------------------------------------------------------------------
// Loads
// ---------------------------------------------------------------------------

// mqttSharedGroupLoad is a group record read together with the sequence a CAS must name.
type mqttSharedGroupLoad struct {
	rec    *mqttSharedGroupRecord
	seq    uint64
	status mqttOutboundLoadStatus
	// err says WHY a corrupt or indeterminate read is refused, for the diagnostic a refusal
	// carries; it decides nothing.
	err error
}

// loadSharedGroup reads one group record. Corrupt and indeterminate are never absent: a record
// this broker cannot interpret may name a live incarnation, so every caller refuses the
// decision that depends on it.
func (jsa *mqttJSA) loadSharedGroup(epoch, qkey string) mqttSharedGroupLoad {
	if h := mqttTestSharedGroupLoadHookV.Load(); h != nil {
		if l, handled := (*h)(qkey); handled {
			return l
		}
	}
	smsg, err := jsa.loadLastMsgFor(mqttSharedLifecycleStreamName, mqttSharedGroupSubject(qkey))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return mqttSharedGroupLoad{status: mqttOutboundIndeterminate, err: err}
		}
		return mqttSharedGroupLoad{status: mqttOutboundAbsent}
	}
	g, derr := mqttDecodeSharedGroup(smsg.Data, epoch, qkey)
	if derr != nil {
		return mqttSharedGroupLoad{seq: smsg.Sequence, status: mqttOutboundCorrupt, err: derr}
	}
	return mqttSharedGroupLoad{rec: g, seq: smsg.Sequence, status: mqttOutboundValid}
}

// mqttSharedLedgerLoad is a ledger read together with the sequence a CAS must name.
type mqttSharedLedgerLoad struct {
	rec    *mqttSharedLedger
	seq    uint64
	status mqttOutboundLoadStatus
	// err says WHY a corrupt or indeterminate read is refused; it decides nothing.
	err error
}

// loadSharedLedger reads one generation's ledger.
func (jsa *mqttJSA) loadSharedLedger(id, epoch, gen string) mqttSharedLedgerLoad {
	smsg, err := jsa.loadLastMsgFor(mqttSharedLifecycleStreamName, mqttSharedLedgerSubject(getHash(id), gen))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			return mqttSharedLedgerLoad{status: mqttOutboundIndeterminate, err: err}
		}
		return mqttSharedLedgerLoad{status: mqttOutboundAbsent}
	}
	l, derr := mqttDecodeSharedLedger(smsg.Data, id, epoch, gen)
	if derr != nil {
		return mqttSharedLedgerLoad{seq: smsg.Sequence, status: mqttOutboundCorrupt, err: derr}
	}
	return mqttSharedLedgerLoad{rec: l, seq: smsg.Sequence, status: mqttOutboundValid}
}

// ---------------------------------------------------------------------------
// The batch shapes and their write boundary
// ---------------------------------------------------------------------------

// mqttSharedShape is the closed set of batches the shared-lifecycle writer sends. Each shape
// has its own rule set below; a shape with no rule fails closed at the default.
type mqttSharedShape uint8

const (
	// mqttSharedShapeJoin: group + ledger. Under tenure T, the ledger gains exactly one entry
	// and the group gains exactly one holder (or mounts a fresh incarnation with one).
	mqttSharedShapeJoin mqttSharedShape = iota + 1
	// mqttSharedShapeRelease: group + ledger, ANY tenure. Authorized by a record verdict or the
	// seal: the ledger loses exactly one entry and the group one holder; the last holder moves
	// the incarnation into retiring.
	mqttSharedShapeRelease
	// mqttSharedShapeInstall: ledger alone. A resume's tenure installation, under the
	// non-regression rule; over absence it is also the abort's INIT FENCE.
	mqttSharedShapeInstall
	// mqttSharedShapeSeal: ledger alone. The closing's fence, authorized by the teardown claim
	// and never by tenure; preserves acquisitions and tenure.
	mqttSharedShapeSeal
	// mqttSharedShapeRetireDone: group alone. Removes exactly one retiring entry after its
	// consumer's deletion was classified removed or gone.
	mqttSharedShapeRetireDone
	// mqttSharedShapeBump: ledger alone. The abort fence over a present ledger under our own
	// tenure: the body is byte-identical and only the sequence moves.
	mqttSharedShapeBump
	// mqttSharedShapeClose: ledger alone, ANY tenure. The first step of an open ledger's release
	// (F-105 7e), authorized by the verdict the caller executes: exactly one listed acquisition
	// becomes closed to new dependencies, and nothing else moves.
	mqttSharedShapeClose
)

func (s mqttSharedShape) String() string {
	switch s {
	case mqttSharedShapeJoin:
		return "join"
	case mqttSharedShapeRelease:
		return "release"
	case mqttSharedShapeInstall:
		return "install"
	case mqttSharedShapeSeal:
		return "seal"
	case mqttSharedShapeRetireDone:
		return "retire-done"
	case mqttSharedShapeBump:
		return "bump"
	case mqttSharedShapeClose:
		return "close"
	}
	return "unknown"
}

// mqttSharedBatch is one attempted write, assembled by a composer below and committed by
// commitSharedLifecycleBatch, which validates it against the PRIOR records it was composed
// from and compares those with the store before sending.
type mqttSharedBatch struct {
	Shape mqttSharedShape
	Epoch string

	// The group side, present for join, release and retire-done.
	Qkey        string
	GroupPrior  *mqttSharedGroupRecord // nil when composed over absence
	GroupSeq    uint64                 // the sequence GroupPrior was read at; 0 over absence
	GroupNext   *mqttSharedGroupRecord
	hasGroup    bool
	groupExpect uint64

	// The ledger side, present for join, release, install, seal and bump.
	ID           string
	Gen          string
	LedgerPrior  *mqttSharedLedger // nil when composed over absence
	LedgerSeq    uint64
	LedgerNext   *mqttSharedLedger
	hasLedger    bool
	ledgerExpect uint64

	// Tenure is the tenure the composer holds: required for join, install and bump.
	Tenure mqttSharedTenure
	// Acq is the acquisition a join adds or a release removes.
	Acq mqttSharedAcqRef
	// RetiredInc is the entry a retire-done removes.
	RetiredInc uint64
	// LedgerMax is the ledger bound a join is checked against (mqttSharedLedgerMaxAcqs of the
	// configured charge); 0 means the default charge's bound.
	LedgerMax int
}

// mqttSharedWriteOutcome classifies what the store said about one batch.
type mqttSharedWriteOutcome uint8

const (
	// mqttSharedWriteLanded: acknowledged, every member stored.
	mqttSharedWriteLanded mqttSharedWriteOutcome = iota + 1
	// mqttSharedWriteRejected: a CAS refused it and nothing was stored (10071, or 10164's
	// clustered contention). The caller re-reads and re-composes.
	mqttSharedWriteRejected
	// mqttSharedWriteAmbiguous: no acknowledgement. The batch may still land; the caller
	// settles by re-reading and, for a join, by re-attempting the IDENTICAL batch or fencing.
	mqttSharedWriteAmbiguous
)

// mqttSharedWriteResult is the outcome and, when landed, the sequences the members took.
type mqttSharedWriteResult struct {
	Outcome   mqttSharedWriteOutcome
	GroupSeq  uint64
	LedgerSeq uint64
	Err       error
}

// mqttTestSharedComposeHookV fires once per batch commitSharedLifecycleBatch is asked to send,
// with the shape and the acquisition nonce (empty for shapes that carry none), BEFORE the
// boundary validates it. A test counts compositions here to assert a caller's layer — that a
// stale tenure composed no join after observing a foreign one, that a superseded resume
// composed no second installation — independently of the boundary's refusal, which is
// asserted by its own tests.
var mqttTestSharedComposeHookV atomic.Pointer[func(shape mqttSharedShape, acq string)]

// mqttTestSharedGroupLoadHookV substitutes one group record's load, so a test can present a
// reading the store will not produce on demand (an unreadable record for one entry of a
// closing) and prove the fail-closed arm positively.
var mqttTestSharedGroupLoadHookV atomic.Pointer[func(qkey string) (mqttSharedGroupLoad, bool)]

// mqttTestSharedWireHookV intercepts the FRAMED messages of one batch after the boundary has
// validated them and before they are sent, and may hold or alter them. This is the injection
// point a mutation needs to make a stale composition reach the store with its expectations
// stripped: a mutation at any earlier layer is refused by the boundary and proves nothing.
var mqttTestSharedWireHookV atomic.Pointer[func(shape mqttSharedShape, msgs []mqttBatchMsg) []mqttBatchMsg]

// mqttTestSharedPauseHookV pauses a schedule at a named point.
var mqttTestSharedPauseHookV atomic.Pointer[func(point string)]

// mqttTestSharedDeleteHookV overrides the CLASSIFIED outcome of one incarnation's consumer
// deletion, so a test can force the AMBIGUOUS result the real JetStream delete will not
// produce on demand — the reading that leaves a retiring entry standing for the sweep.
// Returns (outcome, true) to override, (_, false) to let the real delete run. Nil in
// production.
var mqttTestSharedDeleteHookV atomic.Pointer[func(qkey string, inc uint64) (mqttSharedRetireOutcome, bool)]

func mqttTestSharedPause(point string) {
	if h := mqttTestSharedPauseHookV.Load(); h != nil {
		(*h)(point)
	}
}

// commitSharedLifecycleBatch is THE ONE writer for both record classes.
//
// Every rule about which write is legal lives here, at the write boundary, against the prior
// records the batch was composed from and against the store as it stands. A caller that
// assembled a legal-looking batch by hand meets the same rules a composer's does.
func (jsa *mqttJSA) commitSharedLifecycleBatch(b *mqttSharedBatch) mqttSharedWriteResult {
	if h := mqttTestSharedComposeHookV.Load(); h != nil {
		(*h)(b.Shape, b.Acq.Acq)
	}
	if err := mqttCheckSharedLifecycleBatch(b); err != nil {
		return mqttSharedWriteResult{Err: fmt.Errorf("refusing to send an unsupported %s batch: %w", b.Shape, err)}
	}
	// THE PRIORS ARE COMPARED WITH THE STORE, not merely validated as bodies a caller handed
	// in: every rule above draws conclusions about STORED state from the caller's struct.
	if b.hasGroup {
		cur := jsa.loadSharedGroup(b.Epoch, b.Qkey)
		if perr := mqttSharedPriorMatches(cur.status, cur.seq, b.GroupPrior != nil, b.GroupSeq, "group record "+b.Qkey); perr != nil {
			return mqttSharedWriteResult{Outcome: mqttSharedWriteRejected, Err: perr}
		}
	}
	if b.hasLedger {
		cur := jsa.loadSharedLedger(b.ID, b.Epoch, b.Gen)
		if perr := mqttSharedPriorMatches(cur.status, cur.seq, b.LedgerPrior != nil, b.LedgerSeq, "ledger of generation "+b.Gen); perr != nil {
			return mqttSharedWriteResult{Outcome: mqttSharedWriteRejected, Err: perr}
		}
	}

	var msgs []mqttBatchMsg
	var groupIdx, ledgerIdx = -1, -1
	frame := func(subj string, expect uint64, body []byte, batchID string, seq int, commit bool) {
		m, h := mqttFrameBatchWrite(expect, body, batchID, seq, commit)
		msgs = append(msgs, mqttBatchMsg{subj: subj, hdr: h, msg: m})
	}
	two := b.hasGroup && b.hasLedger
	batchID := _EMPTY_
	if two {
		jsa.mu.Lock()
		batchID = jsa.nuid.Next()
		jsa.mu.Unlock()
	}
	seq := 1
	if b.hasLedger {
		body, _ := json.Marshal(b.LedgerNext)
		ledgerIdx = len(msgs)
		frame(mqttSharedLedgerSubject(getHash(b.ID), b.Gen), b.ledgerExpect, body, batchID, seq, two && !b.hasGroup)
		seq++
	}
	if b.hasGroup {
		body, _ := json.Marshal(b.GroupNext)
		groupIdx = len(msgs)
		frame(mqttSharedGroupSubject(b.Qkey), b.groupExpect, body, batchID, seq, two)
	}
	if h := mqttTestSharedWireHookV.Load(); h != nil {
		msgs = (*h)(b.Shape, msgs)
	}

	var resp *JSPubAckResponse
	var err error
	if two {
		var bi any
		bi, err = jsa.newAtomicBatchRequest(mqttJSAMsgStore, msgs)
		if err == nil {
			resp = bi.(*JSPubAckResponse)
			if aerr := resp.ToError(); aerr != nil {
				err = aerr
			} else if resp.BatchId != batchID || resp.BatchSize != uint64(len(msgs)) {
				return mqttSharedWriteResult{Outcome: mqttSharedWriteAmbiguous,
					Err: fmt.Errorf("the %s batch was acknowledged as %d message(s) under batch %q, but this broker sent %d under %q",
						b.Shape, resp.BatchSize, resp.BatchId, len(msgs), batchID)}
			}
		}
	} else {
		resp, err = jsa.storeMsg(msgs[0].subj, msgs[0].hdr, msgs[0].msg)
	}
	switch {
	case err == nil:
		out := mqttSharedWriteResult{Outcome: mqttSharedWriteLanded}
		// The PubAck names the LAST member's sequence; in a two-member batch the members take
		// consecutive sequences, so the earlier one is the sequence before it.
		last := resp.Sequence
		if groupIdx >= 0 && ledgerIdx >= 0 {
			out.LedgerSeq, out.GroupSeq = last-1, last
		} else if groupIdx >= 0 {
			out.GroupSeq = last
		} else {
			out.LedgerSeq = last
		}
		return out
	case mqttIsRecordCASRejection(err):
		return mqttSharedWriteResult{Outcome: mqttSharedWriteRejected, Err: err}
	default:
		return mqttSharedWriteResult{Outcome: mqttSharedWriteAmbiguous, Err: err}
	}
}

// mqttSharedPriorMatches compares a composer's prior with the store: same presence, same
// sequence. A mismatch is classified as a CAS rejection — nothing was sent — so the caller
// takes the same re-read path a refused write takes.
func mqttSharedPriorMatches(status mqttOutboundLoadStatus, seq uint64, priorPresent bool, priorSeq uint64, what string) error {
	switch status {
	case mqttOutboundAbsent:
		if priorPresent {
			return fmt.Errorf("the %s this batch was composed against is no longer there", what)
		}
		return nil
	case mqttOutboundValid:
		if !priorPresent {
			return fmt.Errorf("the %s this batch creates already exists at sequence %d", what, seq)
		}
		if seq != priorSeq {
			return fmt.Errorf("the %s moved from sequence %d to %d since this batch was composed", what, priorSeq, seq)
		}
		return nil
	default:
		return fmt.Errorf("the %s reads %v, which licenses nothing", what, status)
	}
}

// mqttCheckSharedLifecycleBatch enforces every shape's rules against the priors.
func mqttCheckSharedLifecycleBatch(b *mqttSharedBatch) error {
	if b == nil {
		return fmt.Errorf("no batch")
	}
	if b.Epoch == _EMPTY_ {
		return fmt.Errorf("the batch carries no recovery epoch")
	}
	switch b.Shape {
	case mqttSharedShapeJoin:
		if !b.hasGroup || !b.hasLedger {
			return fmt.Errorf("a join carries a group member and a ledger member")
		}
		if err := mqttCheckSharedLedgerStep(b); err != nil {
			return err
		}
		return mqttCheckSharedGroupStep(b)
	case mqttSharedShapeRelease:
		if !b.hasGroup || !b.hasLedger {
			return fmt.Errorf("a release carries a group member and a ledger member")
		}
		if err := mqttCheckSharedLedgerStep(b); err != nil {
			return err
		}
		return mqttCheckSharedGroupStep(b)
	case mqttSharedShapeInstall, mqttSharedShapeSeal, mqttSharedShapeBump, mqttSharedShapeClose:
		if b.hasGroup || !b.hasLedger {
			return fmt.Errorf("a %s carries a ledger member alone", b.Shape)
		}
		return mqttCheckSharedLedgerStep(b)
	case mqttSharedShapeRetireDone:
		if !b.hasGroup || b.hasLedger {
			return fmt.Errorf("a retire-done carries a group member alone")
		}
		return mqttCheckSharedGroupStep(b)
	}
	return fmt.Errorf("shape %d has no rule and is refused", b.Shape)
}

// mqttCheckSharedLedgerStep validates the ledger member as a legal step from its prior.
func mqttCheckSharedLedgerStep(b *mqttSharedBatch) error {
	next := b.LedgerNext
	if next == nil {
		return fmt.Errorf("the ledger member is empty")
	}
	if err := mqttValidateSharedLedger(next, b.ID, b.Epoch, b.Gen); err != nil {
		return fmt.Errorf("the ledger member is unsupported: %w", err)
	}
	prior := b.LedgerPrior
	if prior == nil {
		if b.ledgerExpect != 0 || b.LedgerSeq != 0 {
			return fmt.Errorf("a ledger created over absence must be create-only")
		}
	} else {
		if b.ledgerExpect == 0 || b.ledgerExpect != b.LedgerSeq {
			return fmt.Errorf("a ledger step names the sequence its prior was read at, %d, not %d", b.LedgerSeq, b.ledgerExpect)
		}
		if err := mqttValidateSharedLedger(prior, b.ID, b.Epoch, b.Gen); err != nil {
			return fmt.Errorf("the ledger prior is unsupported: %w", err)
		}
		// THE TENURE NEVER DECREASES, and changes only by an installation.
		if next.Tenure.Seq < prior.Tenure.Seq {
			return fmt.Errorf("the ledger step regresses the tenure from %d to %d", prior.Tenure.Seq, next.Tenure.Seq)
		}
		if next.Tenure != prior.Tenure && b.Shape != mqttSharedShapeInstall {
			return fmt.Errorf("a %s may not change the tenure", b.Shape)
		}
		// A SEAL IS NEVER REOPENED.
		if prior.State == mqttSharedLedgerSealed && next.State != mqttSharedLedgerSealed {
			return fmt.Errorf("a sealed ledger is never reopened")
		}
	}
	switch b.Shape {
	case mqttSharedShapeJoin:
		if b.Tenure.isZero() {
			return fmt.Errorf("a join is composed under a tenure")
		}
		if next.State != mqttSharedLedgerOpen || next.Tenure != b.Tenure {
			return fmt.Errorf("a join's ledger member is open under the composer's tenure")
		}
		if b.Acq.Group == _EMPTY_ || b.Acq.Acq == _EMPTY_ || b.Acq.Inc == 0 {
			return fmt.Errorf("a join names the acquisition it adds")
		}
		// A JOIN NEVER CREATES A LEDGER (F-105 8b): its prior is the ledger an installed tenure
		// created, whatever the composer did.
		if prior == nil {
			return fmt.Errorf("a join composed against an absent ledger: only a tenure installation creates one")
		}
		if prior.State != mqttSharedLedgerOpen {
			return fmt.Errorf("a join composed against a sealed ledger")
		}
		if prior.Tenure != b.Tenure {
			return fmt.Errorf("a join composed under tenure %+v against a ledger held by %+v", b.Tenure, prior.Tenure)
		}
		base := prior.Acqs
		if _, dup := (&mqttSharedLedger{Acqs: base}).lists(b.Acq.Acq); dup {
			return fmt.Errorf("the ledger already lists acquisition %q", b.Acq.Acq)
		}
		baseClosed := prior.Closed
		if !mqttSharedStringsEqual(next.Closed, baseClosed) {
			return fmt.Errorf("a join preserves the ledger's closed list and closes nothing")
		}
		if !mqttSharedAcqsEqual(next.Acqs, append(append([]mqttSharedAcqRef(nil), base...), b.Acq)) {
			return fmt.Errorf("a join's ledger member gains exactly its own acquisition and preserves every other entry")
		}
		bound := b.LedgerMax
		if bound <= 0 {
			bound = mqttSharedLedgerMaxAcqs(mqttDefaultMaxAckPending)
		}
		if len(next.Acqs) > bound {
			return fmt.Errorf("a join would take the ledger to %d acquisitions, above its bound %d", len(next.Acqs), bound)
		}
	case mqttSharedShapeRelease:
		if prior == nil {
			return fmt.Errorf("a release is composed against a standing ledger")
		}
		if next.State != prior.State || next.Tenure != prior.Tenure {
			return fmt.Errorf("a release preserves the ledger's state and tenure")
		}
		want, found := mqttSharedAcqsWithout(prior.Acqs, b.Acq.Acq)
		if !found {
			return fmt.Errorf("a release names acquisition %q, which the ledger does not list", b.Acq.Acq)
		}
		if !mqttSharedAcqsEqual(next.Acqs, want) {
			return fmt.Errorf("a release's ledger member loses exactly its own acquisition and preserves every other entry")
		}
		if entry, _ := prior.lists(b.Acq.Acq); entry != b.Acq {
			return fmt.Errorf("a release names %+v but the ledger lists %+v", b.Acq, entry)
		}
		// THE FENCE'S FIRST STEP PRECEDES L1 (F-105 7e): an open ledger releases only an
		// acquisition it already lists as closed to new dependencies; a sealed one is closed
		// generation-wide. The rest of the fence lives on $MQTT_inflight, where this boundary
		// cannot see it; executeSharedRelease is its one route.
		if !prior.isClosed(b.Acq.Acq) {
			return fmt.Errorf("a release of acquisition %q from an open ledger that has not closed it", b.Acq.Acq)
		}
		if !mqttSharedStringsEqual(next.Closed, mqttSharedStringsWithout(prior.Closed, b.Acq.Acq)) {
			return fmt.Errorf("a release's ledger member drops exactly its own acquisition from the closed list")
		}
	case mqttSharedShapeClose:
		if prior == nil {
			return fmt.Errorf("a close is composed against a standing ledger")
		}
		if prior.State != mqttSharedLedgerOpen {
			return fmt.Errorf("a close over a sealed ledger: a sealed ledger is closed generation-wide")
		}
		if next.State != prior.State || next.Tenure != prior.Tenure || !mqttSharedAcqsEqual(next.Acqs, prior.Acqs) {
			return fmt.Errorf("a close preserves the ledger's state, tenure and acquisitions")
		}
		if entry, listed := prior.lists(b.Acq.Acq); !listed || entry != b.Acq {
			return fmt.Errorf("a close names %+v, which the ledger does not list as such", b.Acq)
		}
		if prior.isClosed(b.Acq.Acq) {
			return fmt.Errorf("acquisition %q is already closed", b.Acq.Acq)
		}
		if !mqttSharedStringsEqual(next.Closed, append(mqttSharedCopyStrings(prior.Closed), b.Acq.Acq)) {
			return fmt.Errorf("a close appends exactly its own acquisition to the closed list")
		}
	case mqttSharedShapeInstall:
		if b.Tenure.isZero() {
			return fmt.Errorf("an installation names the tenure it installs")
		}
		if next.State != mqttSharedLedgerOpen || next.Tenure != b.Tenure {
			return fmt.Errorf("an installation writes an open ledger under the installed tenure")
		}
		if prior == nil {
			if len(next.Acqs) != 0 {
				return fmt.Errorf("an installation over absence carries no acquisitions")
			}
			return nil
		}
		if prior.State != mqttSharedLedgerOpen {
			return fmt.Errorf("an installation over a sealed ledger is refused: the generation is closing")
		}
		if prior.Tenure.Seq >= b.Tenure.Seq {
			return fmt.Errorf("an installation of tenure %d over the standing tenure %d is refused: a tenure never regresses",
				b.Tenure.Seq, prior.Tenure.Seq)
		}
		if !mqttSharedAcqsEqual(next.Acqs, prior.Acqs) || !mqttSharedStringsEqual(next.Closed, prior.Closed) {
			return fmt.Errorf("an installation preserves every acquisition and the closed list")
		}
	case mqttSharedShapeSeal:
		if next.State != mqttSharedLedgerSealed {
			return fmt.Errorf("a seal writes a sealed ledger")
		}
		if prior == nil {
			if len(next.Acqs) != 0 || !next.Tenure.isZero() {
				return fmt.Errorf("a seal over absence carries no acquisitions and no tenure")
			}
			return nil
		}
		if next.Tenure != prior.Tenure || !mqttSharedAcqsEqual(next.Acqs, prior.Acqs) ||
			!mqttSharedStringsEqual(next.Closed, prior.Closed) {
			return fmt.Errorf("a seal preserves the ledger's acquisitions, closed list and tenure, whatever the tenure")
		}
	case mqttSharedShapeBump:
		if prior == nil {
			return fmt.Errorf("a bump is composed against a standing ledger")
		}
		if b.Tenure.isZero() || prior.Tenure != b.Tenure {
			return fmt.Errorf("a bump is composed under the ledger's own tenure")
		}
		if prior.State != mqttSharedLedgerOpen {
			return fmt.Errorf("a bump over a sealed ledger")
		}
		pb, _ := json.Marshal(prior)
		nb, _ := json.Marshal(next)
		if !bytes.Equal(pb, nb) {
			return fmt.Errorf("a bump changes nothing but the sequence")
		}
	}
	return nil
}

// mqttCheckSharedGroupStep validates the group member as a legal step from its prior.
func mqttCheckSharedGroupStep(b *mqttSharedBatch) error {
	next := b.GroupNext
	if next == nil {
		return fmt.Errorf("the group member is empty")
	}
	if err := mqttValidateSharedGroup(next, b.Epoch, b.Qkey); err != nil {
		return fmt.Errorf("the group member is unsupported: %w", err)
	}
	prior := b.GroupPrior
	if prior == nil {
		if b.groupExpect != 0 || b.GroupSeq != 0 {
			return fmt.Errorf("a group record created over absence must be create-only")
		}
	} else {
		if b.groupExpect == 0 || b.groupExpect != b.GroupSeq {
			return fmt.Errorf("a group step names the sequence its prior was read at, %d, not %d", b.GroupSeq, b.groupExpect)
		}
		if err := mqttValidateSharedGroup(prior, b.Epoch, b.Qkey); err != nil {
			return fmt.Errorf("the group prior is unsupported: %w", err)
		}
		if next.Next < prior.Next {
			return fmt.Errorf("the ordinal counter regresses from %d to %d", prior.Next, next.Next)
		}
	}
	switch b.Shape {
	case mqttSharedShapeJoin:
		if b.Acq.Group != b.Qkey {
			return fmt.Errorf("the acquisition names group %q but the batch addresses %q", b.Acq.Group, b.Qkey)
		}
		if prior == nil {
			// The first acquisition of a group: incarnation 1, one holder, counter at 2.
			if next.Live == nil || next.Live.Inc != 1 || next.Live.Holders != 1 || next.Next != 2 || len(next.Retiring) != 0 {
				return fmt.Errorf("a group created by a join mounts incarnation 1 with one holder")
			}
		} else if prior.Live != nil {
			// COUNT DISCIPLINE: +1 holder for the +1 ledger entry, nothing else moves.
			if next.Live == nil || next.Live.Inc != prior.Live.Inc || next.Live.Holders != prior.Live.Holders+1 ||
				next.Next != prior.Next || !mqttSharedUint64sEqual(next.Retiring, prior.Retiring) {
				return fmt.Errorf("a join onto a live incarnation adds exactly one holder and changes nothing else")
			}
		} else {
			// MOUNT: a fresh incarnation with one holder, the counter advanced by one.
			if len(prior.Retiring) >= mqttSharedRetiringMax {
				return fmt.Errorf("mounting a new incarnation over a full retiring list is refused; finish an entry first")
			}
			if next.Live == nil || next.Live.Inc != prior.Next || next.Live.Holders != 1 ||
				next.Next != prior.Next+1 || !mqttSharedUint64sEqual(next.Retiring, prior.Retiring) {
				return fmt.Errorf("a join that mounts takes the next ordinal with one holder and advances the counter by one")
			}
		}
		if next.Live.Inc != b.Acq.Inc {
			return fmt.Errorf("the acquisition holds incarnation %d but the group serves %d", b.Acq.Inc, next.Live.Inc)
		}
	case mqttSharedShapeRelease:
		if prior == nil || prior.Live == nil {
			return fmt.Errorf("a release is composed against a group with a live incarnation")
		}
		if b.Acq.Group != b.Qkey || b.Acq.Inc != prior.Live.Inc {
			return fmt.Errorf("a release of an acquisition on incarnation %d against a group serving %d", b.Acq.Inc, prior.Live.Inc)
		}
		if next.Next != prior.Next {
			return fmt.Errorf("a release does not move the ordinal counter")
		}
		if prior.Live.Holders > 1 {
			if next.Live == nil || next.Live.Inc != prior.Live.Inc || next.Live.Holders != prior.Live.Holders-1 ||
				!mqttSharedUint64sEqual(next.Retiring, prior.Retiring) {
				return fmt.Errorf("a release from a held incarnation removes exactly one holder and changes nothing else")
			}
		} else {
			// THE LAST HOLDER: the incarnation leaves live and enters retiring, in one act.
			if next.Live != nil || !mqttSharedUint64sEqual(next.Retiring, append(append([]uint64(nil), prior.Retiring...), prior.Live.Inc)) {
				return fmt.Errorf("the last release moves the incarnation from live into retiring and nothing else")
			}
		}
	case mqttSharedShapeRetireDone:
		if prior == nil {
			return fmt.Errorf("a retire-done is composed against a standing group record")
		}
		if b.RetiredInc == 0 || !prior.isRetiring(b.RetiredInc) {
			return fmt.Errorf("a retire-done names incarnation %d, which the group does not list as retiring", b.RetiredInc)
		}
		want := make([]uint64, 0, len(prior.Retiring))
		for _, k := range prior.Retiring {
			if k != b.RetiredInc {
				want = append(want, k)
			}
		}
		if !mqttSharedUint64sEqual(next.Retiring, want) ||
			next.Next != prior.Next || !mqttSharedLiveEqual(next.Live, prior.Live) {
			return fmt.Errorf("a retire-done removes exactly one retiring entry and changes nothing else")
		}
	}
	return nil
}

func mqttSharedUint64sEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func mqttSharedLiveEqual(a, b *mqttSharedLive) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// ---------------------------------------------------------------------------
// Composers: each builds one shape from the priors it read
// ---------------------------------------------------------------------------

// mqttComposeSharedJoin builds a join of acq onto group qkey for (id, gen) under tenure. The
// incarnation is taken from the group record it CASes against — the live one, or the next
// ordinal when nobody holds one — so a join never names a retiring or retired incarnation.
// A full retiring list on a mount is refused here, before anything is sent; the caller
// finishes entries first.
func mqttComposeSharedJoin(epoch, id, gen, qkey, acq string, tenure mqttSharedTenure,
	g mqttSharedGroupLoad, l mqttSharedLedgerLoad, ledgerMax int) (*mqttSharedBatch, error) {

	b := &mqttSharedBatch{Shape: mqttSharedShapeJoin, Epoch: epoch, Qkey: qkey, ID: id, Gen: gen,
		Tenure: tenure, hasGroup: true, hasLedger: true, LedgerMax: ledgerMax}
	switch g.status {
	case mqttOutboundAbsent:
		b.GroupNext = &mqttSharedGroupRecord{Epoch: epoch, Group: qkey,
			Live: &mqttSharedLive{Inc: 1, Holders: 1}, Retiring: []uint64{}, Next: 2}
	case mqttOutboundValid:
		b.GroupPrior, b.GroupSeq, b.groupExpect = g.rec, g.seq, g.seq
		next := *g.rec
		next.Retiring = mqttSharedCopyUint64s(g.rec.Retiring)
		if g.rec.Live != nil {
			next.Live = &mqttSharedLive{Inc: g.rec.Live.Inc, Holders: g.rec.Live.Holders + 1}
		} else {
			if len(g.rec.Retiring) >= mqttSharedRetiringMax {
				return nil, errMQTTSharedRetiringFull
			}
			next.Live = &mqttSharedLive{Inc: g.rec.Next, Holders: 1}
			next.Next = g.rec.Next + 1
		}
		b.GroupNext = &next
	default:
		return nil, fmt.Errorf("the group record of %q reads %v, which licenses no join", qkey, g.status)
	}
	b.Acq = mqttSharedAcqRef{Group: qkey, Inc: b.GroupNext.Live.Inc, Acq: acq}
	switch l.status {
	case mqttOutboundAbsent:
		// A JOIN NEVER CREATES A LEDGER (F-105 8b). Every acceptance installs its tenure before it
		// activates, and that installation over absence is the one creator; a join meeting an
		// absent ledger was composed by a connection that never installed — a stale or crashed
		// acceptance — and creating the ledger here would bypass exactly that prerequisite.
		return nil, errMQTTSharedJoinWithoutLedger
	case mqttOutboundValid:
		b.LedgerPrior, b.LedgerSeq, b.ledgerExpect = l.rec, l.seq, l.seq
		next := *l.rec
		next.Acqs = append(mqttSharedCopyAcqs(l.rec.Acqs), b.Acq)
		next.Closed = mqttSharedCopyStrings(l.rec.Closed)
		b.LedgerNext = &next
	default:
		return nil, fmt.Errorf("the ledger of generation %q reads %v, which licenses no join", gen, l.status)
	}
	return b, nil
}

// errMQTTSharedJoinWithoutLedger: a join composed against an absent acquisition ledger. Only an
// acceptance's tenure installation creates a ledger, so the joining connection never installed:
// its filter fails, and nothing is written.
var errMQTTSharedJoinWithoutLedger = fmt.Errorf("the generation has no acquisition ledger: only an installed tenure may join")

// errMQTTSharedRetiringFull is a mount refused by the retiring bound: the caller finishes the
// entries it can and, failing that, fails the filter with a retryable reason.
var errMQTTSharedRetiringFull = fmt.Errorf("the group's retiring list is full; no new incarnation can be mounted until one is finished")

// mqttComposeSharedRelease builds the release of acq. ANY tenure may compose it: its
// authority is the record verdict or the seal the caller holds, and the CAS makes concurrent
// executors converge.
func mqttComposeSharedRelease(epoch, id, gen string, acq mqttSharedAcqRef,
	g mqttSharedGroupLoad, l mqttSharedLedgerLoad) (*mqttSharedBatch, error) {

	if g.status != mqttOutboundValid || l.status != mqttOutboundValid {
		return nil, fmt.Errorf("a release of %q needs both records readable: group %v, ledger %v", acq.Acq, g.status, l.status)
	}
	if g.rec.Live == nil || g.rec.Live.Inc != acq.Inc {
		// The consistency check of the plan's 3.2.1: an entry listed on a non-live incarnation is
		// a contradiction count discipline forbids. Refused with a diagnostic; nothing released.
		return nil, fmt.Errorf("acquisition %q holds incarnation %d of %q but the group serves %v: a listed acquisition "+
			"on a non-live incarnation is an inconsistency this broker refuses to act on", acq.Acq, acq.Inc, acq.Group, g.rec.Live)
	}
	b := &mqttSharedBatch{Shape: mqttSharedShapeRelease, Epoch: epoch, Qkey: acq.Group, ID: id, Gen: gen,
		Acq: acq, hasGroup: true, hasLedger: true,
		GroupPrior: g.rec, GroupSeq: g.seq, groupExpect: g.seq,
		LedgerPrior: l.rec, LedgerSeq: l.seq, ledgerExpect: l.seq}
	gn := *g.rec
	gn.Retiring = mqttSharedCopyUint64s(g.rec.Retiring)
	if g.rec.Live.Holders > 1 {
		gn.Live = &mqttSharedLive{Inc: g.rec.Live.Inc, Holders: g.rec.Live.Holders - 1}
	} else {
		gn.Live = nil
		gn.Retiring = append(gn.Retiring, g.rec.Live.Inc)
	}
	b.GroupNext = &gn
	ln := *l.rec
	acqs, found := mqttSharedAcqsWithout(l.rec.Acqs, acq.Acq)
	if !found {
		return nil, fmt.Errorf("the ledger of generation %q does not list acquisition %q", gen, acq.Acq)
	}
	ln.Acqs = acqs
	ln.Closed = mqttSharedStringsWithout(l.rec.Closed, acq.Acq)
	b.LedgerNext = &ln
	return b, nil
}

// mqttComposeSharedInstall builds a tenure installation (or, over absence, the abort's init
// fence). It returns (nil, nil) when the tenure already stands, and a typed refusal when a
// newer tenure or a seal stands.
func mqttComposeSharedInstall(epoch, id, gen string, tenure mqttSharedTenure, l mqttSharedLedgerLoad) (*mqttSharedBatch, error) {
	b := &mqttSharedBatch{Shape: mqttSharedShapeInstall, Epoch: epoch, ID: id, Gen: gen, Tenure: tenure, hasLedger: true}
	switch l.status {
	case mqttOutboundAbsent:
		b.LedgerNext = &mqttSharedLedger{Epoch: epoch, ID: id, Gen: gen, State: mqttSharedLedgerOpen,
			Tenure: tenure, Acqs: []mqttSharedAcqRef{}, Closed: []string{}}
		return b, nil
	case mqttOutboundValid:
	default:
		return nil, fmt.Errorf("the ledger of generation %q reads %v, which licenses no installation", gen, l.status)
	}
	switch {
	case l.rec.State == mqttSharedLedgerSealed:
		return nil, errMQTTSharedLedgerSealed
	case l.rec.Tenure == tenure:
		return nil, nil
	case l.rec.Tenure.Seq > tenure.Seq:
		return nil, errMQTTSharedTenureSuperseded
	case l.rec.Tenure.Seq == tenure.Seq:
		// Two tenures at one accept sequence cannot exist: the sequence is the record write's.
		return nil, fmt.Errorf("the ledger of generation %q carries tenure %+v at the same sequence as %+v, which is an inconsistency",
			gen, l.rec.Tenure, tenure)
	}
	b.LedgerPrior, b.LedgerSeq, b.ledgerExpect = l.rec, l.seq, l.seq
	next := *l.rec
	next.Tenure = tenure
	next.Acqs = mqttSharedCopyAcqs(l.rec.Acqs)
	next.Closed = mqttSharedCopyStrings(l.rec.Closed)
	b.LedgerNext = &next
	return b, nil
}

var (
	// errMQTTSharedLedgerSealed: the generation is closing. A CONNECT meeting it is refused; a
	// join meeting it fails its filter.
	errMQTTSharedLedgerSealed = fmt.Errorf("the generation's acquisition ledger is sealed: the generation is closing")
	// errMQTTSharedTenureSuperseded: a newer CONNECT of this generation owns acquisition. The
	// superseded CONNECT is refused before its CONNACK; a stale tenure never acquires again.
	errMQTTSharedTenureSuperseded = fmt.Errorf("a newer tenure of this generation holds its acquisition ledger")
)

// mqttComposeSharedSeal builds the closing's seal. Authorized by the teardown claim the caller
// holds, never by tenure; it preserves the acquisitions and whatever tenure stands. It returns
// (nil, nil) when the ledger is already sealed.
func mqttComposeSharedSeal(epoch, id, gen string, l mqttSharedLedgerLoad) (*mqttSharedBatch, error) {
	b := &mqttSharedBatch{Shape: mqttSharedShapeSeal, Epoch: epoch, ID: id, Gen: gen, hasLedger: true}
	switch l.status {
	case mqttOutboundAbsent:
		b.LedgerNext = &mqttSharedLedger{Epoch: epoch, ID: id, Gen: gen, State: mqttSharedLedgerSealed,
			Acqs: []mqttSharedAcqRef{}, Closed: []string{}}
		return b, nil
	case mqttOutboundValid:
	default:
		return nil, fmt.Errorf("the ledger of generation %q reads %v, which licenses no seal", gen, l.status)
	}
	if l.rec.State == mqttSharedLedgerSealed {
		return nil, nil
	}
	b.LedgerPrior, b.LedgerSeq, b.ledgerExpect = l.rec, l.seq, l.seq
	next := *l.rec
	next.State = mqttSharedLedgerSealed
	next.Acqs = mqttSharedCopyAcqs(l.rec.Acqs)
	next.Closed = mqttSharedCopyStrings(l.rec.Closed)
	b.LedgerNext = &next
	return b, nil
}

// mqttComposeSharedBump builds the abort fence over a present ledger: the same body at the
// observed sequence, so every attempt composed against an earlier sequence is excluded.
func mqttComposeSharedBump(epoch, id, gen string, tenure mqttSharedTenure, l mqttSharedLedgerLoad) (*mqttSharedBatch, error) {
	if l.status != mqttOutboundValid {
		return nil, fmt.Errorf("the ledger of generation %q reads %v, which licenses no bump", gen, l.status)
	}
	if l.rec.State != mqttSharedLedgerOpen || l.rec.Tenure != tenure {
		return nil, fmt.Errorf("a bump is composed only over an open ledger under this tenure")
	}
	next := *l.rec
	next.Acqs = mqttSharedCopyAcqs(l.rec.Acqs)
	next.Closed = mqttSharedCopyStrings(l.rec.Closed)
	return &mqttSharedBatch{Shape: mqttSharedShapeBump, Epoch: epoch, ID: id, Gen: gen, Tenure: tenure,
		hasLedger: true, LedgerPrior: l.rec, LedgerSeq: l.seq, ledgerExpect: l.seq, LedgerNext: &next}, nil
}

// mqttComposeSharedClose builds the close of acq over an OPEN ledger: the first step of its release
// (F-105 7e). ANY tenure may compose it, as for a release: its authority is the verdict the caller
// executes. It returns (nil, nil) when the acquisition is already closed or the ledger sealed.
func mqttComposeSharedClose(epoch, id, gen string, acq mqttSharedAcqRef, l mqttSharedLedgerLoad) (*mqttSharedBatch, error) {
	if l.status != mqttOutboundValid {
		return nil, fmt.Errorf("the ledger of generation %q reads %v, which licenses no close", gen, l.status)
	}
	if l.rec.isClosed(acq.Acq) {
		return nil, nil
	}
	if entry, listed := l.rec.lists(acq.Acq); !listed || entry != acq {
		return nil, fmt.Errorf("the ledger of generation %q does not list acquisition %+v", gen, acq)
	}
	next := *l.rec
	next.Acqs = mqttSharedCopyAcqs(l.rec.Acqs)
	next.Closed = append(mqttSharedCopyStrings(l.rec.Closed), acq.Acq)
	return &mqttSharedBatch{Shape: mqttSharedShapeClose, Epoch: epoch, ID: id, Gen: gen, Acq: acq,
		hasLedger: true, LedgerPrior: l.rec, LedgerSeq: l.seq, ledgerExpect: l.seq, LedgerNext: &next}, nil
}

// mqttComposeSharedRetireDone builds the group CAS that removes one retiring entry after its
// consumer's deletion was classified removed or gone. The caller supplies that outcome; this
// only frames it.
func mqttComposeSharedRetireDone(epoch, qkey string, inc uint64, g mqttSharedGroupLoad) (*mqttSharedBatch, error) {
	if g.status != mqttOutboundValid {
		return nil, fmt.Errorf("the group record of %q reads %v, which licenses no retirement completion", qkey, g.status)
	}
	if !g.rec.isRetiring(inc) {
		return nil, fmt.Errorf("incarnation %d of %q is not retiring", inc, qkey)
	}
	next := *g.rec
	next.Retiring = make([]uint64, 0, len(g.rec.Retiring))
	for _, k := range g.rec.Retiring {
		if k != inc {
			next.Retiring = append(next.Retiring, k)
		}
	}
	if g.rec.Live != nil {
		live := *g.rec.Live
		next.Live = &live
	}
	return &mqttSharedBatch{Shape: mqttSharedShapeRetireDone, Epoch: epoch, Qkey: qkey, RetiredInc: inc,
		hasGroup: true, GroupPrior: g.rec, GroupSeq: g.seq, groupExpect: g.seq, GroupNext: &next}, nil
}
