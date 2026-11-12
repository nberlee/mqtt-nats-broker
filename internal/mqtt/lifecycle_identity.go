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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Lifecycle and generation identity (issue #133).
//
// A client's records are reclaimable only if every reader can tell, from one positive read,
// whether the scope a record belongs to has ended. Two ordinals make that arithmetic rather
// than inferred from an absence:
//
//   - a LIFECYCLE L is allocated by the client's registry shard, and is never reissued for
//     that client within the account's recovery epoch;
//   - a GENERATION is (L, GenOrd), GenOrd allocated by the lifecycle's authority in the very
//     transition that installs the generation, and never reissued within L.
//
// So "has this generation ended" is a comparison against one Valid authority (or, once the
// lifecycle itself has retired, against its registry shard), never "is its closure absent" —
// a closure that reclamation has purged reads exactly like one never written.

// mqttGenTokenLifecycleWidth and mqttGenTokenOrdinalWidth split the generation token. Their sum
// is mqttSessGenLen, and both halves are base-62, so a generation token is a well-formed
// generation by mqttValidSessGen and every name already derived from a generation keeps its
// shape.
const (
	mqttGenTokenLifecycleWidth = 10
	mqttGenTokenOrdinalWidth   = mqttSessGenLen - mqttGenTokenLifecycleWidth
)

// mqttMaxLifecycle is the largest lifecycle ordinal the token can carry: 62^10 - 1.
const mqttMaxLifecycle uint64 = 839299365868340223

const mqttBase62Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// mqttFormatBase62 renders v in exactly width base-62 digits, or reports that it does not fit.
func mqttFormatBase62(v uint64, width int) (string, bool) {
	b := make([]byte, width)
	for i := width - 1; i >= 0; i-- {
		b[i] = mqttBase62Alphabet[v%62]
		v /= 62
	}
	return string(b), v == 0
}

func mqttParseBase62(s string) (uint64, bool) {
	var v uint64
	for i := 0; i < len(s); i++ {
		c := s[i]
		var d uint64
		switch {
		case c >= '0' && c <= '9':
			d = uint64(c - '0')
		case c >= 'A' && c <= 'Z':
			d = uint64(c-'A') + 10
		case c >= 'a' && c <= 'z':
			d = uint64(c-'a') + 36
		default:
			return 0, false
		}
		if v > (^uint64(0)-d)/62 {
			return 0, false
		}
		v = v*62 + d
	}
	return v, true
}

// mqttGenToken is THE generation identity of (L, GenOrd). Both ordinals start at 1 for L and
// 0 for GenOrd; a zero lifecycle is no lifecycle and is refused.
func mqttGenToken(l, genOrd uint64) (string, error) {
	if l == 0 || l > mqttMaxLifecycle {
		return _EMPTY_, fmt.Errorf("lifecycle %d is outside the generation token's range", l)
	}
	lt, _ := mqttFormatBase62(l, mqttGenTokenLifecycleWidth)
	ot, ok := mqttFormatBase62(genOrd, mqttGenTokenOrdinalWidth)
	if !ok {
		return _EMPTY_, fmt.Errorf("generation ordinal %d is outside the generation token's range", genOrd)
	}
	return lt + ot, nil
}

// mqttParseGenToken returns the (L, GenOrd) a generation token names. ok is false for anything
// that is not a token mqttGenToken produced, so a reader never classifies a malformed name.
func mqttParseGenToken(gen string) (l, genOrd uint64, ok bool) {
	if !mqttValidSessGen(gen) {
		return 0, 0, false
	}
	l, lok := mqttParseBase62(gen[:mqttGenTokenLifecycleWidth])
	genOrd, gok := mqttParseBase62(gen[mqttGenTokenLifecycleWidth:])
	if !lok || !gok || l == 0 || l > mqttMaxLifecycle {
		return 0, 0, false
	}
	return l, genOrd, true
}

// mqttLifecycleScopePrefix opens a LIFECYCLE outcome scope, L.<L>. One character, so a
// lifecycle scope can never be read as a generation token (fixed width mqttSessGenLen): a
// generation's purge filter ob.*.<idHash>.<g>.> never reaches a lifecycle-scoped outcome.
const mqttLifecycleScopePrefix = "L"

// mqttLifecycleScope is the outcome scope of a transition that changes lifecycle-level state:
// the lifecycle token L.<L>, two subject tokens.
func mqttLifecycleScope(l uint64) (string, error) {
	if l == 0 || l > mqttMaxLifecycle {
		return _EMPTY_, fmt.Errorf("lifecycle %d is outside the outcome scope's range", l)
	}
	lt, _ := mqttFormatBase62(l, mqttGenTokenLifecycleWidth)
	return mqttLifecycleScopePrefix + "." + lt, nil
}

// mqttParseOutcomeScope returns what an outcome scope names: the lifecycle it belongs to,
// and the generation when it is a generation scope (gen empty for a lifecycle scope). ok is
// false for anything neither constructor produced.
func mqttParseOutcomeScope(scope string) (l uint64, gen string, ok bool) {
	if lt, found := strings.CutPrefix(scope, mqttLifecycleScopePrefix+"."); found {
		if len(lt) != mqttGenTokenLifecycleWidth {
			return 0, _EMPTY_, false
		}
		l, lok := mqttParseBase62(lt)
		if !lok || l == 0 || l > mqttMaxLifecycle {
			return 0, _EMPTY_, false
		}
		return l, _EMPTY_, true
	}
	l, _, gok := mqttParseGenToken(scope)
	if !gok {
		return 0, _EMPTY_, false
	}
	return l, scope, true
}

// ---------------------------------------------------------------------------
// The lifecycle registry
// ---------------------------------------------------------------------------
//
// One record per shard, at a fixed subject, rewritten by compare-and-swap. It allocates
// lifecycles (Next) and holds the client's lifecycle membership EXPLICITLY: Active names the
// lifecycle a client's authority belongs to, Retiring the one being reclaimed. Membership is
// explicit because the alternative — "a lifecycle below Next whose authority is absent is
// retired" — is a two-record predicate an absent authority can satisfy for the wrong reason.

const mqttLifecycleRegistryClassTk = "lreg"

// mqttLifecycleRegistryShards is fixed at stream creation (format 1). Sized like the
// recovery directory: the count bounds independent positions, not the population.
const mqttLifecycleRegistryShards = 64

// The registry shard's capacity (plan section 9). Every entry, active or retiring, is budgeted
// at the size of a retiring entry at its widest ordinals, so moving a client from active to
// retiring never grows the body; creation is the only step that adds an entry, and it is
// refused once the shard holds mqttLifecycleRegistryShardMaxEntries.
const (
	mqttLifecycleRegistryShardBudget = 512 * 1024
	// mqttLifecycleRegistryEntryBudget is one entry's budgeted size: a quoted client hash key
	// and a retiring entry with every ordinal at its widest, with the separator, rounded up.
	mqttLifecycleRegistryEntryBudget     = 96
	mqttLifecycleRegistryShardMaxEntries = mqttLifecycleRegistryShardBudget / mqttLifecycleRegistryEntryBudget
)

// errMQTTRegistryShardFull: the client's registry shard holds as many lifecycles as it may, so
// a new client cannot be created until one retires.
var errMQTTRegistryShardFull = errors.New("the lifecycle registry shard is full")

// mqttRegistryShardHasRoom reports whether the shard can take one more lifecycle.
func mqttRegistryShardHasRoom(r *mqttLifecycleRegistryShard) bool {
	return r == nil || len(r.Active)+len(r.Retiring) < mqttLifecycleRegistryShardMaxEntries
}

// mqttLifecycleRegistryShardFor is the shard a client's lifecycles are allocated in, by the
// same client hashing the recovery directory uses.
func mqttLifecycleRegistryShardFor(idHash string) uint64 { return mqttDirShardFor(idHash) }

func mqttLifecycleRegistrySubject(shard uint64) string {
	return mqttOutboundRecordSubjectPrefix() + mqttLifecycleRegistryClassTk + "." + strconv.FormatUint(shard, 10)
}

// mqttLifecycleRetirePhase is a retiring lifecycle's durable progress (plan 6.4). Each phase is
// written once, in order, so a crashed reclaimer is finished from the phase alone.
type mqttLifecycleRetirePhase uint8

const (
	// mqttLifecycleFenced: the authority's last write set it retiring; nothing writes for the
	// client from here but the registry.
	mqttLifecycleFenced mqttLifecycleRetirePhase = 1
	// mqttLifecycleBounded: Bound is the stream's last sequence read after the fence, so every
	// record of the lifecycle lies at or below it.
	mqttLifecycleBounded mqttLifecycleRetirePhase = 2
)

// mqttRetiringLifecycle is one client's lifecycle under reclamation.
type mqttRetiringLifecycle struct {
	L     uint64                   `json:"l"`
	Phase mqttLifecycleRetirePhase `json:"phase"`
	// Bound is set from the bounded phase on and immutable after: a stale reclaimer purges
	// only below it, which precedes every successor write.
	Bound uint64 `json:"bound,omitempty"`
}

// mqttLifecycleRegistryShard is one shard's body.
type mqttLifecycleRegistryShard struct {
	Epoch string `json:"epoch"`
	Shard uint64 `json:"shard"`
	// Next is the next lifecycle this shard allocates. Monotone; a lifecycle below it was
	// allocated, and is active, retiring, or RETIRED by membership, never by inference.
	Next     uint64                           `json:"next"`
	Active   map[string]uint64                `json:"active"`
	Retiring map[string]mqttRetiringLifecycle `json:"retiring"`
}

// mqttLifecycleStatus is what a registry shard says about one lifecycle of one client.
type mqttLifecycleStatus uint8

const (
	// mqttLifecycleUnallocated: L >= Next; no record of this lifecycle can exist.
	mqttLifecycleUnallocated mqttLifecycleStatus = iota + 1
	// mqttLifecycleActive: the client's current lifecycle.
	mqttLifecycleActive
	// mqttLifecycleRetiringStatus: under reclamation; a reader waits.
	mqttLifecycleRetiringStatus
	// mqttLifecycleRetired: allocated, and neither active nor retiring. Its records are gone or
	// going, and no publication for it can land.
	mqttLifecycleRetired
)

// classify is THE retirement predicate of a lifecycle: one Valid shard read, membership
// explicit.
func (r *mqttLifecycleRegistryShard) classify(idHash string, l uint64) mqttLifecycleStatus {
	switch {
	case l == 0 || l >= r.Next:
		return mqttLifecycleUnallocated
	case r.Active[idHash] == l:
		return mqttLifecycleActive
	case r.Retiring[idHash].L == l:
		return mqttLifecycleRetiringStatus
	default:
		return mqttLifecycleRetired
	}
}

var mqttLifecycleRegistryMembers = map[string]struct{}{
	"epoch": {}, "shard": {}, "next": {}, "active": {}, "retiring": {},
}

var mqttRetiringLifecycleMembers = map[string]struct{}{"l": {}, "phase": {}, "bound": {}}

var mqttRetiringLifecycleRequired = map[string]struct{}{"l": {}, "phase": {}}

// mqttValidateLifecycleRegistryShard refuses a body this broker does not write.
func mqttValidateLifecycleRegistryShard(r *mqttLifecycleRegistryShard, wantEpoch string, wantShard uint64) error {
	if r == nil {
		return errors.New("lifecycle registry shard is absent")
	}
	if r.Epoch != wantEpoch {
		return fmt.Errorf("lifecycle registry shard carries recovery epoch %q, not %q", r.Epoch, wantEpoch)
	}
	if r.Shard != wantShard || r.Shard >= mqttLifecycleRegistryShards {
		return fmt.Errorf("lifecycle registry shard names shard %d, not %d", r.Shard, wantShard)
	}
	if r.Next == 0 || r.Next > mqttMaxLifecycle+1 {
		return fmt.Errorf("lifecycle registry shard carries an allocator %d outside the token range", r.Next)
	}
	for h, l := range r.Active {
		if !mqttValidIDHashToken(h) || mqttLifecycleRegistryShardFor(h) != r.Shard {
			return fmt.Errorf("lifecycle registry shard %d lists client hash %q it does not own", r.Shard, h)
		}
		if l == 0 || l >= r.Next {
			return fmt.Errorf("lifecycle registry shard lists active lifecycle %d of %q outside its allocator", l, h)
		}
	}
	for h, e := range r.Retiring {
		if !mqttValidIDHashToken(h) || mqttLifecycleRegistryShardFor(h) != r.Shard {
			return fmt.Errorf("lifecycle registry shard %d lists retiring client hash %q it does not own", r.Shard, h)
		}
		if e.L == 0 || e.L >= r.Next {
			return fmt.Errorf("lifecycle registry shard lists retiring lifecycle %d of %q outside its allocator", e.L, h)
		}
		if a, ok := r.Active[h]; ok && a == e.L {
			return fmt.Errorf("lifecycle %d of %q is both active and retiring", e.L, h)
		}
		switch e.Phase {
		case mqttLifecycleFenced:
			if e.Bound != 0 {
				return fmt.Errorf("a fenced lifecycle %d of %q carries a bound", e.L, h)
			}
		case mqttLifecycleBounded:
			if e.Bound == 0 {
				return fmt.Errorf("a bounded lifecycle %d of %q carries no bound", e.L, h)
			}
		default:
			return fmt.Errorf("retiring lifecycle %d of %q carries an unsupported phase %d", e.L, h, e.Phase)
		}
	}
	return nil
}

// mqttDecodeLifecycleRegistryShard is the closed-schema decode of a shard body.
func mqttDecodeLifecycleRegistryShard(data []byte, wantEpoch string, wantShard uint64) (*mqttLifecycleRegistryShard, error) {
	if err := mqttJSONMembersAreExactAndUnique(data, mqttLifecycleRegistryMembers, "a lifecycle registry shard"); err != nil {
		return nil, err
	}
	if err := mqttJSONMembersArePresent(data, mqttLifecycleRegistryMembers, "a lifecycle registry shard"); err != nil {
		return nil, err
	}
	raw := struct {
		Retiring map[string]json.RawMessage `json:"retiring"`
	}{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	for h, e := range raw.Retiring {
		noun := fmt.Sprintf("the retiring lifecycle of %q", h)
		if err := mqttJSONMembersAreExactAndUnique(e, mqttRetiringLifecycleMembers, noun); err != nil {
			return nil, err
		}
		if err := mqttJSONMembersArePresent(e, mqttRetiringLifecycleRequired, noun); err != nil {
			return nil, err
		}
	}
	r := &mqttLifecycleRegistryShard{}
	if err := json.Unmarshal(data, r); err != nil {
		return nil, err
	}
	if r.Active == nil || r.Retiring == nil {
		return nil, mqttShapeErr("a lifecycle registry shard carries a null membership map")
	}
	if err := mqttValidateLifecycleRegistryShard(r, wantEpoch, wantShard); err != nil {
		return nil, err
	}
	return r, nil
}

// mqttNewLifecycleRegistryShard is the body of a shard nothing has allocated from yet. It is
// written create-only by the first creation that needs it; its absence means exactly this
// body, because the shard is the only allocator of its lifecycles.
func mqttNewLifecycleRegistryShard(epoch string, shard uint64) *mqttLifecycleRegistryShard {
	return &mqttLifecycleRegistryShard{
		Epoch: epoch, Shard: shard, Next: 1,
		Active: map[string]uint64{}, Retiring: map[string]mqttRetiringLifecycle{},
	}
}

// mqttRegistryLoad is one read of a shard.
type mqttRegistryLoad struct {
	rec    *mqttLifecycleRegistryShard
	seq    uint64
	status mqttOutboundLoadStatus
	err    error
}

// loadLifecycleRegistryShard reads one shard. An absent shard is reported Absent with the
// empty body, so a creation composes its create-only member from it; every other reader
// treats Absent as "nothing was ever allocated here".
func (jsa *mqttJSA) loadLifecycleRegistryShard(epoch string, shard uint64) mqttRegistryLoad {
	smsg, err := jsa.loadLastMsgFor(mqttSessStreamName, mqttLifecycleRegistrySubject(shard))
	if err != nil {
		if IsNatsErr(err, JSNoMessageFoundErr) {
			return mqttRegistryLoad{rec: mqttNewLifecycleRegistryShard(epoch, shard), status: mqttOutboundAbsent}
		}
		return mqttRegistryLoad{status: mqttOutboundIndeterminate, err: err}
	}
	r, derr := mqttDecodeLifecycleRegistryShard(smsg.Data, epoch, shard)
	if derr != nil {
		return mqttRegistryLoad{status: mqttOutboundCorrupt, err: derr, seq: smsg.Sequence}
	}
	return mqttRegistryLoad{rec: r, seq: smsg.Sequence, status: mqttOutboundValid}
}
