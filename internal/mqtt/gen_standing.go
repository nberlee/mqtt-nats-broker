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

import "encoding/json"

// Where a generation stands (issue #133, plan section 7): the ONE classification every former
// reader of the retirement watermark uses.
//
// The watermark was a single slot per client, so it lost every earlier generation's identity
// once a later teardown replaced it, and its readers compared stream sequences to recover what
// it lost. The authority carries the answer outright: a generation is current only while the
// authority SERVES it (Gen alone is not enough, since a closed authority keeps its last
// generation's name, consult 10); a noncurrent one is outstanding until it retires; and every
// generation a lifecycle allocated below its allocator that is neither is retired, because no
// transition reinstates a discarded generation. A generation of an earlier lifecycle is retired
// exactly when the registry says that lifecycle is.

// mqttGenStanding is a generation's standing.
type mqttGenStanding uint8

const (
	// mqttGenUnresolved: the evidence could not be read, or does not settle the question.
	// Never a licence and never a permission.
	mqttGenUnresolved mqttGenStanding = iota
	// mqttGenCurrent: the authority serves this generation.
	mqttGenCurrent
	// mqttGenOutstanding: noncurrent and not yet retired; its teardown entry says how it ended.
	mqttGenOutstanding
	// mqttGenRetired: noncurrent, and its records are reclaimed or reclaimable.
	mqttGenRetired
)

func (s mqttGenStanding) String() string {
	switch s {
	case mqttGenCurrent:
		return "current"
	case mqttGenOutstanding:
		return "outstanding"
	case mqttGenRetired:
		return "retired"
	}
	return "unresolved"
}

// mqttOutstandingIndex is the position of generation ordinal ord in life's Outstanding list, or
// -1 when it is not outstanding.
func mqttOutstandingIndex(life *mqttLifecycleState, ord uint64) int {
	if life == nil {
		return -1
	}
	for i := range life.Outstanding {
		if life.Outstanding[i].Ord == ord {
			return i
		}
	}
	return -1
}

// mqttGenStandingIn classifies gen against one authority read. needRegistry reports that the
// authority does not carry gen's lifecycle, so only the registry can say whether it retired.
func mqttGenStandingIn(al mqttAuthorityLoad, gen string) (s mqttGenStanding, entry *mqttOutstandingGen, needRegistry bool) {
	l, ord, ok := mqttParseGenToken(gen)
	if !ok {
		return mqttGenUnresolved, nil, false
	}
	switch {
	case al.status == mqttOutboundAbsent:
		return mqttGenUnresolved, nil, true
	case al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil:
		return mqttGenUnresolved, nil, false
	}
	life := al.rec.Life
	switch {
	case life.L > l:
		return mqttGenUnresolved, nil, true
	case life.L < l:
		// A generation of a lifecycle the authority has not reached: no writer produces it.
		return mqttGenUnresolved, nil, false
	}
	if al.rec.State == mqttAuthorityServing && al.rec.Gen == gen {
		return mqttGenCurrent, nil, false
	}
	if i := mqttOutstandingIndex(life, ord); i >= 0 {
		o := life.Outstanding[i]
		return mqttGenOutstanding, &o, false
	}
	if ord < life.NextGenOrd {
		return mqttGenRetired, nil, false
	}
	return mqttGenUnresolved, nil, false
}

// genStandingFromLoad finishes the classification of gen for the client of idHash from an
// authority read, consulting the registry when the authority does not carry gen's lifecycle.
func (as *mqttAccountSessionManager) genStandingFromLoad(idHash, gen string, al mqttAuthorityLoad) (mqttGenStanding, *mqttOutstandingGen) {
	return as.jsa.genStandingFromLoad(as.sessStreamEpoch, idHash, gen, al)
}

// genStandingFromLoad is the classification for a reader that holds only the store view.
func (jsa *mqttJSA) genStandingFromLoad(epoch, idHash, gen string, al mqttAuthorityLoad) (mqttGenStanding, *mqttOutstandingGen) {
	s, entry, needRegistry := mqttGenStandingIn(al, gen)
	if !needRegistry {
		return s, entry
	}
	l, _, _ := mqttParseGenToken(gen)
	if jsa.lifecycleRetired(epoch, idHash, l) {
		return mqttGenRetired, nil
	}
	return mqttGenUnresolved, nil
}

// lifecycleRetired reports whether the client's registry shard positively says lifecycle l is
// RETIRED. Anything it cannot read says nothing.
func (jsa *mqttJSA) lifecycleRetired(epoch, idHash string, l uint64) bool {
	rl := jsa.loadLifecycleRegistryShard(epoch, mqttLifecycleRegistryShardFor(idHash))
	if rl.status != mqttOutboundValid && rl.status != mqttOutboundAbsent {
		return false
	}
	return rl.rec.classify(idHash, l) == mqttLifecycleRetired
}

// outcomeScopeRetired reports whether an attempt's outcome scope is positively RETIRED: its
// generation retired under the authority that carries its lifecycle, or its lifecycle retired in
// the registry. A live, fenced, unreadable or unknown scope is not.
func (jsa *mqttJSA) outcomeScopeRetired(a *mqttTransitionAttempt) bool {
	l, gen, ok := mqttParseOutcomeScope(a.Scope)
	if !ok {
		return false
	}
	idHash := getHash(a.ID)
	al := jsa.loadOutboundAuthority(a.ID, a.Epoch)
	if gen != _EMPTY_ {
		s, _ := jsa.genStandingFromLoad(a.Epoch, idHash, gen, al)
		return s == mqttGenRetired
	}
	switch {
	case al.status == mqttOutboundValid && al.rec != nil && al.rec.Life != nil && al.rec.Life.L == l:
		return false
	case al.status != mqttOutboundValid && al.status != mqttOutboundAbsent:
		return false
	}
	return jsa.lifecycleRetired(a.Epoch, idHash, l)
}

// genStanding classifies one generation of one client.
func (as *mqttAccountSessionManager) genStanding(clientID, gen string) (mqttGenStanding, *mqttOutstandingGen) {
	return as.genStandingFromLoad(getHash(clientID), gen, as.jsa.loadOutboundAuthority(clientID, as.sessStreamEpoch))
}

// genStandingByHash is genStanding for a reader that holds only the client-ID hash, the orphan
// sweep: the authority's own client ID is taken as the claimed identity, required to hash back
// to the subject's token, and the body then decoded with exactly loadOutboundAuthority's
// validation. A body that names another client is Corrupt, never absent.
func (as *mqttAccountSessionManager) genStandingByHash(idHash, gen string) (mqttGenStanding, *mqttOutstandingGen) {
	al := mqttAuthorityLoad{status: mqttOutboundAbsent}
	smsg, err := as.jsa.loadLastMsgFor(mqttSessStreamName, mqttOutboundAuthoritySubject(idHash))
	switch {
	case err != nil && isErrorOtherThan(err, JSNoMessageFoundErr):
		al.status = mqttOutboundIndeterminate
	case err == nil:
		var claimed struct {
			ID string `json:"id"`
		}
		al = mqttAuthorityLoad{seq: smsg.Sequence, status: mqttOutboundCorrupt}
		if uerr := json.Unmarshal(smsg.Data, &claimed); uerr == nil && claimed.ID != _EMPTY_ && getHash(claimed.ID) == idHash {
			if a, derr := mqttDecodeOutboundAuthority(smsg.Data, claimed.ID, as.sessStreamEpoch); derr == nil {
				al = mqttAuthorityLoad{rec: a, seq: smsg.Sequence, status: mqttOutboundValid}
			}
		}
	}
	return as.genStandingFromLoad(idHash, gen, al)
}
