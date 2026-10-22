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

// The admission readiness of an ordinary QoS 1/2 PUBLISH's dependencies, read in two rounds.

package mqtt

import (
	"fmt"
)

// mqttReadinessDep is one admission dependency's readiness evidence: the consumer it names and
// what the rounds read about it.
//
// Every read here is a pre-batch SNAPSHOT. The write boundary re-checks the ownership (ownedLive)
// and fences the resource by its CAS, and the stream and consumer reads are snapshots by their own
// nature, so the order the reads are ISSUED in carries no decision. The order they are JUDGED in
// does: it decides which refusal an admission reports, and it is the serial order, whatever order
// the replies arrived in.
type mqttReadinessDep struct {
	owned      mqttOwnedStream
	streamName string
	durName    string

	// Round 1, keyed by the durable name and its stream alone.
	own    *mqttNameOwnership
	ownErr error
	si     *StreamInfo
	siErr  error
	ci     *ConsumerInfo
	ciErr  error

	// Round 2, keyed by the owning incarnation round 1 found.
	resRead bool
	res     *mqttOutboundResource
	resErr  error
}

// assembleAdmitDeps resolves and READINESS-gates the resource dependencies an ordinary QoS 1/2
// PUBLISH admission takes: the source consumer in the source-replay role, and for QoS 2 the
// canonical PUBREL consumer in the canonical-pubrel role. Both are admitted in the same
// admission for a QoS 2 PUBLISH. These two, with retained-material for a retained payload, are
// the only roles an edge carries; no later act adds a role to this obligation.
func (sess *mqttSession) assembleAdmitDeps(c *client, jsDur string, qos byte, epoch, gen string) ([]mqttAdmitDependency, bool) {
	deps := []*mqttReadinessDep{{owned: mqttOwnedStreamMsgs, streamName: mqttStreamName, durName: jsDur}}
	roles := []mqttResourceRole{mqttRoleSourceReplay}
	if qos >= 2 {
		_, _, pubRelDur := mqttPubRelNames(sess.idHash, gen)
		deps = append(deps, &mqttReadinessDep{owned: mqttOwnedStreamOut, streamName: mqttOutStreamName, durName: pubRelDur})
		roles = append(roles, mqttRoleCanonicalPubRel)
	}
	incs, ok := sess.readyDependencies(c, epoch, gen, deps)
	if !ok {
		return nil, false
	}
	out := make([]mqttAdmitDependency, len(incs))
	for i, inc := range incs {
		out[i] = mqttAdmitDependency{Inc: inc, Role: roles[i]}
	}
	return out, true
}

// readyDependencyInc resolves one consumer name's owning resource incarnation and proves it READY
// for use: readyDependencies for a single dependency.
func (sess *mqttSession) readyDependencyInc(c *client, owned mqttOwnedStream, streamName, durName, epoch, gen string) (string, bool) {
	incs, ok := sess.readyDependencies(c, epoch, gen,
		[]*mqttReadinessDep{{owned: owned, streamName: streamName, durName: durName}})
	if !ok {
		return _EMPTY_, false
	}
	return incs[0], true
}

// readyDependencies resolves each consumer name's owning resource incarnation from its
// name-ownership record — the delivery callback holds the durable NAME, not the incarnation — and
// proves the resource READY for use before it is admitted as a dependency: the durable half (the
// resource stands LIVE for a logical-use kind, its name-ownership stands LIVE and names this exact
// incarnation, and the stream carries no inactivity timer that could reap a depended-on consumer)
// PLUS a live check that the physical consumer exists with no effective inactivity threshold. Any
// gap REFUSES the admission — never a fallback (Codex consult 11); AckWait re-offers, and once the
// consumer becomes usable the admission lands.
//
// TWO ROUNDS, not five reads per dependency. Round 1 reads every dependency's ownership, stream and
// consumer together; round 2 reads the resource of every dependency whose ownership record was read,
// keyed by the incarnation it names. The ownership is read ONCE: the record round 1 read is the one
// judged, with the equalities a second read used to supply implicitly (the resource's name and
// stream are the durable's, the record names this client) checked explicitly. A LIVE name's owner
// cannot change (mqttNameOwnershipTransitionLegal), so a later read could only newly see it RETIRED,
// and the write boundary refuses exactly that.
//
// THE COST OF A ROUND falls on refusals: a dependency refused at its ownership has still had its
// stream and consumer read, and for QoS 2 the other dependency has still been read. Every refusal
// stays within the reads the admitting path issues (docs/ARCHITECTURE.md, "Admission readiness in
// two rounds"), and a refusal under a persistent fault repeats on each AckWait re-offer.
func (sess *mqttSession) readyDependencies(c *client, epoch, gen string, deps []*mqttReadinessDep) ([]string, bool) {
	sess.jsa.readReadinessRounds(sess.id, epoch, gen, deps)
	incs, err := mqttJudgeReadinessAll(sess.id, gen, deps)
	if err != nil {
		c.Warnf("MQTT: %v", err)
		return nil, false
	}
	return incs, true
}

// mqttJudgeReadinessAll judges the dependencies in order, each completely before the next, so a
// refusal of the first is reported whatever the second's reads found.
func mqttJudgeReadinessAll(id, gen string, deps []*mqttReadinessDep) ([]string, error) {
	incs := make([]string, len(deps))
	for i, d := range deps {
		inc, err := mqttJudgeReadiness(id, gen, d)
		if err != nil {
			return nil, err
		}
		incs[i] = inc
	}
	return incs, nil
}

// readReadinessRounds issues the two rounds and records each read on its dependency. A round that
// failed or timed out leaves every unanswered position's error set, so it is judged a failed read
// at its turn: never an absence, never readiness.
func (jsa *mqttJSA) readReadinessRounds(id, epoch, gen string, deps []*mqttReadinessDep) {
	round := make([]mqttJSARoundRequest, 0, 3*len(deps))
	for _, d := range deps {
		round = append(round,
			mqttLastForRead(mqttInflightStreamName, mqttNameOwnershipSubject(d.owned, d.durName)),
			mqttJSARoundRequest{kind: mqttJSAStreamLookup, subject: fmt.Sprintf(JSApiStreamInfoT, d.streamName)},
			mqttJSARoundRequest{kind: mqttJSAConsumerLookup, subject: fmt.Sprintf(JSApiConsumerInfoT, d.streamName, d.durName)})
	}
	resps, rerr := jsa.requestRound(round)
	for i, d := range deps {
		smsg, oerr := mqttRoundMsgGet(resps, 3*i, rerr)
		d.own, _, d.ownErr = mqttDecodeNameOwnershipPhysical(smsg, oerr, epoch, d.owned, d.durName)
		d.si, d.siErr = mqttRoundStreamInfo(resps, 3*i+1, rerr)
		d.ci, d.ciErr = mqttRoundConsumerInfo(resps, 3*i+2, rerr)
	}

	second := mqttReadinessSecondRound(deps)
	if len(second) == 0 {
		return
	}
	round = nil
	idHash := getHash(id)
	for _, d := range second {
		round = append(round, mqttLastForRead(mqttInflightStreamName, mqttResourceSubject(idHash, gen, d.own.OwnerInc)))
	}
	resps, rerr = jsa.requestRound(round)
	for i, d := range second {
		smsg, err := mqttRoundMsgGet(resps, i, rerr)
		d.resRead = true
		d.res, _, d.resErr = mqttDecodeOutboundResourceRead(smsg, err, id, epoch, gen, d.own.OwnerInc)
	}
}

// mqttReadinessSecondRound names the dependencies whose resource round 2 reads: every one whose
// ownership record was READ, whatever else round 1 found for it or for the other dependency. The
// resource is judged before the ownership's state, the stream and the consumer, so a resource
// refusal must be reachable wherever an ownership record exists; without a record there is no
// incarnation to read.
func mqttReadinessSecondRound(deps []*mqttReadinessDep) []*mqttReadinessDep {
	var second []*mqttReadinessDep
	for _, d := range deps {
		if d.ownErr == nil && d.own != nil {
			second = append(second, d)
		}
	}
	return second
}

// mqttRoundResponse is position i of a round, or the round's error when that position went
// unanswered (a sparse timeout result, or a round that failed before anything was sent).
func mqttRoundResponse(resps []*mqttJSAResponse, i int, rerr error) (any, error) {
	if i < len(resps) && resps[i] != nil {
		return resps[i].value, nil
	}
	if rerr == nil {
		rerr = fmt.Errorf("no response")
	}
	return nil, rerr
}

// mqttRoundMsgGet reads position i as loadLastMsgFor would have returned it.
func mqttRoundMsgGet(resps []*mqttJSAResponse, i int, rerr error) (*StoredMsg, error) {
	v, err := mqttRoundResponse(resps, i, rerr)
	if err != nil {
		return nil, err
	}
	r, _ := v.(*JSApiMsgGetResponse)
	if r == nil {
		return nil, fmt.Errorf("unexpected response type %T", v)
	}
	return r.Message, r.ToError()
}

// mqttRoundStreamInfo reads position i as lookupStream would have returned it.
func mqttRoundStreamInfo(resps []*mqttJSAResponse, i int, rerr error) (*StreamInfo, error) {
	v, err := mqttRoundResponse(resps, i, rerr)
	if err != nil {
		return nil, err
	}
	r, _ := v.(*JSApiStreamInfoResponse)
	if r == nil {
		return nil, fmt.Errorf("unexpected response type %T", v)
	}
	return r.StreamInfo, r.ToError()
}

// mqttRoundConsumerInfo reads position i as lookupConsumer would have returned it.
func mqttRoundConsumerInfo(resps []*mqttJSAResponse, i int, rerr error) (*ConsumerInfo, error) {
	v, err := mqttRoundResponse(resps, i, rerr)
	if err != nil {
		return nil, err
	}
	r, _ := v.(*JSApiConsumerInfoResponse)
	if r == nil {
		return nil, fmt.Errorf("unexpected response type %T", v)
	}
	if err := r.ToError(); err != nil {
		return nil, err
	}
	return r.ConsumerInfo, nil
}

// mqttJudgeReadiness judges one dependency's gathered reads in the serial order, so the refusal it
// reports is the one the five serial reads reported: the ownership read, then the resource, then
// the ownership's state and owner, then the stream, then the consumer. It returns the owning
// incarnation when every check passes, else the refusal, worded for the admission's warning.
func mqttJudgeReadiness(id, gen string, d *mqttReadinessDep) (string, error) {
	if d.ownErr != nil {
		return _EMPTY_, fmt.Errorf("refusing to admit against %q on %q for client %q: its name ownership could not be read: %v",
			d.durName, d.streamName, id, d.ownErr)
	}
	if d.own == nil {
		return _EMPTY_, fmt.Errorf("refusing to admit against %q on %q for client %q: no name-ownership record stands",
			d.durName, d.streamName, id)
	}
	inc := d.own.OwnerInc
	refuse := func(err error) (string, error) {
		return _EMPTY_, fmt.Errorf("refusing to admit against %q/%q on %q for client %q: %v", gen, inc, d.streamName, id, err)
	}
	if !d.resRead {
		return refuse(fmt.Errorf("readiness for use of %q/%q: the resource was not read", gen, inc))
	}
	stream, err := mqttJudgeReadyResource(gen, inc, d.res, d.resErr)
	if err != nil {
		return refuse(err)
	}
	// The equalities the second ownership read supplied by construction, now checked: the resource
	// this name's owner names is a consumer of THIS name on THIS stream, and the record is this
	// client's (the physical read validates the body, not the client).
	if stream != d.owned || d.res.Name != d.durName {
		return refuse(fmt.Errorf("readiness for use of %q/%q: the resource names consumer %q on %q, not %q on %q",
			gen, inc, d.res.Name, stream, d.durName, d.owned))
	}
	var oerr error
	if d.own.ID != id {
		oerr = fmt.Errorf("the ownership of consumer name %q on %q names client %q, not %q",
			d.durName, d.owned, d.own.ID, id)
	}
	if err := mqttJudgeReadyOwnership(gen, inc, d.durName, d.own, oerr); err != nil {
		return refuse(err)
	}
	// The stream whose limit was read is the owned stream's: a caller naming another would have
	// the limit of the wrong stream judged.
	if streamName, serr := mqttStreamForOwned(d.owned); serr != nil || streamName != d.streamName {
		return refuse(fmt.Errorf("readiness for use of %q/%q: the owned stream %q is not %q (%v)",
			gen, inc, d.owned, d.streamName, serr))
	}
	if err := mqttJudgeReadyStream(gen, inc, d.streamName, d.si, d.siErr); err != nil {
		return refuse(err)
	}
	if d.ciErr != nil || d.ci == nil || d.ci.Config == nil {
		return _EMPTY_, fmt.Errorf("refusing to admit against %q on %q for client %q: its physical consumer is not usable: %v",
			d.durName, d.streamName, id, d.ciErr)
	}
	// THE EFFECTIVE CONFIGURATION, as the server reports it (F-105 7c-lc). The broker creates these
	// consumers with no inactivity threshold, but what it requested is not what stands: an
	// administrator's update, or a stream default substituted for zero, arms a timer that would
	// reap a consumer an admitted obligation depends on. The stream half is checked above; this is
	// the consumer's own. A snapshot, not a fence — the supported-configuration boundary in
	// docs/CONFORMANCE.md states what it cannot cover.
	if d.ci.Config.InactiveThreshold != 0 {
		return _EMPTY_, fmt.Errorf("refusing to admit against %q on %q for client %q: the consumer carries an effective "+
			"inactivity threshold of %v, which would reap it under an obligation that depends on it",
			d.durName, d.streamName, id, d.ci.Config.InactiveThreshold)
	}
	return inc, nil
}
