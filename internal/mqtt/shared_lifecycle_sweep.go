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
	"strings"
)

// The shared-group lifecycle's SWEEP arms (F-105, the shared-group lifecycle transfer): what
// the periodic per-account maintenance does for group durables now that no walk of the
// session records decides their fate.
//
//   - sweepSharedConsumer classifies one `$MQTT_SHARED_` consumer the orphaned-consumer sweep
//     found against its GROUP RECORD — a record that exists — and deletes it only under that
//     record's authority: the incarnation is retiring (finish the retirement), or it is
//     DERIVED retired (a delayed create that landed after retirement). Everything else is
//     retained: the live incarnation, an ordinal the record never issued, a group record
//     this broker cannot read or that is absent, and the pre-transfer name shape, which
//     carries no incarnation and is an unsupported persisted shape.
//   - sweepSharedGroupRetirements walks the group records and finishes every retiring entry
//     whose leaver crashed inside L2: the consumer's deletion classified (a consumer already
//     gone is `gone`) and the entry removed.
//   - sweepAcqReleaseVerdicts is a pass OF ITS OWN over the session records that EXECUTES
//     committed release verdicts, connected or not — the verdict is the release authority,
//     and a hard crash leaves DisconnectedAt at zero on a record nothing else revisits when its
//     session never expires — once the same verdicts have been observed on two consecutive
//     passes, WHATEVER the record's sequence did meanwhile (F-105 7e): a connected session's
//     unrelated saves move the sequence on every pass, and keying on it left a verdict whose
//     owner's run failed with no executor at all. It never writes the record: the owner clears
//     a verdict on its next save, and the release CAS makes this executor converge with a live
//     owner.
//
// Every arm acts only on records it READ. Exhaustion of any walk authorizes nothing, and
// neither record class on $MQTT_shared is ever deleted.

// mqttSharedConsumerDisposition is what the consumer sweep decided about one shared consumer.
type mqttSharedConsumerDisposition uint8

const (
	// mqttSharedConsumerRetain: the live incarnation, or a reading that licenses nothing.
	mqttSharedConsumerRetain mqttSharedConsumerDisposition = iota + 1
	// mqttSharedConsumerRefusedShape: the pre-transfer name, retained and reported once per pass.
	mqttSharedConsumerRefusedShape
	// mqttSharedConsumerRetiring: its deletion is owed; the sweep finishes L2.
	mqttSharedConsumerRetiring
	// mqttSharedConsumerRetired: derived retired; deleted under the group record's authority.
	mqttSharedConsumerRetired
)

// classifySharedConsumer reads the consumer's group record and applies the derived-retirement
// rule. A group record that is absent attributes nothing: a consumer is created only after its
// join landed, so absence licenses nothing and the consumer is retained with a warning.
func (as *mqttAccountSessionManager) classifySharedConsumer(name string) (mqttSharedConsumerDisposition, string, uint64) {
	tail, ok := strings.CutPrefix(name, mqttSharedConsumerDurablePrefix)
	if !ok {
		return mqttSharedConsumerRetain, _EMPTY_, 0
	}
	qkey, inc, ok := mqttParseSharedIncName(tail)
	if !ok {
		return mqttSharedConsumerRefusedShape, _EMPTY_, 0
	}
	g := as.jsa.loadSharedGroup(as.sessStreamEpoch, qkey)
	if g.status != mqttOutboundValid {
		return mqttSharedConsumerRetain, qkey, inc
	}
	switch g.rec.classifyInc(inc) {
	case mqttSharedIncRetiring:
		return mqttSharedConsumerRetiring, qkey, inc
	case mqttSharedIncRetired:
		return mqttSharedConsumerRetired, qkey, inc
	}
	return mqttSharedConsumerRetain, qkey, inc
}

// sweepSharedConsumers disposes of the shared consumers the orphaned-consumer sweep collected.
func (as *mqttAccountSessionManager) sweepSharedConsumers(s *Server, names []string) {
	refused := 0
	for _, name := range names {
		disp, qkey, inc := as.classifySharedConsumer(name)
		switch disp {
		case mqttSharedConsumerRefusedShape:
			refused++
		case mqttSharedConsumerRetiring:
			if err := as.jsa.retireSharedIncarnation(as.sessStreamEpoch, qkey, inc); err != nil {
				s.Debugf("MQTT orphaned consumer sweep: finishing the retirement of %q: %v", name, err)
			}
		case mqttSharedConsumerRetired:
			// DERIVED from the group record: allocated, not live, not retiring, so it passed
			// through live -> retiring -> deleted and this consumer is a delayed create that
			// landed afterwards. Deleted under that record's authority, never on its absence.
			if _, err := as.jsa.deleteConsumer(mqttStreamName, name, false); isErrorOtherThan(err, JSConsumerNotFoundErr) {
				s.Warnf("MQTT orphaned consumer sweep: unable to delete retired shared consumer %q: %v", name, err)
				continue
			}
			s.Debugf("MQTT orphaned consumer sweep deleted retired shared consumer %q (incarnation %d of %q)", name, inc, qkey)
		default:
			if qkey == _EMPTY_ {
				continue
			}
			s.Debugf("MQTT orphaned consumer sweep retains shared consumer %q: its group record licenses no deletion", name)
		}
	}
	if refused > 0 {
		s.Warnf("MQTT orphaned consumer sweep: %d shared consumer(s) carry a name with no incarnation, a "+
			"format this broker no longer writes; they are retained, not deleted. Stop this account's "+
			"brokers and recreate the bridge-owned JetStream state (the $MQTT_* streams); this is not a "+
			"rolling upgrade", refused)
	}
}

// sweepSharedGroupRetirements finishes every retiring entry on every group record: a leaver
// that crashed inside L2 left the entry as the durable marker of its unfinished reclamation.
func (as *mqttAccountSessionManager) sweepSharedGroupRetirements(s *Server) {
	filter := mqttSharedGroupWalkFilter()
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSharedLifecycleStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT shared group sweep ended: %v", err)
			}
			return
		}
		seq = smsg.Sequence + 1
		rest := strings.TrimPrefix(smsg.Subject, mqttSharedLifecycleStreamSubjectPrefix+mqttSharedGroupClassTk+".")
		g, derr := mqttDecodeSharedGroup(smsg.Data, as.sessStreamEpoch, rest)
		if derr != nil {
			// A record this broker cannot interpret is retained whole; nothing on it is acted
			// on, and it is reported so an operator can act.
			s.Warnf("MQTT shared group sweep: the group record at sequence %d cannot be interpreted and is left alone: %v",
				smsg.Sequence, derr)
			continue
		}
		for _, inc := range g.Retiring {
			if err := as.jsa.retireSharedIncarnation(as.sessStreamEpoch, g.Group, inc); err != nil {
				s.Debugf("MQTT shared group sweep: finishing the retirement of incarnation %d of %q: %v", inc, g.Group, err)
			}
		}
	}
}

// mqttSharedVerdictObservation is one pass's reading of a record's verdicts, keyed by the
// record's subject: the verdicts it carried. NOT the sequence it stood at: a verdict is durable
// authority in every record that carries it, so its stability is the verdicts' own.
type mqttSharedVerdictObservation struct {
	verdicts string
}

// sweepAcqReleaseVerdicts executes committed release verdicts from every session record,
// after the same verdicts were observed on two consecutive passes. Executing concurrently with a
// live owner is safe — L1's CAS on the exact entry converges — and a live owner that keeps
// saving its record for other reasons is no evidence that it executes its verdicts.
func (as *mqttAccountSessionManager) sweepAcqReleaseVerdicts(s *Server) {
	filter := mqttSessStreamSubjectPrefix + as.domainTk + ">"
	seen := make(map[string]mqttSharedVerdictObservation)
	for seq := uint64(1); ; {
		smsg, err := as.jsa.loadNextMsgFromSeq(mqttSessStreamName, filter, seq)
		if err != nil {
			if isErrorOtherThan(err, JSNoMessageFoundErr) {
				s.Debugf("MQTT shared verdict sweep ended: %v", err)
				return
			}
			break
		}
		seq = smsg.Sequence + 1
		ps := &mqttPersistedSession{}
		if uerr := json.Unmarshal(smsg.Data, ps); uerr != nil {
			// Unreadable: nothing here is acted on. The record-shape refusals belong to the
			// readers that would mutate the record; this one only executes what a readable
			// record says.
			continue
		}
		if len(ps.AcqReleases) == 0 {
			continue
		}
		rendered, _ := json.Marshal(ps.AcqReleases)
		obs := mqttSharedVerdictObservation{verdicts: string(rendered)}
		seen[smsg.Subject] = obs
		as.mu.Lock()
		prev, had := as.sharedVerdictSeen[smsg.Subject]
		as.mu.Unlock()
		if !had || prev != obs {
			continue
		}
		// STABLY OBSERVED: the same verdicts on two consecutive passes.
		// Execute them. The record is never written here; its owner clears a verdict on its
		// own next save, and the release CAS converges with it.
		if !mqttValidSessGen(ps.Gen) {
			continue
		}
		for _, r := range ps.AcqReleases {
			if _, rerr := as.jsa.executeSharedRelease(as.sessStreamEpoch, ps.ID, ps.Gen, r); rerr != nil {
				s.Debugf("MQTT shared verdict sweep: executing the release of %q for %q: %v", r.Acq, ps.ID, rerr)
			}
		}
	}
	as.mu.Lock()
	as.sharedVerdictSeen = seen
	as.mu.Unlock()
}
