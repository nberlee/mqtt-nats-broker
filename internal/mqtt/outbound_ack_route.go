// Copyright 2026 The mqtt-nats-bridge Authors
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

// A LATE ACKNOWLEDGEMENT IS BOUND BEFORE IT ARRIVES (F-088 / F-097, WP2).
//
// A transmitted PUBLISH whose scheduling entry is removed before the client answers used to lose
// its answer: the handler found no entry and no binding and dropped the PUBACK, and the exchange
// was held to the generation's discard. Every entry removal now states WHY (mqttUntrackReason), and
// only one reason — removed UNANSWERED with positive evidence the PUBLISH was ATTEMPTED — installs
// an AckRoute binding, in the same session-lock section that removes the entry. The binding routes
// the client's acknowledgement to the existing completion owners; it never authorizes output,
// allocates nothing and holds no charge (the custody ledger holds those, by the exchange).

// mqttSendEvidence is what an ENTRY proves about whether its PUBLISH was attempted (R1-F2). The zero
// value is Unknown, and Unknown never installs anything: a zero value is not evidence.
type mqttSendEvidence uint8

const (
	// mqttSendEvidenceUnknown: nothing established.
	mqttSendEvidenceUnknown mqttSendEvidence = iota
	// mqttSendEvidenceAttemptedConfirmed: an acting, settled, own entry recorded its transmission.
	mqttSendEvidenceAttemptedConfirmed
	// mqttSendEvidenceMarkerStanding: the allocation's unsent marker stands; never transmitted.
	mqttSendEvidenceMarkerStanding
)

// (The RECORD form of this evidence - a register's marker read authoritatively absent - is the
// write boundary's own read, mqttRefutedCancellation; no binding is ever built from a record.)

// mqttEntrySendEvidence is the ENTRY form of the evidence: AttemptedConfirmed only for an acting
// entry this object owns that recorded its transmission, and that is not a pending store, a foreign
// or marker-opaque reservation, or a cancelled placeholder; MarkerStanding for an acting entry that
// holds its marker and never transmitted; Unknown otherwise.
func mqttEntrySendEvidence(ack *mqttPending) mqttSendEvidence {
	if ack == nil || !ack.acts() || ack.regPending || ack.regForeign || ack.markerOpaque || ack.regCancelled {
		return mqttSendEvidenceUnknown
	}
	switch {
	case ack.transmitted:
		return mqttSendEvidenceAttemptedConfirmed
	case ack.markerSeq != 0:
		return mqttSendEvidenceMarkerStanding
	}
	return mqttSendEvidenceUnknown
}

// mqttUntrackReason is the MANDATORY reason a PUBLISH entry is removed (R1-F1, Q1).
type mqttUntrackReason uint8

const (
	// mqttUntrackAnswered: the client's acknowledgement is being handled (the ACK handler, and
	// endSendWith's acknowledgement settlement). Installs nothing.
	mqttUntrackAnswered mqttUntrackReason = iota + 1
	// mqttUntrackEnded: the exchange positively ENDED (removeLineageProjectionsLocked, from an
	// accepted ended observation). Installs nothing.
	mqttUntrackEnded
	// mqttUntrackUnanswered: every other removal. With AttemptedConfirmed evidence an AckRoute is
	// installed in the same section; with MarkerStanding nothing is bound (a cancellation that
	// decided it owes its withdrawal, recorded at its failure exits); with Unknown nothing is bound
	// and the producing owner keeps it.
	mqttUntrackUnanswered
)

// installAckRouteForRemovalLocked installs the AckRoute an UNANSWERED removal of a transmitted
// entry owes, before the entry goes. It reports whether one was installed. Refused — silently, as
// nothing is lost against HEAD — for an ended lineage (the veto), an identifier another
// allocation's binding holds, a retained or slot-only entry, or an entry that is not provably
// attempted.
//
// Session lock held on entry.
func (sess *mqttSession) installAckRouteForRemovalLocked(ack *mqttPending, why mqttUntrackReason) bool {
	if why != mqttUntrackUnanswered || mqttEntrySendEvidence(ack) != mqttSendEvidenceAttemptedConfirmed {
		return false
	}
	b := newMQTTAckRouteBinding(sess, ack)
	if b == nil {
		return false
	}
	return sess.installAbandonedAckLocked(b)
}
