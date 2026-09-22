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

// Point-read census node records for the shared-group lifecycle transfer's two per-subject
// readers (F-105). Both read $MQTT_shared through loadLastMsgFor and CLASSIFY a 404 as
// absence, so both inherit F-108's exposure: a store-read failure that arrives as
// JSNoMessageFoundErr is read as "the subject is empty". Their callers settle that apparent
// absence with a create-only CAS, which is F-110's boundary — the store's contract, not this
// broker's defect (docs/known-upstream-issues.md). The census records the sites; it does not
// compensate for the upstream defect.

func init() {
	prCensusRegister("shared_lifecycle.go:(mqttJSA).loadSharedGroup", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating the group's arbitration record as ABSENT, which across its callers " +
			"authorizes creating the group create-only at incarnation 1, mounting a fresh " +
			"incarnation, and reading a group as never-joined — each settled by a create-only " +
			"CAS that inherits F-110",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "none available here: the group record has no sequence a re-attempt could " +
			"name, and a create-only CAS over apparent absence is F-110's, whose admission " +
			"check does not establish that the subject was empty. The destructive consequence " +
			"is SUSPECTED and needs a traced witness (docs/ROADMAP.md's F-105 bullet says so): " +
			"the classification is demonstrated, the wire effect is not",
		Edges: []prCensusEdge{
			{Key: "CALLER shared_lifecycle.go:(mqttJSA).commitSharedLifecycleBatch#4", Disp: edgeExpand, To: "shared_lifecycle.go:(mqttJSA).commitSharedLifecycleBatch"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttJSA).executeSharedRelease#3", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttJSA).executeSharedRelease"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttJSA).retireSharedIncarnation#2", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttJSA).retireSharedIncarnation"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttSession).acquireShared#10", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttSession).acquireShared"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttSession).installAndReconcileShared#32", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttSession).installAndReconcileShared"},
			{Key: "CALLER shared_lifecycle_sweep.go:(mqttAccountSessionManager).classifySharedConsumer#2", Disp: edgeExpand, To: "shared_lifecycle_sweep.go:(mqttAccountSessionManager).classifySharedConsumer"},

			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "the test-hook pointer Load; nil in production and never a read"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "the test hook invocation; nil in production"},
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "mqttSharedGroupSubject builds the subject string; reads nothing"},
			{Key: "CALL#4", Disp: edgeDischarge,
				Why: "isErrorOtherThan(err, JSNoMessageFoundErr) IS this node's local mapping and " +
					"its point of failure: a store-read failure reaches it normalized to " +
					"JSNoMessageFoundErr, returns false, and the absence arm is taken. Accounted " +
					"on axis A rather than as a destination"},
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "mqttDecodeSharedGroup runs with a StoredMsg in hand; an undecodable body is " +
					"a DEFINITE fact about a record that WAS read, classified corrupt not absent"},
		},
	})

	prCensusRegister("shared_lifecycle.go:(mqttJSA).loadSharedLedger", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating the generation's acquisition ledger as ABSENT, which across its callers " +
			"authorizes a first acquisition creating the ledger create-only, a tenure " +
			"installation over absence, the abort init fence, and an empty seal over absence — " +
			"each a create-only CAS that inherits F-110",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "none available here, for loadSharedGroup's reason: the ledger is permanent, so " +
			"true absence means the generation never acquired, but a create-only CAS cannot " +
			"establish that the subject was empty (F-110). The consequence is SUSPECTED",
		Edges: []prCensusEdge{
			{Key: "CALLER shared_lifecycle.go:(mqttJSA).commitSharedLifecycleBatch#6", Disp: edgeExpand, To: "shared_lifecycle.go:(mqttJSA).commitSharedLifecycleBatch"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttJSA).executeSharedRelease#0", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttJSA).executeSharedRelease"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttJSA).installSharedTenure#0", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttJSA).installSharedTenure"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttJSA).sealSharedLedger#0", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttJSA).sealSharedLedger"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttJSA).sharedClosingComplete#0", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttJSA).sharedClosingComplete"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttSession).abortSharedAcquisition#0", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttSession).abortSharedAcquisition"},
			{Key: "CALLER shared_lifecycle_ops.go:(mqttSession).acquireShared#9", Disp: edgeExpand, To: "shared_lifecycle_ops.go:(mqttSession).acquireShared"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "mqttSharedLedgerSubject builds the subject string; reads nothing"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "getHash(id) builds the client-ID hash for the subject; reads nothing"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "isErrorOtherThan(err, JSNoMessageFoundErr) IS this node's local mapping, as " +
					"in loadSharedGroup: a store-read failure normalized to the 404 identifier " +
					"takes the absence arm. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "mqttDecodeSharedLedger runs with a StoredMsg in hand; an undecodable body " +
					"is corrupt, a definite fact about a record that WAS read"},
		},
	})
}
