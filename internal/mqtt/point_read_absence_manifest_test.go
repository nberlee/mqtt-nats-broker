// F-109's census manifest: the population, node by node.
//
// The accounting contract is stated in point_read_absence_census_test.go and the dossier is
// docs/findings/point-read-absence-census.md. Every node here declares its interpretation of a
// point read, where it makes one, and an entry for EVERY candidate boundary edge the scanner
// finds leaving it — expand, link, or discharge with a cited reason. A missing entry fails;
// a stale one fails; a discharge with no reason fails.
//
// The axes and what they mean are in the census file. Two things about reading this manifest:
//
//   - `Auth` is what the ABSENT outcome authorizes, which is the column a repair has to
//     satisfy. It is written from the code, not from the site's own comment: this package's
//     comments are frequently right about the response and wrong about the store, which is the
//     whole of F-109.
//   - `Repair` names a candidate and its OUTSTANDING PREMISES. Naming a fact class does not
//     establish that a repair exists, so a row that has no available candidate says so.

package mqtt

func init() {
	// ---- mqtt.go:(mqttAccountSessionManager).loadRetainedMessages ------------------------
	//
	// The bullet's own row, confirmed by reading. The batched classification here is INLINE:
	// it never calls mqttClassifyMsgGetResponse and spells its 404 test `IsNatsErr`, so
	// repairing the shared classifier does not repair this path and no grep on the shared
	// predicate finds it. That independence is why the bullet marks it an independent audit
	// obligation.
	prCensusRegister("mqtt.go:(mqttAccountSessionManager).loadRetainedMessages", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchDistinguished,
		Auth: "removing this subject's entry from the retained index, at the sequence the index " +
			"recorded. removeRetainedMsg compares that sequence and declines to remove when it " +
			"has moved, which covers a retained message that ARRIVED during the load — the case " +
			"the code comment is about — but it does not arbitrate this one: a failed read of a " +
			"standing record whose sequence has not moved passes the compare and the live " +
			"retained message is dropped from the index",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "no exact-sequence re-attempt is available on the terms this caller needs, and " +
			"the reason is deliberate: the site's own comment explains that loading BY SEQUENCE " +
			"would wrongly remove the entry when a newer retained message has replaced it, which " +
			"is why it reads last-for-subject. A direct multi-last get is the only unblocked " +
			"candidate and carries its six stated premises — truncation must be rejected, a " +
			"marker must be ENCOUNTERED rather than requested, a captured UpToSeq can hide a " +
			"replacement and pushes the request into HISTORICAL backward selection which ignores " +
			"lookup errors, and direct get is follower-servable so it owes freshness evidence. " +
			"The consequence is SUSPECTED from the traced path rather than from a comment: false " +
			"absence removes the index entry, and a later SUBSCRIBE then delivers no retained " +
			"message for that filter. No runtime witness exists, so this census does not assert " +
			"the wire effect",
		Edges: []prCensusEdge{
			{Key: "CALLER mqtt.go:(mqttAccountSessionManager).processSubs#35", Disp: edgeExpand,
				To: "mqtt.go:(mqttAccountSessionManager).processSubs"},

			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :11580 make
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :11580 len
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"}, // :11583 getCachedRetainedMsg
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"}, // :11586 append
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"}, // :11590 len
			{Key: "CALL#5", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgForMulti"},

			{Key: "CALL#6", Disp: edgeDischarge,
				Why: "the transport-level error from the batch is WARNED and the function then " +
					"proceeds with the responses it did get. The warning carries the obligation " +
					"nowhere; what proceeding with a short batch authorizes is accounted by the " +
					"per-response entries below, and by the no-response arm this node records as " +
					"distinguished"},

			{Key: "CALL#7", Disp: edgeDischarge,
				Why: "result.ToError() produces the very error this node classifies, so it is the " +
					"operand of the local mapping rather than a destination for it. The mapping " +
					"itself is this node's axis A and is accounted there"},
			{Key: "CALL#8", Disp: edgeDischarge,
				Why: "len() over the subject prefix is string arithmetic on ss[i], a value fixed " +
					"before the read; it cannot carry the read's outcome"},
			{Key: "CALL#9", Disp: edgeDischarge,
				Why: "IsNatsErr(err, JSNoMessageFoundErr) IS the 404 test — the defective " +
					"classification this row records on axis A. It is the mapping, not an edge " +
					"the mapping's result travels along"},

			{Key: "CALL#10", Disp: edgeExpand, To: "mqtt.go:(mqttAccountSessionManager).removeRetainedMsg",
				Why: "THE consequential call on the absent path: a 404, genuine or normalized " +
					"from a failed read, removes the retained-index entry here"},

			{Key: "CALL#11", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY",
				Why: "this Warnf is the NON-404 arm, which is the arm a trustworthy indeterminate " +
					"read would take; it is the fallback F-109 shows cannot be reached for a " +
					"store-read failure"},
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :11626 decode
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :11630 len
			{Key: "CALL#14", Disp: edgeDischarge,
				Why: "this removeRetainedMsg is on the DECODE-FAILURE arm, reached only with a " +
					"message in hand. It removes the entry for a different fact — a body this " +
					"broker cannot interpret — and no outcome of the read's absence arm reaches " +
					"it, because that arm has already continued"},
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :11633 Warnf
			{Key: "CALL#16", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :11636 len
			{Key: "CALL#17", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :11642 len
			{Key: "CALL#18", Disp: edgeDischarge,
				Why: "this removeRetainedMsg is on the empty-body TOMBSTONE arm, reached with a " +
					"decoded message whose payload is empty. That is a positive fact about a " +
					"record that was read, not an inference from absence"},
			{Key: "CALL#19", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :11650 setCachedRetainedMsg

			{Key: "MUT#3", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER",
				Why: "this write is the CACHE-HIT path and runs before the read; rms is returned " +
					"to the caller and that return is the accounted edge"},
			{Key: "MUT#15", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER",
				Why: "the present path's insertion of a decoded message into the returned map"},
		},
	})
}

func init() {
	// ---- mqtt.go:(mqttJSA).loadSessionMsg -------------------------------------------------
	//
	// A PURE FORWARDER, and the reason this census is keyed on interpretation sites rather
	// than on loader calls. It returns (*StoredMsg, error) from loadLastMsgFor untested, so it
	// classifies nothing — and its SIXTEEN callers are the real 404-interpretation sites,
	// three of which F-109's bullet names directly (sess_teardown.go:1066 "absent: done",
	// sess_teardown.go:235's mqttTeardownClaimSuperseded, and sess_gone_retire.go:392). An
	// inventory keyed on the primitive call would have carried ONE row here and hidden all
	// sixteen.
	//
	// F-034's ingress manifest describes this function as "reads a session record; absence
	// means no session, which is a positive answer here rather than an unknown". That is the
	// claim this census qualifies: the function establishes no absence at all, and whether
	// the absence its callers read is a positive answer is exactly what F-109 denies.
	prCensusRegister("mqtt.go:(mqttJSA).loadSessionMsg", prCensusNode{
		Mapping: mapForwarded,
		Batch:   batchNA,
		ClassEv: consNA,
		ConsEv:  consNA,
		Reach:   reachLive,
		Repair: "none of its own: a forwarder has no absence to repair. The repair candidates " +
			"belong to the sixteen interpretation sites it expands into, and they differ — a " +
			"teardown asking whether its release landed is an entitlement question, while a " +
			"gone-retirement asking whether a record is authoritatively absent is an occupancy " +
			"one",
		Edges: []prCensusEdge{
			{Key: "CALLER mqtt.go:(mqttAccountSessionManager).evictDisplacedOnRemotePersist#12", Disp: edgeExpand, To: "mqtt.go:(mqttAccountSessionManager).evictDisplacedOnRemotePersist"},
			{Key: "CALLER mqtt.go:(mqttAccountSessionManager).reconcileRemoteSessionExpiry#4", Disp: edgeExpand, To: "mqtt.go:(mqttAccountSessionManager).reconcileRemoteSessionExpiry"},
			{Key: "CALLER mqtt.go:(mqttAccountSessionManager).expireSessionFired#18", Disp: edgeExpand, To: "mqtt.go:(mqttAccountSessionManager).expireSessionFired"},
			{Key: "CALLER mqtt.go:(mqttAccountSessionManager).loadExpiringSessionRecord#0", Disp: edgeExpand, To: "mqtt.go:(mqttAccountSessionManager).loadExpiringSessionRecord"},
			{Key: "CALLER mqtt.go:(mqttAccountSessionManager).createOrRestoreSessionWithin#1", Disp: edgeExpand, To: "mqtt.go:(mqttAccountSessionManager).createOrRestoreSessionWithin"},
			{Key: "CALLER sess_fork_lineage.go:(mqttSession).disposalForDirectRejection#0", Disp: edgeExpand, To: "sess_fork_lineage.go:(mqttSession).disposalForDirectRejection"},
			{Key: "CALLER sess_gone_retire.go:(mqttAccountSessionManager).proveGoneRetirement#8", Disp: edgeExpand, To: "sess_gone_retire.go:(mqttAccountSessionManager).proveGoneRetirement"},
			{Key: "CALLER sess_rejected_write.go:(mqttSession).probeRecordRejectionInTx#4", Disp: edgeExpand, To: "sess_rejected_write.go:(mqttSession).probeRecordRejectionInTx"},
			{Key: "CALLER sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave#6", Disp: edgeExpand, To: "sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave"},
			{Key: "CALLER sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave#26", Disp: edgeExpand, To: "sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave"},
			{Key: "CALLER sess_teardown.go:(mqttSession).claimTeardown#9", Disp: edgeExpand, To: "sess_teardown.go:(mqttSession).claimTeardown"},
			{Key: "CALLER sess_teardown.go:(mqttSession).releaseTeardown#8", Disp: edgeExpand, To: "sess_teardown.go:(mqttSession).releaseTeardown"},
			{Key: "CALLER sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx#5", Disp: edgeExpand, To: "sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx"},
			{Key: "CALLER sess_uncertain.go:(mqttSession).reattemptUncertainWrite#6", Disp: edgeExpand, To: "sess_uncertain.go:(mqttSession).reattemptUncertainWrite"},
			{Key: "CALLER sess_uncertain.go:(mqttAccountSessionManager).reconcileResidentOwnership#11", Disp: edgeExpand, To: "sess_uncertain.go:(mqttAccountSessionManager).reconcileResidentOwnership"},
			{Key: "CALLER will_outbox.go:(mqttAccountSessionManager).loadFenceSession#3", Disp: edgeExpand, To: "will_outbox.go:(mqttAccountSessionManager).loadFenceSession"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
		},
	})

	// ---- inflight_register.go:(mqttJSA).loadInflightRecord --------------------------------
	//
	// The four-outcome register read, and the sharpest row in the bullet. Its own doc comment
	// states the rule F-109 defeats: "(nil, nil, err) INDETERMINATE, never to be guessed into
	// an absence: absence authorizes reusing an identifier, which is the one decision that can
	// put a duplicate identifier on the wire." The indeterminate arm is gated on
	// isErrorOtherThan(err, JSNoMessageFoundErr), and a store-read failure arrives AS
	// JSNoMessageFoundErr, so for that failure the arm cannot fire and the 404 arm returns
	// (nil, nil, nil) — the absence the comment forbids guessing.
	prCensusRegister("inflight_register.go:(mqttJSA).loadInflightRecord", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating the register subject as EMPTY, which across its callers authorizes " +
			"reusing the packet identifier the register reserves, releasing a slot as though " +
			"nothing stood there, and CASing create-only against a subject an occupant may hold",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "an exact-sequence re-attempt is NOT generally available here, because the " +
			"caller that most needs it is asking whether ANY register stands on a subject and " +
			"therefore has no sequence to name. A create-only CAS as the settlement inherits " +
			"F-110, whose admission check does not establish that the subject was empty. The " +
			"duplicate-identifier consequence its comment names is SUSPECTED and needs a traced " +
			"witness: the classification is demonstrated, the wire effect is not",
		Edges: []prCensusEdge{
			{Key: "CALLER inflight_marker.go:(mqttAccountSessionManager).loadInflightMarker#4", Disp: edgeExpand, To: "inflight_marker.go:(mqttAccountSessionManager).loadInflightMarker"},
			{Key: "CALLER inflight_marker.go:(mqttAccountSessionManager).siblingObservation#0", Disp: edgeExpand, To: "inflight_marker.go:(mqttAccountSessionManager).siblingObservation"},
			{Key: "CALLER inflight_register.go:(mqttSession).casInflightSlot#1", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).casInflightSlot"},
			{Key: "CALLER inflight_register.go:(mqttSession).mqttSlotEvidenceFor#0", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).mqttSlotEvidenceFor"},
			{Key: "CALLER inflight_register.go:(mqttSession).confirmSlotReleased#0", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).confirmSlotReleased"},
			{Key: "CALLER inflight_register.go:(mqttAccountSessionManager).loadInflightRecord#3", Disp: edgeExpand, To: "inflight_register.go:(mqttAccountSessionManager).loadInflightRecord"},
			{Key: "CALLER inflight_register.go:(mqttAccountSessionManager).releaseBoundInflightRegister#0", Disp: edgeExpand, To: "inflight_register.go:(mqttAccountSessionManager).releaseBoundInflightRegister"},
			{Key: "CALLER inflight_register.go:(mqttAccountSessionManager).releaseBoundInflightRegister#20", Disp: edgeExpand, To: "inflight_register.go:(mqttAccountSessionManager).releaseBoundInflightRegister"},
			{Key: "CALLER inflight_register.go:(mqttSession).attemptTerminalRegisterRelease#9", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).attemptTerminalRegisterRelease"},
			{Key: "CALLER inflight_register.go:(mqttSession).reapRegisterGatedOutcome#0", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).reapRegisterGatedOutcome"},
			{Key: "CALLER inflight_register.go:(mqttSession).markInflightRegisterCancelled#7", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).markInflightRegisterCancelled"},
			{Key: "CALLER inflight_register.go:(mqttSession).markInflightRegisterCancelledPending#7", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).markInflightRegisterCancelledPending"},
			{Key: "CALLER inflight_register.go:(mqttSession).storeInflightRegister#15", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).storeInflightRegister"},
			{Key: "CALLER inflight_register.go:(mqttSession).acquirePublishAction#1", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).acquirePublishAction"},
			{Key: "CALLER inflight_register.go:(mqttSession).resolveUnacquiredPublishAction#0", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).resolveUnacquiredPublishAction"},
			{Key: "CALLER inflight_register.go:(mqttSession).drainPublishAction#1", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).drainPublishAction"},
			{Key: "CALLER inflight_register.go:(mqttSession).claimRecoveredInflight#16", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).claimRecoveredInflight"},
			{Key: "CALLER inflight_register.go:(mqttSession).recoverInflightRegistersOnce#252", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).recoverInflightRegistersOnce"},
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOrphanInflight#22", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOrphanInflight"},
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).settleRetiredAffineRegister#16", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).settleRetiredAffineRegister"},
			{Key: "CALLER outbound_replace_register.go:(mqttJSA).checkRegisterReplacements#11", Disp: edgeExpand, To: "outbound_replace_register.go:(mqttJSA).checkRegisterReplacements"},
			{Key: "CALLER outbound_admit_orchestrate.go:(mqttSession).landedRegisterSeq#0", Disp: edgeExpand, To: "outbound_admit_orchestrate.go:(mqttSession).landedRegisterSeq"},
			{Key: "CALLER outbound_withdraw_unsent.go:(mqttJSA).opWithdrawUnsent#3", Disp: edgeExpand, To: "outbound_withdraw_unsent.go:(mqttJSA).opWithdrawUnsent"},
			{Key: "CALLER outbound_withdraw_unsent.go:(mqttSession).endUnsentBeforeDestroy#7", Disp: edgeExpand, To: "outbound_withdraw_unsent.go:(mqttSession).endUnsentBeforeDestroy"},
			{Key: "CALLER outbound_withdraw_unsent.go:(mqttSession).endUnsentBeforeDestroy#8", Disp: edgeExpand, To: "outbound_withdraw_unsent.go:(mqttSession).endUnsentBeforeDestroy"},
			{Key: "CALLER outbound_abandon_delivery.go:(mqttJSA).opAbandonDeniedDelivery#4", Disp: edgeExpand, To: "outbound_abandon_delivery.go:(mqttJSA).opAbandonDeniedDelivery"},
			{Key: "CALLER outbound_abandon_delivery.go:(mqttSession).abandonCurrentOutputVeto#0", Disp: edgeExpand, To: "outbound_abandon_delivery.go:(mqttSession).abandonCurrentOutputVeto"},
			{Key: "CALLER pubrel_action.go:(mqttSession).resolvePubRelIntent#0", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).resolvePubRelIntent"},
			{Key: "CALLER pubrel_action.go:(mqttSession).phaseRecoveredPublishSlotToReady#5", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).phaseRecoveredPublishSlotToReady"},
			{Key: "CALLER pubrel_action.go:(mqttSession).resolveUnacquiredAction#0", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).resolveUnacquiredAction"},
			// F-105 row 2: the ordinary PUBREL acquisition's base read (#1) and its post-landing
			// readback (#8). Both fail closed — any absent, unreadable or erroring read returns
			// not-acquired with nothing emitted, and the caller's resolveUnacquiredAction reread
			// (already expanded above) settles an ambiguous attempt — so the obligation the
			// caller carries is the same one that resolution already accounts.
			{Key: "CALLER pubrel_action.go:(mqttSession).acquirePubRelThroughAuthority#1", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).acquirePubRelThroughAuthority"},
			{Key: "CALLER pubrel_action.go:(mqttSession).acquirePubRelThroughAuthority#8", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).acquirePubRelThroughAuthority"},
			{Key: "CALLER pubrel_action.go:(mqttSession).startActionDrain#2", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).startActionDrain"},
			{Key: "CALLER pubrel_action.go:(mqttSession).attemptOwedMaterialization#2", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).attemptOwedMaterialization"},
			{Key: "CALLER pubrel_action.go:(mqttSession).mqttTerminalizeOnPubComp#1", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).mqttTerminalizeOnPubComp"},
			{Key: "CALLER pubrel_action.go:(mqttSession).finalizeTerminalSlot#0", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).finalizeTerminalSlot"},
			{Key: "CALLER pubrel_action.go:(mqttSession).finalizeTerminalSlot#8", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).finalizeTerminalSlot"},
			{Key: "CALLER pubrel_action.go:(mqttSession).finalizeTerminalSlot#14", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).finalizeTerminalSlot"},
			{Key: "CALLER pubrel_action.go:(mqttSession).advanceFinishedSlotToTerminal#2", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).advanceFinishedSlotToTerminal"},
			{Key: "CALLER pubrel_action.go:(mqttSession).admitStoredPubRel#7", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).admitStoredPubRel"},
			{Key: "CALLER pubrel_action.go:(mqttSession).publishTransmissionStands#0", Disp: edgeExpand, To: "pubrel_action.go:(mqttSession).publishTransmissionStands"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "isErrorOtherThan(err, JSNoMessageFoundErr) IS this node's local mapping and " +
					"the precise point of failure: it is the guard the doc comment relies on to " +
					"keep an indeterminate read out of the absence arm, and a store-read failure " +
					"reaches it already normalized to JSNoMessageFoundErr, so it returns false and " +
					"the absence arm is taken. Accounted on axis A rather than as a destination"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "mqttDecodeInflightRecord runs with a StoredMsg in hand; its own outcome — an " +
					"unreadable body — is a DEFINITE fact about a record that was read, which the " +
					"file's comment is careful to distinguish from an indeterminate read"},
		},
	})

	// ---- inflight_register.go:(mqttAccountSessionManager).loadInflightRecordsMulti --------
	//
	// A forwarder with a test seam. Its own comment states the census's thesis exactly:
	// "Both teardown and resume batch through this boundary; sharing the transport does not
	// share their disposition rules."
	prCensusRegister("inflight_register.go:(mqttAccountSessionManager).loadInflightRecordsMulti", prCensusNode{
		Mapping: mapForwarded,
		Batch:   batchNA,
		ClassEv: consNA,
		ConsEv:  consNA,
		Reach:   reachLive,
		Repair: "none of its own; the five call sites it expands into classify the batch " +
			"themselves, three of them through mqttClassifyMsgGetResponse and two through their " +
			"own length checks",
		Edges: []prCensusEdge{
			{Key: "CALLER inflight_register.go:(mqttAccountSessionManager).reapIncarnationRegisters#15", Disp: edgeExpand, To: "inflight_register.go:(mqttAccountSessionManager).reapIncarnationRegisters"},
			{Key: "CALLER inflight_register.go:(mqttAccountSessionManager).reapIncarnationMarkers#12", Disp: edgeExpand, To: "inflight_register.go:(mqttAccountSessionManager).reapIncarnationMarkers"},
			{Key: "CALLER inflight_register.go:(mqttSession).recoverInflightRegistersOnce#22", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).recoverInflightRegistersOnce"},
			{Key: "CALLER inflight_register.go:(mqttSession).recoverInflightRegistersOnce#240", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).recoverInflightRegistersOnce"},
			{Key: "CALLER inflight_register.go:(mqttSession).recoverInflightRegistersOnce#271", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).recoverInflightRegistersOnce"},

			{Key: "CALL#0", Disp: edgeDischarge, Exp: "LOCK"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "LOCK"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "SEAM"},
			{Key: "CALL#3", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgForMulti"},
		},
	})
}

// The `isErrorOtherThan(err, JSNoMessageFoundErr)` call in each of the nodes below is the
// LOCAL MAPPING, not a call on the present path. Every one of them is the guard its file's
// comment relies on to keep an indeterminate read out of the absence arm, and every one of
// them is defeated the same way: the store-read failure reaches it already normalized to
// JSNoMessageFoundErr, so the guard returns false and the absence arm is taken. They are
// accounted on axis A rather than as destinations, and the repetition is the finding.
func init() {
	// ---- qos2_move.go:(mqttJSA).loadQoS2Record --------------------------------------------
	prCensusRegister("qos2_move.go:(mqttJSA).loadQoS2Record", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating the inbound QoS 2 hold subject as EMPTY. Its own comment says so — " +
			"\"(nil, 0, nil) means the subject is empty; a non-nil error means the read was " +
			"indeterminate and must never be guessed into an absence\" — and across its callers " +
			"that empty reading authorizes admitting an inbound PUBLISH as a FRESH hold over a " +
			"standing one, and licenses the orphan sweep's reconcile decisions about a hold it " +
			"believes is gone",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "the admission caller settles with a create-only CAS (mqtt.go:18765 " +
			"storeQoS2HoldCAS with expectation 0), so its candidate inherits F-110: a create-only " +
			"CAS does not establish that the subject was empty, and the re-attempt can be " +
			"admitted over a standing-but-unreadable hold. An exact-sequence re-attempt is " +
			"available only where the caller already holds the sequence, which the admission path " +
			"does not. The wire consequence is SUSPECTED, not demonstrated",
		Edges: []prCensusEdge{
			{Key: "CALLER mqtt.go:(Server).mqttStoreQoS2MsgOnce#2", Disp: edgeExpand, To: "mqtt.go:(Server).mqttStoreQoS2MsgOnce"},
			{Key: "CALLER mqtt.go:(Server).mqttStoreQoS2MsgOnce#10", Disp: edgeExpand, To: "mqtt.go:(Server).mqttStoreQoS2MsgOnce"},
			{Key: "CALLER mqtt.go:(Server).mqttProcessPubRel#6", Disp: edgeExpand, To: "mqtt.go:(Server).mqttProcessPubRel"},
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#0", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2"},
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#23", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2"},
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#27", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2"},
			{Key: "CALLER qos2_move.go:(mqttAccountSessionManager).loadQoS2Record#3", Disp: edgeExpand, To: "qos2_move.go:(mqttAccountSessionManager).loadQoS2Record"},
			{Key: "CALLER qos2_move.go:(mqttAccountSessionManager).finalizeQoS2MoveInner#4", Disp: edgeExpand, To: "qos2_move.go:(mqttAccountSessionManager).finalizeQoS2MoveInner"},
			{Key: "CALLER qos2_release_claim.go:(mqttAccountSessionManager).verifyQoS2HoldForRelease#0", Disp: edgeExpand, To: "qos2_release_claim.go:(mqttAccountSessionManager).verifyQoS2HoldForRelease"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "this is the local mapping itself — the defeated indeterminate guard described " +
					"above this init block — and is accounted on axis A"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "mqttClassifyQoS2Record runs with a StoredMsg in hand"},
		},
	})

	// ---- qos2_release_claim.go:(mqttJSA).loadQoS2RelClaim ---------------------------------
	prCensusRegister("qos2_release_claim.go:(mqttJSA).loadQoS2RelClaim", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating the release-claim subject as empty, which authorizes electing a FRESH " +
			"release claim while one stands. Its doc comment names the stake: absence \"is the " +
			"one decision that can violate the serialization this file provides\"",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "claimQoS2ReleaseInner (:510) already does a create-only CAS FIRST and RETRIES it " +
			"on false absence, which is a real guard and narrows the window — but it does not " +
			"arbitrate, because per F-110 that create-only re-attempt can itself be admitted over " +
			"a standing-but-unreadable claim. So the candidate here is an exact-sequence " +
			"re-attempt, and its outstanding premise is a sequence this caller does not hold when " +
			"it is asking whether any claim stands. The violated-serialization consequence the " +
			"comment names is SUSPECTED: the path does not simply bypass arbitration, and F-109's " +
			"bullet is explicit that this row's consequence needs a traced witness",
		Edges: []prCensusEdge{
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#1", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2"},
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).qos2ClaimUnchanged#0", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).qos2ClaimUnchanged"},
			{Key: "CALLER qos2_release_claim.go:(mqttAccountSessionManager).loadQoS2RelClaim#3", Disp: edgeExpand, To: "qos2_release_claim.go:(mqttAccountSessionManager).loadQoS2RelClaim"},
			{Key: "CALLER qos2_release_claim.go:(mqttAccountSessionManager).claimQoS2ReleaseInner#6", Disp: edgeExpand, To: "qos2_release_claim.go:(mqttAccountSessionManager).claimQoS2ReleaseInner"},
			{Key: "CALLER qos2_release_claim.go:(mqttAccountSessionManager).discardQoS2Hold#4", Disp: edgeExpand, To: "qos2_release_claim.go:(mqttAccountSessionManager).discardQoS2Hold"},
			{Key: "CALLER qos2_release_claim.go:(mqttAccountSessionManager).awaitQoS2Release#0", Disp: edgeExpand, To: "qos2_release_claim.go:(mqttAccountSessionManager).awaitQoS2Release"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "the local mapping itself — the defeated indeterminate guard — accounted on axis A"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "an identity check on a decoded claim; its failure is a DEFINITE fact about a " +
					"record that was read, which this file keeps apart from an indeterminate read"},
		},
	})

	// ---- sess_accept_fact.go:(mqttJSA).readAcceptFact -------------------------------------
	prCensusRegister("sess_accept_fact.go:(mqttJSA).readAcceptFact", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a LANDED acceptance fact as never landed. The present path is guarded by " +
			"a content digest compare, so a fact that is read is classified by content rather " +
			"than by presence — but that guard sits beyond the absence arm and cannot run when " +
			"the read failed",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLive,
		Repair: "an exact-sequence re-attempt is not the shape here, because the question is " +
			"whether a fact this CONNECT wrote is durable, and the settlement rule " +
			"docs/ARCHITECTURE.md states — an ambiguous record write is settled by a CAS, never " +
			"by a readback — already covers it. Where that CAS is create-only it inherits F-110; " +
			"the acceptance fact is written inside the accepted-CONNECT atomic batch, so the " +
			"premise a repair owes is which member of that batch supplies a nonzero expectation. " +
			"No downstream consequence is identified in this census: every consumer found so far " +
			"re-attempts rather than concluding from the absence alone",
		Edges: []prCensusEdge{
			{Key: "CALLER mqtt.go:(mqttSession).persistRecord#29", Disp: edgeExpand, To: "mqtt.go:(mqttSession).persistRecord"},
			{Key: "CALLER sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx#4", Disp: edgeExpand, To: "sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx"},
			{Key: "CALLER sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx#10", Disp: edgeExpand, To: "sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx"},
			{Key: "CALLER sess_uncertain.go:(mqttSession).reattemptUncertainWrite#8", Disp: edgeExpand, To: "sess_uncertain.go:(mqttSession).reattemptUncertainWrite"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "the local mapping itself — the defeated indeterminate guard — accounted on axis A"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "mqttRecordDigest compares the CONTENT of a fact that was read, which is the " +
					"guard that makes this site classify by content rather than by presence"},
		},
	})

	// ---- sess_teardown.go:(mqttJSA).loadGenRetirementRecord -------------------------------
	prCensusRegister("sess_teardown.go:(mqttJSA).loadGenRetirementRecord", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "a retirement WATERMARK decision taken against a record that may exist. Absent " +
			"means no watermark stands for this client, which lets a caller act as though no " +
			"generation had been retired",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLive,
		Repair: "an exact-sequence re-attempt is available to the WRITER, and the site is already " +
			"built for it: the function returns the subject sequence on every non-absent outcome " +
			"\"so the writer can CAS at the exact sequence it read\". The outstanding premise is " +
			"the absent case itself, which has no sequence to name and therefore falls back to a " +
			"create-only CAS inheriting F-110",
		Edges: []prCensusEdge{
			{Key: "CALLER sess_teardown.go:(mqttJSA).loadGenRetirement#2", Disp: edgeExpand, To: "sess_teardown.go:(mqttJSA).loadGenRetirement"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "mqttTeardownSubject is the read's own subject argument, evaluated on the same " +
					"line but strictly before it"},
			{Key: "CALL#2", Disp: edgeDischarge,
				Why: "the local mapping itself — the defeated indeterminate guard — accounted on axis A"},
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "validation of a decoded watermark; its failure is mqttRetirementCorrupt, a " +
					"definite fact about a record that was read. F-034's ingress manifest calls " +
					"this site \"fail-closed when corrupt\", which stays TRUE — what F-109 " +
					"qualifies is that a store-read failure arrives as absence BEFORE this guard " +
					"runs"},
		},
	})

	// ---- will_outbox.go:(mqttJSA).loadWillOutbox ------------------------------------------
	prCensusRegister("will_outbox.go:(mqttJSA).loadWillOutbox", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a WillID's outbox subject as empty, which authorizes enqueuing an outbox " +
			"record for a Will whose record may already stand, and lets the Will fence act on an " +
			"absence it did not establish",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "the enqueue settles with a create-only CAS (will_outbox.go:706 " +
			"storeWillEnqueueCAS with expectation 0), so its candidate inherits F-110. An " +
			"exact-sequence re-attempt is available to callers that already read a record and " +
			"hold its sequence — cancelIfSuperseded and finalizeWillPublish do — and its premise " +
			"is that the absence arm, which has no sequence, is the one that needs the repair. " +
			"The duplicate-publication consequence is SUSPECTED and needs a traced witness",
		Edges: []prCensusEdge{
			{Key: "CALLER will_outbox.go:(mqttAccountSessionManager).armDelayedWillTimer#3", Disp: edgeExpand, To: "will_outbox.go:(mqttAccountSessionManager).armDelayedWillTimer"},
			{Key: "CALLER will_outbox.go:(mqttAccountSessionManager).enqueueDelayedWill#13", Disp: edgeExpand, To: "will_outbox.go:(mqttAccountSessionManager).enqueueDelayedWill"},
			{Key: "CALLER will_outbox.go:(mqttAccountSessionManager).cancelIfSuperseded#19", Disp: edgeExpand, To: "will_outbox.go:(mqttAccountSessionManager).cancelIfSuperseded"},
			{Key: "CALLER will_outbox.go:(mqttAccountSessionManager).finalizeWillPublish#3", Disp: edgeExpand, To: "will_outbox.go:(mqttAccountSessionManager).finalizeWillPublish"},

			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "the local mapping itself — the defeated indeterminate guard — accounted on axis A"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRESENT-ONLY",
				Why: "validateWillOutboxRecord refuses a record that decodes but is not supported, " +
					"returning it as an ERROR rather than as a record or an absence. F-034's " +
					"manifest calls this \"refuses an uninterpretable record\", which stays TRUE; " +
					"what F-109 qualifies is that a store-read failure never reaches it"},
		},
	})
}

// The pending-expansion frontier for the nodes above. Consult C3's staging rule: a
// destination that is owed a node record stays VISIBLE and accounted AS UNFINISHED, with the
// obligation it would receive written down, so the checkpoint cannot pass by simply omitting
// the expansion. Membership is listed per edge; there is no wildcard.
func init() {
	prFormerFrontierCallers("mqtt.go:(mqttAccountSessionManager).loadRetainedMessages",
		"the returned retained-message map, from which every subject whose read reported 404 has been OMITTED and its index entry removed. The caller cannot tell an omission caused by a failed read from a subject that genuinely holds no retained message",
		"mqtt.go:(mqttAccountSessionManager).processSubs#35",
	)

	prFormerFrontierCallers("mqtt.go:(mqttJSA).loadSessionMsg",
		"the UNTESTED (*StoredMsg, error) pair straight from loadLastMsgFor. Every caller performs its own 404 test, so each one is an interpretation site in its own right and owes its own axes; this node establishes no absence at all",
		"mqtt.go:(mqttAccountSessionManager).evictDisplacedOnRemotePersist#12",
		"mqtt.go:(mqttAccountSessionManager).reconcileRemoteSessionExpiry#4",
		"mqtt.go:(mqttAccountSessionManager).expireSessionFired#18",
		"mqtt.go:(mqttAccountSessionManager).loadExpiringSessionRecord#0",
		"mqtt.go:(mqttAccountSessionManager).createOrRestoreSessionWithin#1",
		"sess_fork_lineage.go:(mqttSession).disposalForDirectRejection#0",
		"sess_gone_retire.go:(mqttAccountSessionManager).proveGoneRetirement#8",
		"sess_rejected_write.go:(mqttSession).probeRecordRejectionInTx#4",
		"sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave#6",
		"sess_resume_reconcile.go:(mqttSession).resolveContendedRecordSave#26",
		"sess_teardown.go:(mqttSession).claimTeardown#9",
		"sess_teardown.go:(mqttSession).releaseTeardown#8",
		"sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx#5",
		"sess_uncertain.go:(mqttSession).reattemptUncertainWrite#6",
		"sess_uncertain.go:(mqttAccountSessionManager).reconcileResidentOwnership#11",
		"will_outbox.go:(mqttAccountSessionManager).loadFenceSession#3",
	)

	prFormerFrontierCallers("inflight_register.go:(mqttJSA).loadInflightRecord",
		"the four-outcome register reading, in which a failed store read is indistinguishable from an empty subject: both arrive as (nil, nil, nil). What each caller then does with that empty reading - reuse an identifier, release a slot, CAS create-only - is its own obligation",
		"inflight_marker.go:(mqttAccountSessionManager).siblingObservation#0",
		"inflight_register.go:(mqttSession).casInflightSlot#1",
		"inflight_register.go:(mqttSession).mqttSlotEvidenceFor#0",
		"inflight_register.go:(mqttSession).confirmSlotReleased#0",
		"inflight_register.go:(mqttAccountSessionManager).loadInflightRecord#3",
		"inflight_register.go:(mqttAccountSessionManager).releaseBoundInflightRegister#0",
		"inflight_register.go:(mqttAccountSessionManager).releaseBoundInflightRegister#20",
		"inflight_register.go:(mqttSession).attemptTerminalRegisterRelease#9",
		"inflight_register.go:(mqttSession).reapRegisterGatedOutcome#0",
		"inflight_register.go:(mqttSession).markInflightRegisterCancelled#7",
		"inflight_register.go:(mqttSession).markInflightRegisterCancelledPending#7",
		"inflight_register.go:(mqttSession).storeInflightRegister#15",
		"inflight_register.go:(mqttSession).acquirePublishAction#1",
		"inflight_register.go:(mqttSession).resolveUnacquiredPublishAction#0",
		"inflight_register.go:(mqttSession).drainPublishAction#1",
		"inflight_register.go:(mqttSession).claimRecoveredInflight#16",
		"inflight_register.go:(mqttSession).recoverInflightRegistersOnce#252",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOrphanInflight#22",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).settleRetiredAffineRegister#16",
		"outbound_replace_register.go:(mqttJSA).checkRegisterReplacements#11",
		"pubrel_action.go:(mqttSession).resolvePubRelIntent#0",
		"pubrel_action.go:(mqttSession).phaseRecoveredPublishSlotToReady#5",
		"pubrel_action.go:(mqttSession).resolveUnacquiredAction#0",
		"pubrel_action.go:(mqttSession).startActionDrain#2",
		"pubrel_action.go:(mqttSession).attemptOwedMaterialization#2",
		"pubrel_action.go:(mqttSession).mqttTerminalizeOnPubComp#1",
		"pubrel_action.go:(mqttSession).finalizeTerminalSlot#0",
		"pubrel_action.go:(mqttSession).finalizeTerminalSlot#8",
		"pubrel_action.go:(mqttSession).finalizeTerminalSlot#14",
		"pubrel_action.go:(mqttSession).advanceFinishedSlotToTerminal#2",
		"pubrel_action.go:(mqttSession).admitStoredPubRel#7",
		"pubrel_action.go:(mqttSession).publishTransmissionStands#0",
	)

	prFormerFrontierCallers("inflight_register.go:(mqttAccountSessionManager).loadInflightRecordsMulti",
		"the raw batch of responses and its transport error, untouched. A nil entry, a 404 response and a real response are all still distinguishable here; whether the caller keeps them apart is the caller's obligation",
		"inflight_register.go:(mqttAccountSessionManager).reapIncarnationRegisters#15",
		"inflight_register.go:(mqttAccountSessionManager).reapIncarnationMarkers#12",
		"inflight_register.go:(mqttSession).recoverInflightRegistersOnce#22",
		"inflight_register.go:(mqttSession).recoverInflightRegistersOnce#240",
		"inflight_register.go:(mqttSession).recoverInflightRegistersOnce#271",
	)

	prFormerFrontierCallers("qos2_move.go:(mqttJSA).loadQoS2Record",
		"a (record, seq, err) triple in which (nil, 0, nil) asserts that the inbound QoS 2 hold subject is EMPTY, an assertion a failed store read also produces",
		"mqtt.go:(Server).mqttStoreQoS2MsgOnce#2",
		"mqtt.go:(Server).mqttStoreQoS2MsgOnce#10",
		"mqtt.go:(Server).mqttProcessPubRel#6",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#0",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#23",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#27",
		"qos2_move.go:(mqttAccountSessionManager).loadQoS2Record#3",
		"qos2_move.go:(mqttAccountSessionManager).finalizeQoS2MoveInner#4",
		"qos2_release_claim.go:(mqttAccountSessionManager).verifyQoS2HoldForRelease#0",
	)

	prFormerFrontierCallers("qos2_release_claim.go:(mqttJSA).loadQoS2RelClaim",
		"a (claim, smsg, err) triple in which (nil, nil, nil) asserts that no release claim stands, an assertion a failed store read also produces",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).reconcileOneInboundQoS2#1",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).qos2ClaimUnchanged#0",
		"qos2_release_claim.go:(mqttAccountSessionManager).loadQoS2RelClaim#3",
		"qos2_release_claim.go:(mqttAccountSessionManager).claimQoS2ReleaseInner#6",
		"qos2_release_claim.go:(mqttAccountSessionManager).discardQoS2Hold#4",
		"qos2_release_claim.go:(mqttAccountSessionManager).awaitQoS2Release#0",
	)

	prFormerFrontierCallers("sess_accept_fact.go:(mqttJSA).readAcceptFact",
		"an mqttAcceptFactReadback verdict in which mqttAcceptFactAbsent asserts that no acceptance fact landed, which a failed store read also produces; mqttAcceptFactUnreadable, the arm that would have withheld, cannot be reached for that failure",
		"mqtt.go:(mqttSession).persistRecord#29",
		"sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx#4",
		"sess_uncertain.go:(mqttSession).resolveUncertainWriteInTx#10",
		"sess_uncertain.go:(mqttSession).reattemptUncertainWrite#8",
	)

	prFormerFrontierCallers("sess_teardown.go:(mqttJSA).loadGenRetirementRecord",
		"an mqttRetirementStatus in which mqttRetirementAbsent asserts that no retirement watermark stands, which a failed store read also produces; the mqttRetirementIndeterminate arm cannot be reached for it",
		"sess_teardown.go:(mqttJSA).loadGenRetirement#2",
	)

	prFormerFrontierCallers("will_outbox.go:(mqttJSA).loadWillOutbox",
		"a (record, seq, err) triple in which (nil, 0, nil) asserts that the WillID's outbox subject is empty, which a failed store read also produces",
		"will_outbox.go:(mqttAccountSessionManager).armDelayedWillTimer#3",
		"will_outbox.go:(mqttAccountSessionManager).enqueueDelayedWill#13",
		"will_outbox.go:(mqttAccountSessionManager).cancelIfSuperseded#19",
		"will_outbox.go:(mqttAccountSessionManager).finalizeWillPublish#3",
	)

	prFormerFrontierEntry("mqtt.go:(mqttAccountSessionManager).loadRetainedMessages", "CALL#10",
		"mqtt.go:(mqttAccountSessionManager).removeRetainedMsg",
		"the instruction to remove this subject's retained-index entry at the sequence the index recorded, taken on the strength of a 404 that may be a failed read")
}

// The two EXPERIMENTAL rows. Both are affine-only, so their repair belongs to chapter P
// (docs/ROADMAP.md) rather than to F-109's alpha scope — but they are CLASSIFIED here, because
// an affine caller sharing a defective primitive is part of the population even when its fix
// is somebody else's.
func init() {
	// ---- qos2_shared_affine.go:(mqttJSA).loadQ2AffClaim ------------------------------------
	prCensusRegister("qos2_shared_affine.go:(mqttJSA).loadQ2AffClaim", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating the assignment subject as empty, which authorizes electing a FRESH affine " +
			"claim. The file's own comment says absence \"is the one decision that can violate " +
			"affinity\"",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachExperiment,
		Repair: "chapter P's, not this item's. Recorded because the premise is shared: the claim " +
			"election settles with a create-only CAS (qos2_shared_affine.go:2575, expectation 0) " +
			"and therefore inherits F-110. The affinity violation its comment names is SUSPECTED " +
			"and needs a traced witness",
		Edges: []prCensusEdge{
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).forgetIfQ2AffClaimGone#0", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).forgetIfQ2AffClaimGone"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).q2AffClaimForCarriedCopy#2", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).q2AffClaimForCarriedCopy"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffClaim#3", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffClaim"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).advanceQ2AffClaimChecked#0", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).advanceQ2AffClaimChecked"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).reapQ2AffClaimChecked#0", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).reapQ2AffClaimChecked"},
			{Key: "CALLER qos2_shared_affine.go:(mqttSession).moveStartedSharedQoS2#28", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttSession).moveStartedSharedQoS2"},
			{Key: "CALLER qos2_shared_affine.go:(mqttSession).completeQ2AffOwnClaim#1", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttSession).completeQ2AffOwnClaim"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).q2AffCopyIsDisowned#1", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).q2AffCopyIsDisowned"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffOnBehalf#26", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffOnBehalf"},
			{Key: "CALLER qos2_shared_affine.go:mqttDeliverQ2AffineCb#23", Disp: edgeExpand, To: "qos2_shared_affine.go:mqttDeliverQ2AffineCb"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "the local mapping itself - the defeated indeterminate guard - accounted on axis A"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},
		},
	})

	// ---- qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy -------------------
	//
	// The file header at qos2_shared_affine.go:46 says "Exactly one store can succeed, whatever
	// the dedup window is doing." F-110 records that this is false under the pinned server's
	// admission check; this row records the other half, which is that the COPY PRESENCE read
	// this file uses to resolve its store-succeeded/response-lost window cannot distinguish a
	// failed read from an absent copy either.
	prCensusRegister("qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "mqttQ2AffCopyAbsent, which gates settleSourcelessQ2AffClaim's DISCARD transition " +
			"(:4470). The mqttQ2AffCopyUnknown arm exists precisely so a failed load makes the " +
			"caller fail closed - its own const comment says \"load failed; the caller must fail " +
			"closed\" - and that arm cannot be reached for a store-read failure",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachExperiment,
		Repair: "chapter P's. The outstanding premise here is not a CAS but an authoritative " +
			"presence observation: the subject names exactly one claim, which is what the file " +
			"says makes the lost-response window resolvable without a dedup window, and F-109 " +
			"removes that resolvability. The discard consequence is SUSPECTED, with the " +
			"consequential caller named rather than asserted",
		Edges: []prCensusEdge{
			{Key: "CALLER orphan_qos2_sweep.go:(mqttAccountSessionManager).settleRetiredAffineRegister#24", Disp: edgeExpand, To: "orphan_qos2_sweep.go:(mqttAccountSessionManager).settleRetiredAffineRegister"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).reapQ2AffClaimChecked#9", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).reapQ2AffClaimChecked"},
			{Key: "CALLER qos2_shared_affine.go:(mqttSession).moveStartedSharedQoS2#37", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttSession).moveStartedSharedQoS2"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffCopy#3", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffCopy"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffCopy#6", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffCopy"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).recoverQ2AffClaims#6", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).recoverQ2AffClaims"},
			{Key: "CALLER qos2_shared_affine.go:(mqttAccountSessionManager).settleSourcelessQ2AffClaim#1", Disp: edgeExpand, To: "qos2_shared_affine.go:(mqttAccountSessionManager).settleSourcelessQ2AffClaim"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "LOCK"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "LOCK"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "SEAM"},
			{Key: "CALL#3", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#4", Disp: edgeDischarge,
				Why: "the local mapping itself - the defeated indeterminate guard, here spelled as " +
					"the mqttQ2AffCopyUnknown arm - accounted on axis A"},
		},
	})

	prFormerFrontierCallers("qos2_shared_affine.go:(mqttJSA).loadQ2AffClaim",
		"an (claim, seq, err) triple in which (nil, 0, nil) asserts that no affine claim stands, which a failed store read also produces",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).forgetIfQ2AffClaimGone#0",
		"qos2_shared_affine.go:(mqttAccountSessionManager).q2AffClaimForCarriedCopy#2",
		"qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffClaim#3",
		"qos2_shared_affine.go:(mqttAccountSessionManager).advanceQ2AffClaimChecked#0",
		"qos2_shared_affine.go:(mqttAccountSessionManager).reapQ2AffClaimChecked#0",
		"qos2_shared_affine.go:(mqttSession).moveStartedSharedQoS2#28",
		"qos2_shared_affine.go:(mqttSession).completeQ2AffOwnClaim#1",
		"qos2_shared_affine.go:(mqttAccountSessionManager).q2AffCopyIsDisowned#1",
		"qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffOnBehalf#26",
		"qos2_shared_affine.go:mqttDeliverQ2AffineCb#23",
	)
	prFormerFrontierCallers("qos2_shared_affine.go:(mqttAccountSessionManager).loadQ2AffCopy",
		"an mqttQ2AffCopyState in which mqttQ2AffCopyAbsent asserts that the claim's copy subject is empty; the mqttQ2AffCopyUnknown arm that would have made the caller fail closed cannot be reached for a store-read failure",
		"orphan_qos2_sweep.go:(mqttAccountSessionManager).settleRetiredAffineRegister#24",
		"qos2_shared_affine.go:(mqttAccountSessionManager).reapQ2AffClaimChecked#9",
		"qos2_shared_affine.go:(mqttSession).moveStartedSharedQoS2#37",
		"qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffCopy#3",
		"qos2_shared_affine.go:(mqttAccountSessionManager).materializeQ2AffCopy#6",
		"qos2_shared_affine.go:(mqttAccountSessionManager).recoverQ2AffClaims#6",
		"qos2_shared_affine.go:(mqttAccountSessionManager).settleSourcelessQ2AffClaim#1",
	)
}

// The F-105 outbound substrate's loaders. Every one of them is a LIVE READ whose downstream
// duty is unavailable at HEAD: no production path admits an obligation through this substrate,
// but the reads themselves run - gatherAdoptedReleaseFacts is called by finishAdoptedTeardown on
// production paths and reads the closure, the ledger and the completion certificate before its
// vacuous return. Consult C1 required that distinction to have its own reachability value rather
// than being filed as withheld, because calling a live read withheld hides it.
//
// They share one shape and one defect: an isErrorOtherThan guard that a store-read failure
// cannot reach, because the failure arrives already spelled JSNoMessageFoundErr. The repetition
// is the finding, which is why each row states what ITS absence authorizes rather than deferring
// to a common sentence.
func init() {
	// ---- outbound_affine_readiness.go:(mqttJSA).loadAffineReadiness
	prCensusRegister("outbound_affine_readiness.go:(mqttJSA).loadAffineReadiness", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a generation's affine readiness assertion as ABSENT, which its own comment " +
			"says would refuse a legal provisioning seal while admitting nothing in its place. " +
			"The site is careful about the OTHER direction - an unreadable body is an error " +
			"rather than an absence - and F-109 defeats the guard that distinction rests on",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "an exact-sequence re-attempt is not available: the assertion is written create-only " +
			"(publishAffineReadiness), so the absence question and the write settlement share " +
			"F-110's premise. No consequence is identified at HEAD because no production path " +
			"admits an obligation through this substrate, which is what the reachability value " +
			"records",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_affine_readiness.go:(mqttJSA).publishAffineReadiness#7", Disp: edgeExpand, To: "outbound_affine_readiness.go:(mqttJSA).publishAffineReadiness"},
			{Key: "CALLER outbound_duty_exec.go:(mqttAccountSessionManager).advanceAffinePrerequisites#4", Disp: edgeExpand, To: "outbound_duty_exec.go:(mqttAccountSessionManager).advanceAffinePrerequisites"},
			{Key: "CALLER outbound_transition.go:(mqttJSA).checkResourceOverwrites#13", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).checkResourceOverwrites"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :137 mqttReadinessSubject
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :137 getHash
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :141
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :148 mqttDecodeAffineReadiness
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :150
		},
	})

	prFormerFrontierCallers("outbound_affine_readiness.go:(mqttJSA).loadAffineReadiness",
		"a (readiness, error) pair in which (nil, nil) asserts that no readiness assertion "+
			"stands for the generation, which a failed store read also produces",
		"outbound_affine_readiness.go:(mqttJSA).publishAffineReadiness#7",
		"outbound_duty_exec.go:(mqttAccountSessionManager).advanceAffinePrerequisites#4",
	)

	// ---- outbound_directory.go:(mqttJSA).loadDirShardAt
	prCensusRegister("outbound_directory.go:(mqttJSA).loadDirShardAt", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a directory shard header as absent. The file states that ABSENT is a " +
			"POSITIVE answer there - nothing starts a traversal over that shard, so no generation " +
			"in it is enrolled however many root records exist - and that is exactly the reading " +
			"a failed read now also produces",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the shard header is written under an exact-sequence CAS by the traversal that " +
			"advances it, so the WRITE side has a candidate; the READ side's absent arm has no " +
			"sequence to name and falls back to create-only, inheriting F-110. No consequence is " +
			"identified at HEAD",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_close_enrol.go:(mqttJSA).opCloseGenerationWithEnrolment#4", Disp: edgeExpand, To: "outbound_close_enrol.go:(mqttJSA).opCloseGenerationWithEnrolment"},
			{Key: "CALLER outbound_enrolment_boundary.go:(mqttJSA).checkRecoveryEnrolment#25", Disp: edgeExpand, To: "outbound_enrolment_boundary.go:(mqttJSA).checkRecoveryEnrolment"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opFinalizeHandoff#4", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opFinalizeHandoff"},
			{Key: "CALLER outbound_recovery_pass.go:(mqttAccountSessionManager).sweepOutboundRecovery#6", Disp: edgeExpand, To: "outbound_recovery_pass.go:(mqttAccountSessionManager).sweepOutboundRecovery"},
			{Key: "CALLER outbound_recovery_pass.go:(mqttAccountSessionManager).walkRecoveryShard#8", Disp: edgeExpand, To: "outbound_recovery_pass.go:(mqttAccountSessionManager).walkRecoveryShard"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :398 mqttShardSubject, the read's own argument
			{Key: "CALL#2", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :401
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :408 mqttDecodeDirShard
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :410
		},
	})

	prFormerFrontierCallers("outbound_directory.go:(mqttJSA).loadDirShardAt",
		"a (shard, seq, error) triple in which (nil, 0, nil) asserts that no header stands "+
			"for the shard, which a failed store read also produces",
		"outbound_close_enrol.go:(mqttJSA).opCloseGenerationWithEnrolment#4",
		"outbound_enrolment_boundary.go:(mqttJSA).checkRecoveryEnrolment#25",
		"outbound_ops.go:(mqttJSA).opFinalizeHandoff#4",
		"outbound_recovery_pass.go:(mqttAccountSessionManager).sweepOutboundRecovery#6",
		"outbound_recovery_pass.go:(mqttAccountSessionManager).walkRecoveryShard#8",
	)

	// ---- outbound_directory.go:(mqttJSA).loadRecoveryRoot
	prCensusRegister("outbound_directory.go:(mqttJSA).loadRecoveryRoot", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating an enrolled generation's recovery root as absent, which the file says would " +
			"DROP a generation with outstanding duties out of every future sweep. The site " +
			"distinguishes absence from unreadability deliberately, and F-109 collapses that " +
			"distinction for a failed read",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "none available as written: the root is allocated create-only inside the closing " +
			"transition's atomic batch, so a settlement re-attempt inherits F-110. The candidate " +
			"with the best standing is the closure's RECORDED root address, which the recovery " +
			"pass already reads exactly rather than by enumeration - its premise is that the " +
			"closure itself was readable",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_recovery_pass.go:(mqttAccountSessionManager).walkRecoveryShard#3", Disp: edgeExpand, To: "outbound_recovery_pass.go:(mqttAccountSessionManager).walkRecoveryShard"},
			{Key: "CALLER outbound_release_gate.go:(mqttAccountSessionManager).recoveryRootReachable#4", Disp: edgeExpand, To: "outbound_release_gate.go:(mqttAccountSessionManager).recoveryRootReachable"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :781 mqttRootSubject, the read's own argument
			{Key: "CALL#2", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :784
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :791 mqttDecodeRecoveryRoot
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :796
		},
	})

	prFormerFrontierCallers("outbound_directory.go:(mqttJSA).loadRecoveryRoot",
		"a (root, seq, error) triple in which (nil, 0, nil) asserts that nothing is enrolled "+
			"at that ordinal, which a failed store read also produces",
		"outbound_recovery_pass.go:(mqttAccountSessionManager).walkRecoveryShard#3",
		"outbound_release_gate.go:(mqttAccountSessionManager).recoveryRootReachable#4",
	)

	// ---- outbound_duty_ops.go:(mqttJSA).loadDutyEntryAt
	prCensusRegister("outbound_duty_ops.go:(mqttJSA).loadDutyEntryAt", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a duty entry as absent. The file is explicit that a continuation over an " +
			"absent entry would INVENT the duty it claims to advance, which is why an entry it " +
			"cannot interpret is an error rather than an absence - and a failed read is not an " +
			"entry it cannot interpret, it is one it never saw",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the write side is exact-sequence by construction, since the function returns the " +
			"sequence the entry stands at so a continuation CASes against what it read. The " +
			"absent arm has no sequence, so the repair it needs is an authoritative absence " +
			"rather than a CAS",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_duty_ops.go:(mqttJSA).opResolveDuty#2", Disp: edgeExpand, To: "outbound_duty_ops.go:(mqttJSA).opResolveDuty"},
			{Key: "CALLER outbound_duty_ops.go:(mqttJSA).completeCleanupAndResolve#6", Disp: edgeExpand, To: "outbound_duty_ops.go:(mqttJSA).completeCleanupAndResolve"},
			{Key: "CALLER outbound_duty_ops.go:(mqttJSA).cleanupDutyStandsFor#2", Disp: edgeExpand, To: "outbound_duty_ops.go:(mqttJSA).cleanupDutyStandsFor"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :30 mqttDutySubject
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :30 getHash
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :34
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :41 mqttDecodeDutyEntry
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :43
		},
	})

	prFormerFrontierCallers("outbound_duty_ops.go:(mqttJSA).loadDutyEntryAt",
		"a (entry, seq, error) triple in which (nil, 0, nil) asserts that no duty entry "+
			"stands at that ordinal, which a failed store read also produces",
		"outbound_duty_ops.go:(mqttJSA).opResolveDuty#2",
		"outbound_duty_ops.go:(mqttJSA).completeCleanupAndResolve#6",
		"outbound_duty_ops.go:(mqttJSA).cleanupDutyStandsFor#2",
	)

	// ---- outbound_inventory_boundary.go:(mqttJSA).loadGenLedger
	prCensusRegister("outbound_inventory_boundary.go:(mqttJSA).loadGenLedger", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a generation's inventory ledger as absent. The file says ABSENT is a " +
			"POSITIVE answer here - a generation that has allocated nothing has no ledger, and " +
			"its first allocation creates one create-only - so a failed read licenses a first " +
			"allocation over a ledger that may already carry a high-water",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "this row is where F-110 and F-109 meet most directly: the absent reading authorizes " +
			"a CREATE-ONLY allocation, and F-110 shows that create-only CAS does not establish " +
			"the subject was empty. Neither finding repairs the other, and a candidate must " +
			"satisfy both",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_close_enrol.go:(mqttJSA).opCloseGenerationWithEnrolment#3", Disp: edgeExpand, To: "outbound_close_enrol.go:(mqttJSA).opCloseGenerationWithEnrolment"},
			{Key: "CALLER outbound_duty_exec.go:(mqttAccountSessionManager).concludeRecoveryRoot#1", Disp: edgeExpand, To: "outbound_duty_exec.go:(mqttAccountSessionManager).concludeRecoveryRoot"},
			{Key: "CALLER outbound_duty_ops.go:(mqttJSA).cleanupDutyStandsFor#0", Disp: edgeExpand, To: "outbound_duty_ops.go:(mqttJSA).cleanupDutyStandsFor"},
			{Key: "CALLER outbound_inventory_boundary.go:(mqttJSA).checkInventoryAllocation#8", Disp: edgeExpand, To: "outbound_inventory_boundary.go:(mqttJSA).checkInventoryAllocation"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opFinalizeHandoff#3", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opFinalizeHandoff"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opAdmitPublish#8", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opAdmitPublish"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opProvisionResource#5", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opProvisionResource"},
			{Key: "CALLER outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts#2", Disp: edgeExpand, To: "outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts"},
			{Key: "CALLER sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration#2", Disp: edgeExpand, To: "sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :622 mqttGenLedgerSubject
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :622 getHash
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :626
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :633 mqttDecodeGenLedger
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :635
		},
	})

	prFormerFrontierCallers("outbound_inventory_boundary.go:(mqttJSA).loadGenLedger",
		"a (ledger, seq, error) triple in which (nil, 0, nil) asserts that the generation has "+
			"no inventory ledger, which a failed store read also produces",
		"outbound_close_enrol.go:(mqttJSA).opCloseGenerationWithEnrolment#3",
		"outbound_duty_exec.go:(mqttAccountSessionManager).concludeRecoveryRoot#1",
		"outbound_duty_ops.go:(mqttJSA).cleanupDutyStandsFor#0",
		"outbound_inventory_boundary.go:(mqttJSA).checkInventoryAllocation#8",
		"outbound_ops.go:(mqttJSA).opFinalizeHandoff#3",
		"outbound_ops.go:(mqttJSA).opAdmitPublish#8",
		"outbound_ops.go:(mqttJSA).opProvisionResource#5",
		"outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts#2",
		"sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration#2",
	)

	// ---- outbound_release_gate.go:(mqttJSA).loadDirShard
	prCensusRegister("outbound_release_gate.go:(mqttJSA).loadDirShard", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a directory shard as absent inside the RELEASE GATE, whose whole purpose is " +
			"to refuse releasing a generation that still owes work",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "as for loadDirShardAt, with the gate's own stated rule as the outstanding premise: " +
			"gatherAdoptedReleaseFacts says AN UNRESOLVED READ IS AN ERROR, never an absence, and " +
			"a repair must make that true rather than restate it",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_release_gate.go:(mqttAccountSessionManager).recoveryRootReachable#3", Disp: edgeExpand, To: "outbound_release_gate.go:(mqttAccountSessionManager).recoveryRootReachable"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :480 mqttShardSubject
			{Key: "CALL#2", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :483
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :490 mqttDecodeDirShard
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :492
		},
	})

	prFormerFrontierCallers("outbound_release_gate.go:(mqttJSA).loadDirShard",
		"a (shard, error) pair in which (nil, nil) asserts that no header stands for the "+
			"shard, which a failed store read also produces",
		"outbound_release_gate.go:(mqttAccountSessionManager).recoveryRootReachable#3",
	)

	// ---- outbound_release_gate.go:(mqttJSA).loadGenCompletionCert
	prCensusRegister("outbound_release_gate.go:(mqttJSA).loadGenCompletionCert", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating a generation's completion certificate as absent. The file keeps absence and " +
			"unreadability apart on purpose - absent means no certificate stands, which simply " +
			"fails to satisfy recovery ownership, while a certificate it cannot interpret is a " +
			"refusal - and says both lead to different halves of the same gate",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the certificate is published create-only and settled by reading the same subject " +
			"back, so both the write settlement and this read share the F-110 premise. An exact- " +
			"sequence re-attempt exists only for a caller that already saw the certificate stand",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_duty_ops.go:(mqttJSA).publishGenCompletionCert#7", Disp: edgeExpand, To: "outbound_duty_ops.go:(mqttJSA).publishGenCompletionCert"},
			{Key: "CALLER outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts#5", Disp: edgeExpand, To: "outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :499 mqttGenCertSubject
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :499 getHash
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-WRAP"},      // :503
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :510 mqttJSONMembersAreExactAndUnique
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :512
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :523 mqttJSONMembersArePresent
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :525
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :528 json.Unmarshal
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :531 mqttValidateGenCert
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :532
		},
	})

	prFormerFrontierCallers("outbound_release_gate.go:(mqttJSA).loadGenCompletionCert",
		"a (cert, error) pair in which (nil, nil) asserts that no completion certificate "+
			"stands for the generation, which a failed store read also produces",
		"outbound_duty_ops.go:(mqttJSA).publishGenCompletionCert#7",
		"outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts#5",
	)

}

// The outbound substrate's non-uniform rows: two shapes that the loader family above does not
// cover. The WITHHELD ones refuse on every read error, and are recorded because consult C1
// refuted the idea that refusing makes a node terminal. The ABSENT-IS-INTENDED ones read a 404
// as confirmation of something they were trying to achieve, and C1 ruled that the intent does
// not make the evidence sound - which is why `absent-is-success` is not a value on any axis here.
func init() {
	// ---- outbound_ops.go:(mqttJSA).loadOutboundExchange
	// Consult C1's own witness for why a fail-closed site is not terminal. It references
	// JSNoMessageFoundErr nowhere and handles every read error in one arm, and it still keeps the
	// distinction available downstream through %w.
	prCensusRegister("outbound_ops.go:(mqttJSA).loadOutboundExchange", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth: "nothing: every read error, 404 included, is returned as an error. A genuine absence " +
			"is refused here rather than reported, so no absence-derived fact leaves this node",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLiveNoDuty,
		Repair: "no repair is owed for a FALSE ABSENCE at this site, because it withholds on EVERY " +
			"read error rather than concluding emptiness from one. It is in the census because " +
			"withholding is a mapping value and not a terminal: consult C1 refuted the shortcut " +
			"that a fail-closed site needs no further accounting, using this very file's " +
			"loadOutboundExchange, which handles every read error in one arm and still carries " +
			"the distinction downstream by wrapping the original with %w. What the shared arm " +
			"costs is AVAILABILITY - a genuine absence is refused alongside a failed read - and a " +
			"repair that separated them would have to supply the trustworthy absence F-109 denies",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_continuation.go:(mqttJSA).classifyExchangeDuty#0", Disp: edgeExpand, To: "outbound_continuation.go:(mqttJSA).classifyExchangeDuty"},
			{Key: "CALLER outbound_inventory_boundary.go:(mqttJSA).checkDutyEndingIsEarned#0", Disp: edgeExpand, To: "outbound_inventory_boundary.go:(mqttJSA).checkDutyEndingIsEarned"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opAdvanceExchange#2", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opAdvanceExchange"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opAcquireAction#6", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opAcquireAction"},
			{Key: "CALLER outbound_replace_register.go:(mqttJSA).checkReplacementPredecessorObligation#0", Disp: edgeExpand, To: "outbound_replace_register.go:(mqttJSA).checkReplacementPredecessorObligation"},
			{Key: "CALLER outbound_withdraw_unsent.go:(mqttJSA).opWithdrawUnsent#4", Disp: edgeExpand, To: "outbound_withdraw_unsent.go:(mqttJSA).opWithdrawUnsent"},
			{Key: "CALLER outbound_withdraw_unsent.go:(mqttSession).endUnsentBeforeDestroy#2", Disp: edgeExpand, To: "outbound_withdraw_unsent.go:(mqttSession).endUnsentBeforeDestroy"},
			{Key: "CALLER outbound_withdraw_unsent.go:(mqttSession).withdrawIfEvidenced#1", Disp: edgeExpand, To: "outbound_withdraw_unsent.go:(mqttSession).withdrawIfEvidenced"},
			{Key: "CALLER outbound_abandon_delivery.go:(mqttJSA).opAbandonDeniedDelivery#6", Disp: edgeExpand, To: "outbound_abandon_delivery.go:(mqttJSA).opAbandonDeniedDelivery"},
			{Key: "CALLER outbound_custody_session.go:(mqttSession).enumerateCustodyExchanges#12", Disp: edgeExpand, To: "outbound_custody_session.go:(mqttSession).enumerateCustodyExchanges"},
			{Key: "CALLER outbound_custody_session.go:(mqttSession).observeCustodyTarget#3", Disp: edgeExpand, To: "outbound_custody_session.go:(mqttSession).observeCustodyTarget"},
			{Key: "CALLER outbound_custody_session.go:(mqttSession).readCustodySubject#3", Disp: edgeExpand, To: "outbound_custody_session.go:(mqttSession).readCustodySubject"},
			{Key: "CALLER inflight_register.go:(mqttSession).recoverInflightRegistersOnce#44", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).recoverInflightRegistersOnce"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // mqttExchangeSubject
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // getHash
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :89
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :91 mqttDecodeOutboundExchange
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :93
		},
	})

	prFormerFrontierCallers("outbound_ops.go:(mqttJSA).loadOutboundExchange",
		"an error wrapping the original read failure with %w on every failed arm, so the "+
			"caller can still distinguish what this node did not - which is exactly why consult "+
			"C1 refused to treat a shared arm as a terminal",
		"outbound_continuation.go:(mqttJSA).classifyExchangeDuty#0",
		"outbound_inventory_boundary.go:(mqttJSA).checkDutyEndingIsEarned#0",
		"outbound_ops.go:(mqttJSA).opAdvanceExchange#2",
		"outbound_ops.go:(mqttJSA).opAcquireAction#6",
		"outbound_replace_register.go:(mqttJSA).checkReplacementPredecessorObligation#0",
	)

	// ---- outbound_ops.go:(mqttJSA).loadOutboundResource
	prCensusRegister("outbound_ops.go:(mqttJSA).loadOutboundResource", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth:    "nothing: every read error, 404 included, is returned as an error",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLiveNoDuty,
		Repair: "no repair is owed for a FALSE ABSENCE at this site, because it withholds on EVERY " +
			"read error rather than concluding emptiness from one. It is in the census because " +
			"withholding is a mapping value and not a terminal: consult C1 refuted the shortcut " +
			"that a fail-closed site needs no further accounting, using this very file's " +
			"loadOutboundExchange, which handles every read error in one arm and still carries " +
			"the distinction downstream by wrapping the original with %w. What the shared arm " +
			"costs is AVAILABILITY - a genuine absence is refused alongside a failed read - and a " +
			"repair that separated them would have to supply the trustworthy absence F-109 denies",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_continuation.go:(mqttJSA).classifyResourceDuty#0", Disp: edgeExpand, To: "outbound_continuation.go:(mqttJSA).classifyResourceDuty"},
			{Key: "CALLER outbound_duty_exec.go:(mqttAccountSessionManager).performRecoveryWork#1", Disp: edgeExpand, To: "outbound_duty_exec.go:(mqttAccountSessionManager).performRecoveryWork"},
			{Key: "CALLER outbound_duty_exec.go:(mqttAccountSessionManager).advanceAffinePrerequisites#1", Disp: edgeExpand, To: "outbound_duty_exec.go:(mqttAccountSessionManager).advanceAffinePrerequisites"},
			{Key: "CALLER outbound_duty_exec.go:(mqttAccountSessionManager).transferredCleanupKind#0", Disp: edgeExpand, To: "outbound_duty_exec.go:(mqttAccountSessionManager).transferredCleanupKind"},
			{Key: "CALLER outbound_inventory_boundary.go:(mqttJSA).checkDutyEndingIsEarned#5", Disp: edgeExpand, To: "outbound_inventory_boundary.go:(mqttJSA).checkDutyEndingIsEarned"},
			{Key: "CALLER outbound_inventory_boundary.go:(mqttJSA).resourceAsBatchLeavesIt#4", Disp: edgeExpand, To: "outbound_inventory_boundary.go:(mqttJSA).resourceAsBatchLeavesIt"},
			{Key: "CALLER outbound_manifest.go:(mqttJSA).checkManifestOwnerExists#4", Disp: edgeExpand, To: "outbound_manifest.go:(mqttJSA).checkManifestOwnerExists"},
			{Key: "CALLER outbound_material_delete.go:(mqttJSA).checkPurgeIsAuthorized#0", Disp: edgeExpand, To: "outbound_material_delete.go:(mqttJSA).checkPurgeIsAuthorized"},
			{Key: "CALLER outbound_material_delete.go:(mqttAccountSessionManager).purgeMaterialStep#1", Disp: edgeExpand, To: "outbound_material_delete.go:(mqttAccountSessionManager).purgeMaterialStep"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opAdmitPublish#6", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opAdmitPublish"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opReleaseDependency#2", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opReleaseDependency"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opSealProvisioning#2", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opSealProvisioning"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opAuthorizeDeletion#2", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opAuthorizeDeletion"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).opCompleteCleanup#2", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).opCompleteCleanup"},
			{Key: "CALLER sess_teardown_partition.go:(mqttAccountSessionManager).finishAffinePrerequisites#2", Disp: edgeExpand, To: "sess_teardown_partition.go:(mqttAccountSessionManager).finishAffinePrerequisites"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // mqttResourceSubject
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // getHash
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :102
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :104 mqttDecodeOutboundResource
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :106
		},
	})

	prFormerFrontierCallers("outbound_ops.go:(mqttJSA).loadOutboundResource",
		"an error wrapping the original read failure with %w on every failed arm",
		"outbound_continuation.go:(mqttJSA).classifyResourceDuty#0",
		"outbound_duty_exec.go:(mqttAccountSessionManager).performRecoveryWork#1",
		"outbound_duty_exec.go:(mqttAccountSessionManager).advanceAffinePrerequisites#1",
		"outbound_duty_exec.go:(mqttAccountSessionManager).transferredCleanupKind#0",
		"outbound_inventory_boundary.go:(mqttJSA).checkDutyEndingIsEarned#5",
		"outbound_inventory_boundary.go:(mqttJSA).resourceAsBatchLeavesIt#4",
		"outbound_manifest.go:(mqttJSA).checkManifestOwnerExists#4",
		"outbound_material_delete.go:(mqttJSA).checkPurgeIsAuthorized#0",
		"outbound_material_delete.go:(mqttAccountSessionManager).purgeMaterialStep#1",
		"outbound_ops.go:(mqttJSA).opAdmitPublish#6",
		"outbound_ops.go:(mqttJSA).opReleaseDependency#2",
		"outbound_ops.go:(mqttJSA).opSealProvisioning#2",
		"outbound_ops.go:(mqttJSA).opAuthorizeDeletion#2",
		"outbound_ops.go:(mqttJSA).opCompleteCleanup#2",
		"sess_teardown_partition.go:(mqttAccountSessionManager).finishAffinePrerequisites#2",
	)

	// ---- outbound_material_verify.go:(mqttJSA).verifyMaterial
	prCensusRegister("outbound_material_verify.go:(mqttJSA).verifyMaterial", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth: "nothing: a chunk that could not be read is an error, and the verification refuses " +
			"rather than treating the chunk as missing",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLiveNoDuty,
		Repair: "no repair is owed for a FALSE ABSENCE at this site, because it withholds on EVERY " +
			"read error rather than concluding emptiness from one. It is in the census because " +
			"withholding is a mapping value and not a terminal: consult C1 refuted the shortcut " +
			"that a fail-closed site needs no further accounting, using this very file's " +
			"loadOutboundExchange, which handles every read error in one arm and still carries " +
			"the distinction downstream by wrapping the original with %w. What the shared arm " +
			"costs is AVAILABILITY - a genuine absence is refused alongside a failed read - and a " +
			"repair that separated them would have to supply the trustworthy absence F-109 denies",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_material_verify.go:(mqttJSA).checkSealVerifiesItsMaterial#1", Disp: edgeExpand, To: "outbound_material_verify.go:(mqttJSA).checkSealVerifiesItsMaterial"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :79 fmt.Errorf
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :81 getHash
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"}, // :82 make
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"}, // :83 uint64
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"}, // :84 mqttChunkSubject
			{Key: "CALL#5", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-WRAP"},      // :89
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :94 mqttDecodeRetainedChunk
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :96
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :98 append
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :100 uint64
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :100 len
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :101
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :102 len
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :104 mqttMaterialDigest
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :105
			{Key: "MUT#3", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
		},
	})

	prFormerFrontierCallers("outbound_material_verify.go:(mqttJSA).verifyMaterial",
		"an error naming the chunk that could not be read, wrapping the original",
		"outbound_material_verify.go:(mqttJSA).checkSealVerifiesItsMaterial#1",
	)

	// ---- outbound_material_delete.go:(mqttJSA).checkPurgeConfirmed
	prCensusRegister("outbound_material_delete.go:(mqttJSA).checkPurgeConfirmed", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "that absence CONFIRMS removal: the chunk is accepted as purged, which permits " +
			"advancing the payload to Purged and supports the eventual manifest cleanup that " +
			"follows from it. The intent to delete is what makes this read look safe, and consult " +
			"C1 ruled explicitly that the intent does not make the evidence sound - a failed read " +
			"certifies a removal that may not have happened",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "a confirmed delete at an exact sequence is the shape this needs, and the site has " +
			"the sequence available from its own discovery step. The outstanding premise is the " +
			"one C1 named: the census must not record deletion INTENT as a safety property, so a " +
			"repair has to prove removal rather than infer it from a read that reported nothing",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_manifest.go:(mqttJSA).checkManifestAgainstStore#14", Disp: edgeExpand, To: "outbound_manifest.go:(mqttJSA).checkManifestAgainstStore"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :210 getHash
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :212 mqttChunkSubject
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :216
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :221 mqttDiscoverRetainedChunk
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :223
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :228
			{Key: "MUT#2", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
		},
	})

	prFormerFrontierCallers("outbound_material_delete.go:(mqttJSA).checkPurgeConfirmed",
		"a nil error meaning every chunk in the interval is confirmed removed, a confirmation "+
			"that a failed read also produces",
	)

	// ---- outbound_material_delete.go:(mqttAccountSessionManager).deleteChunkInterval
	prCensusRegister("outbound_material_delete.go:(mqttAccountSessionManager).deleteChunkInterval", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "skipping a chunk as already gone, so the interval's physical delete passes over an " +
			"address that may still hold material",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "as for checkPurgeConfirmed: the delete is by exact sequence once the chunk is " +
			"discovered, and the absent arm is the one with no sequence to name",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_material_delete.go:(mqttAccountSessionManager).purgeMaterialStep#5", Disp: edgeExpand, To: "outbound_material_delete.go:(mqttAccountSessionManager).purgeMaterialStep"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :404 getHash
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :406 mqttChunkSubject
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :410
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :418 mqttDiscoverRetainedChunk
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :420
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :429 as.jsa.deleteMsg
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :430
			{Key: "MUT#2", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
		},
	})

	prFormerFrontierCallers("outbound_material_delete.go:(mqttAccountSessionManager).deleteChunkInterval",
		"a nil error meaning the interval was deleted, which is also produced when a chunk's "+
			"read failed and was skipped",
		"outbound_material_delete.go:(mqttAccountSessionManager).purgeMaterialStep#5",
	)

	// ---- outbound_ops.go:(mqttJSA).opCommitMetadata
	prCensusRegister("outbound_ops.go:(mqttJSA).opCommitMetadata", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "a base sequence of ZERO for the predecessor session record - the metadata half then " +
			"names nothing as what it displaces. The site refuses on a non-404 error, saying the " +
			"record \"could not be read, so this metadata half cannot name what it displaces\"; a " +
			"store-read failure reaches the other arm and supplies zero instead",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "an exact-sequence candidate exists whenever a predecessor record was in fact read, " +
			"and the gap is precisely the arm that claims there was none. The outstanding premise " +
			"is that a zero base is also legitimate for a client with no prior record, so a " +
			"repair must distinguish the two rather than refuse both",
		Edges: []prCensusEdge{
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :451 fmt.Errorf
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :453 jsa.requireAuthority
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"}, // :458 fmt.Errorf
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"}, // :465 fmt.Errorf
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"}, // :468 mqttProposalFromCreation
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"}, // :471 fmt.Errorf
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "PRE-READ"}, // :474 mqttProposalFromHandoff
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRE-READ"}, // :476 fmt.Errorf
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "PRE-READ"}, // :480 fmt.Errorf
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRE-READ"}, // :485 uint64
			{Key: "CALL#10", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // mqttSessSubject
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // getHash
			{Key: "CALL#13", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "ERR-WRAP"},              // :489
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "CONTINUES-PAST-ABSENT"}, // :495 json.Marshal
			{Key: "CALL#16", Disp: edgeDischarge, Exp: "CONTINUES-PAST-ABSENT"}, // :502 json.Marshal
			{Key: "CALL#17", Disp: edgeExpand,
				To: "handoff_metadata.go:mqttFreezeMetadataAttempt",
				Why: "the absent arm does NOT return: it leaves baseSeq at zero and carries that zero " +
					"straight into the frozen attempt, which is what the metadata half names as the " +
					"record it displaces"}, // :506 mqttFreezeMetadataAttempt
			{Key: "CALL#18", Disp: edgeDischarge, Exp: "CONTINUES-PAST-ABSENT"}, // :507 mqttAcceptSubject
			{Key: "CALL#19", Disp: edgeDischarge, Exp: "CONTINUES-PAST-ABSENT"}, // :507 getHash
			{Key: "CALL#20", Disp: edgeExpand,
				To: "handoff_metadata.go:(mqttJSA).commitMetadataAttempt",
				Why: "the commit of that frozen attempt, reached on the absent arm with a zero base " +
					"sequence. This is the consequential write the false absence reaches"}, // :514 jsa.commitMetadataAttempt
			{Key: "MUT#12", Disp: edgeDischarge, Exp: "LOCAL-RECORD-FIELD", Dests: 352},
			{Key: "MUT#13", Disp: edgeDischarge, Exp: "LOCAL-RECORD-FIELD", Dests: 399},
			{Key: "MUT#14", Disp: edgeDischarge, Exp: "LOCAL-RECORD-FIELD", Dests: 19},
		},
	})

	// ---- outbound_manifest.go:(mqttJSA).checkManifestAgainstStore
	prCensusRegister("outbound_manifest.go:(mqttJSA).checkManifestAgainstStore", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "treating the standing manifest as ABSENT, which the file says is \"a POSITIVE answer " +
			"meaning this batch RESERVES the materialization create-only, with nothing written " +
			"and its cleanup owner already named\". So a failed read licenses a create-only " +
			"reservation over a manifest that may stand - which is also where this row meets " +
			"F-110, since that reservation's own admission check is the one F-110 faults",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "neither half is available alone. An exact-sequence CAS is what the present path " +
			"already uses - the fence is a manifest CAS - and the absent path has no sequence; a " +
			"create-only reservation inherits F-110. This is the clearest row in the census for " +
			"why closing F-109 does not close F-110 and vice versa",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_manifest.go:(mqttJSA).checkManifestMembers#21", Disp: edgeExpand, To: "outbound_manifest.go:(mqttJSA).checkManifestMembers"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "ERR-WRAP"},           // :579
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ABSENT-ARM-REFUSAL"}, // :596
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ABSENT-ARM-REFUSAL"}, // :605
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "ABSENT-ARM-REFUSAL"}, // :609
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ABSENT-ARM-REFUSAL"}, // :614
			{Key: "CALL#7", Disp: edgeExpand,
				To: "outbound_manifest.go:(mqttJSA).checkManifestOwnerExists",
				Why: "the TERMINAL call of the absent arm: having read the manifest as absent, this " +
					"batch reserves the materialization create-only and establishes its cleanup owner " +
					"here. A false absence therefore reaches a reservation over a manifest that may " +
					"stand, which is where this row meets F-110"}, // :629 jsa.checkManifestOwnerExists
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :632
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :636
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :640 mqttDecodeRetainedManifest
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :642
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :645 mqttCheckManifestStep
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :650 jsa.checkPurgeIsAuthorized
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :655 jsa.checkPurgeConfirmed
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :667 jsa.checkSealVerifiesItsMaterial
		},
	})

	prFormerFrontierCallers("outbound_manifest.go:(mqttJSA).checkManifestAgainstStore",
		"a nil error meaning the chunk fence is satisfied, reached either because the "+
			"manifest stood at the claimed sequence or because it was read as absent and the "+
			"batch reserves create-only",
		"outbound_manifest.go:(mqttJSA).checkManifestMembers#21",
	)

}

// The outbound transition boundary: three loaders that classify, three checks that withhold,
// and one that skips an unreadable exchange while deciding identifier exclusivity.
func init() {
	// ---- outbound_transition.go:(mqttJSA).loadOutboundAuthority
	prCensusRegister("outbound_transition.go:(mqttJSA).loadOutboundAuthority", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "mqttOutboundAbsent inside an mqttAuthorityLoad - a per-client outbound authority " +
			"reported as not existing. Consult C1 named this shape when it refuted the sentinel " +
			"design: the status travels inside a STRUCT, so a repair that follows only bare " +
			"identifier returns does not see it",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the authority is written under an exact-sequence CAS once it exists, so the write " +
			"side has a candidate; the absent arm is the bootstrap, and a create-only bootstrap " +
			"inherits F-110",
		Edges: []prCensusEdge{
			{Key: "CALLER handoff_metadata.go:(mqttJSA).checkProposalProvenance#0", Disp: edgeExpand, To: "handoff_metadata.go:(mqttJSA).checkProposalProvenance"},
			{Key: "CALLER outbound_duty_exec.go:(mqttAccountSessionManager).recoveryActorIsAuthorized#0", Disp: edgeExpand, To: "outbound_duty_exec.go:(mqttAccountSessionManager).recoveryActorIsAuthorized"},
			{Key: "CALLER outbound_material_delete.go:(mqttAccountSessionManager).stepPurgeCount#0", Disp: edgeExpand, To: "outbound_material_delete.go:(mqttAccountSessionManager).stepPurgeCount"},
			{Key: "CALLER outbound_ops.go:(mqttJSA).requireAuthority#0", Disp: edgeExpand, To: "outbound_ops.go:(mqttJSA).requireAuthority"},
			{Key: "CALLER outbound_transition.go:(mqttJSA).classifyAgainstOutcome#3", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).classifyAgainstOutcome"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :79 mqttOutboundAuthoritySubject
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :79 getHash
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :87 mqttDecodeOutboundAuthority
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).loadOutboundAuthority",
		"an mqttAuthorityLoad whose status is mqttOutboundAbsent, asserting that this client "+
			"has no outbound authority - which a failed store read also produces, since the "+
			"mqttOutboundIndeterminate arm cannot be reached for it",
		"handoff_metadata.go:(mqttJSA).checkProposalProvenance#0",
		"outbound_duty_exec.go:(mqttAccountSessionManager).recoveryActorIsAuthorized#0",
		"outbound_material_delete.go:(mqttAccountSessionManager).stepPurgeCount#0",
		"outbound_ops.go:(mqttJSA).requireAuthority#0",
		"outbound_transition.go:(mqttJSA).classifyAgainstOutcome#3",
	)

	// ---- outbound_transition.go:(mqttJSA).loadGenClosure
	// The guard that states the rule and is defeated by it. Its caller gatherAdoptedReleaseFacts
	// performs LIVE reads on a production path via finishAdoptedTeardown, which is why the
	// reachability value here is live-read-unavailable-duty rather than withheld.
	prCensusRegister("outbound_transition.go:(mqttJSA).loadGenClosure", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "mqttOutboundAbsent for a generation's closure record, which is what the release gate " +
			"reads. gatherAdoptedReleaseFacts states in capitals that AN UNRESOLVED READ IS AN " +
			"ERROR, never an absence, and explains that treating an unreadable closure as missing " +
			"would take the gate's vacuous path - releasing exactly the generation it exists to " +
			"protect. That guard is defeated here: the gate's premise is this function's " +
			"classification, and this function maps a 404 to absent",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "none available as written. The closure is written inside the closing transition's " +
			"atomic batch, so its settlement is create-only and inherits F-110, and what the gate " +
			"requires is an authoritative absence rather than a CAS. This is the census's " +
			"sharpest instance of a carefully written guard whose premise F-109 removes",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_close_enrol.go:(mqttJSA).closeGenerationEnrolled#0", Disp: edgeExpand, To: "outbound_close_enrol.go:(mqttJSA).closeGenerationEnrolled"},
			{Key: "CALLER outbound_duty_exec.go:(mqttAccountSessionManager).performAffineReadiness#1", Disp: edgeExpand, To: "outbound_duty_exec.go:(mqttAccountSessionManager).performAffineReadiness"},
			{Key: "CALLER outbound_inventory_boundary.go:(mqttJSA).checkInventoryAllocation#4", Disp: edgeExpand, To: "outbound_inventory_boundary.go:(mqttJSA).checkInventoryAllocation"},
			{Key: "CALLER outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts#0", Disp: edgeExpand, To: "outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts"},
			{Key: "CALLER outbound_transition.go:(mqttJSA).checkProposedGenerationIsFresh#0", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).checkProposedGenerationIsFresh"},
			{Key: "CALLER sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration#0", Disp: edgeExpand, To: "sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :101 mqttGenClosureSubject
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :101 getHash
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :109 mqttDecodeGenClosure
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).loadGenClosure",
		"an mqttOutboundStatus of mqttOutboundAbsent for the generation's closure, which the "+
			"release gate reads as no closure standing and which a failed store read also "+
			"produces",
		"outbound_close_enrol.go:(mqttJSA).closeGenerationEnrolled#0",
		"outbound_duty_exec.go:(mqttAccountSessionManager).performAffineReadiness#1",
		"outbound_inventory_boundary.go:(mqttJSA).checkInventoryAllocation#4",
		"outbound_release_gate.go:(mqttAccountSessionManager).gatherAdoptedReleaseFacts#0",
		"outbound_transition.go:(mqttJSA).checkProposedGenerationIsFresh#0",
		"sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration#0",
	)

	// ---- outbound_transition.go:(mqttJSA).loadTransitionOutcome
	prCensusRegister("outbound_transition.go:(mqttJSA).loadTransitionOutcome", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "mqttOutboundAbsent for a transition's recorded outcome, so a caller resuming after a " +
			"crash reads this transition left no outcome from a read that may simply have failed",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the outcome record is the evidence a restarted caller classifies itself against, so " +
			"what it needs is an authoritative absence rather than a CAS; an exact-sequence re- " +
			"attempt has nothing to name when the claim is that nothing was written",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_transition.go:(mqttJSA).classifyAgainstOutcome#1", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).classifyAgainstOutcome"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :118 mqttTransitionOutcomeSubject
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :118 getHash
			{Key: "CALL#2", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#3", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :126 mqttDecodeTransitionOutcome
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).loadTransitionOutcome",
		"an mqttOutboundStatus of mqttOutboundAbsent for the transition outcome, which a "+
			"failed store read also produces",
		"outbound_transition.go:(mqttJSA).classifyAgainstOutcome#1",
	)

	// ---- outbound_transition.go:(mqttJSA).checkExchangeOverwrites
	prCensusRegister("outbound_transition.go:(mqttJSA).checkExchangeOverwrites", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth: "nothing: an exchange the batch advances that could not be read is an error, and the " +
			"boundary refuses the whole transition",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLiveNoDuty,
		Repair: "no repair is owed for a FALSE ABSENCE at this site, because it withholds on EVERY " +
			"read error rather than concluding emptiness from one. It is recorded because consult " +
			"C1 refuted the shortcut that a fail-closed site needs no further accounting: " +
			"loadOutboundExchange shares one arm and still carries the distinction downstream by " +
			"wrapping the original with %w. The cost of the shared arm is AVAILABILITY, and " +
			"separating the arms would need the trustworthy absence F-109 denies",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_transition.go:(mqttJSA).commitAuthorityTransition#29", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).commitAuthorityTransition"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1582 mqttF105ObligationClass
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1590 strings.Split
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1590 len
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1590 mqttF105SubjectPrefix
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1591 len
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1592 fmt.Errorf
			{Key: "CALL#6", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "ERR-WRAP"},      // :1598
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :1602
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :1605 mqttDecodeOutboundExchange
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :1607
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :1610 mqttDecodeOutboundExchange
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :1612
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :1622
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :1625 mqttExchangePhaseSucceeds
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :1626
			// F-105 row 5 (consult 34): the DeliveryAbandoned branch's two refusals, reconciled BY
			// IDENTITY — they sit ahead of the withdrawal branch, so the node's pre-existing
			// unaccounted calls (#16-#19 at 7255a64) moved to #18-#21 and stay unaccounted as they were.
			{Key: "CALL#16", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // fmt.Errorf: only an abandon-delivery transition writes DeliveryAbandoned
			{Key: "CALL#17", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // fmt.Errorf: only a PUBLISH delivery can be abandoned
			{Key: "MUT#12", Disp: edgeDischarge, Exp: "LOCAL-RECORD-FIELD", Dests: 56},
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).checkExchangeOverwrites",
		"an error naming the exchange that could not be read",
		"outbound_transition.go:(mqttJSA).commitAuthorityTransition#29",
	)

	// ---- outbound_transition.go:(mqttJSA).checkActionMembers
	prCensusRegister("outbound_transition.go:(mqttJSA).checkActionMembers", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth:    "nothing: an exchange an acquired action names that could not be read is an error",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLiveNoDuty,
		Repair: "no repair is owed for a FALSE ABSENCE at this site, because it withholds on EVERY " +
			"read error rather than concluding emptiness from one. It is recorded because consult " +
			"C1 refuted the shortcut that a fail-closed site needs no further accounting: " +
			"loadOutboundExchange shares one arm and still carries the distinction downstream by " +
			"wrapping the original with %w. The cost of the shared arm is AVAILABILITY, and " +
			"separating the arms would need the trustworthy absence F-109 denies",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_transition.go:(mqttJSA).commitAuthorityTransition#31", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).commitAuthorityTransition"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2147 jsa.batchExchangeState
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2160 mqttF105ObligationClass
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2163 strings.Split
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2163 len
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2163 mqttF105SubjectPrefix
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2164 len
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2165 fmt.Errorf
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2168 strconv.ParseUint
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2170 fmt.Errorf
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRE-READ"},  // :2177 fmt.Errorf
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2182 fmt.Errorf
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2187 mqttDecodeOutboundAction
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2189 fmt.Errorf
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2197 fmt.Errorf
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2201 fmt.Errorf
			{Key: "CALL#15", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#16", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2208 mqttExchangeSubject
			{Key: "CALL#17", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2208 getHash
			{Key: "CALL#18", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :2210
			{Key: "CALL#19", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2213 mqttDecodeOutboundExchange
			{Key: "CALL#20", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :2215
			{Key: "CALL#21", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2220 mqttActionPermittedAtPhase
			{Key: "CALL#22", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :2221
			{Key: "CALL#23", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :2228
			{Key: "MUT#12", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).checkActionMembers",
		"an error naming the exchange that could not be read",
		"outbound_transition.go:(mqttJSA).commitAuthorityTransition#31",
	)

	// ---- outbound_transition.go:(mqttJSA).checkPredecessorAgainstStore
	prCensusRegister("outbound_transition.go:(mqttJSA).checkPredecessorAgainstStore", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth: "nothing: a predecessor authority that could not be read is an error, so the " +
			"transition refuses rather than treating the predecessor as absent",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLiveNoDuty,
		Repair: "no repair is owed for a FALSE ABSENCE at this site, because it withholds on EVERY " +
			"read error rather than concluding emptiness from one. It is recorded because consult " +
			"C1 refuted the shortcut that a fail-closed site needs no further accounting: " +
			"loadOutboundExchange shares one arm and still carries the distinction downstream by " +
			"wrapping the original with %w. The cost of the shared arm is AVAILABILITY, and " +
			"separating the arms would need the trustworthy absence F-109 denies",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_transition.go:(mqttJSA).commitAuthorityTransition#28", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).commitAuthorityTransition"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // mqttOutboundAuthoritySubject
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "an argument of the read call, evaluated before it; the higher occurrence number is " +
					"AST visit order, not evaluation order"}, // getHash
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-WRAP"},      // :2246
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :2250
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :2253 mqttDecodeOutboundAuthority
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :2255
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :2258 json.Marshal
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :2262 json.Marshal
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :2266 bytes.Equal
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :2267
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).checkPredecessorAgainstStore",
		"an error naming the predecessor authority that could not be read",
		"outbound_transition.go:(mqttJSA).commitAuthorityTransition#28",
	)

	// ---- outbound_transition.go:(mqttJSA).checkIdentifierExclusivity
	prCensusRegister("outbound_transition.go:(mqttJSA).checkIdentifierExclusivity", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "skipping an exchange whose record was not read while deciding whether a packet " +
			"identifier is EXCLUSIVELY held. An identifier that looks unclaimed because its " +
			"exchange could not be read is the failure mode this check exists to prevent",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the exclusivity decision is taken over an enumeration, so a repair here needs " +
			"F-108's completeness as well as this row's absence: the walk must not omit an " +
			"eligible exchange AND the per-subject read must not report a failure as nothing. " +
			"Neither finding closes the other",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_transition.go:(mqttJSA).commitAuthorityTransition#32", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).commitAuthorityTransition"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1962 jsa.batchExchangeState
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1969 mqttF105ObligationClass
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1973 strings.Split
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1973 len
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1973 mqttF105SubjectPrefix
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1974 len
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1978 mqttExchangePhaseEnded
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1981 strconv.FormatUint
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1981 uint64
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRE-READ"},  // :1985 len
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1989 getHash
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1991 jsa.streamSubjectsComplete
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1992 mqttExchangeWalkFilter
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRE-READ"}, // :1999 fmt.Errorf
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "this isErrorOtherThan belongs to the ENUMERATION above the read - the " +
					"streamSubjectsComplete walk - and not to this node's point read. It is F-108's " +
					"question rather than F-109's, and conflating the two is what the boundary rule in " +
					"the plan exists to prevent"},
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2005 fmt.Errorf
			{Key: "CALL#16", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2011 strings.Split
			{Key: "CALL#17", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2011 len
			{Key: "CALL#18", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2011 mqttF105SubjectPrefix
			{Key: "CALL#19", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2012 len
			{Key: "CALL#20", Disp: edgeDischarge, Exp: "PRE-READ"}, // :2013 fmt.Errorf
			{Key: "CALL#21", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},
			{Key: "CALL#22", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"},
			{Key: "CALL#23", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :2024
			{Key: "CALL#24", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2029 mqttDecodeOutboundExchange
			{Key: "CALL#25", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :2031
			{Key: "CALL#26", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2034 mqttExchangePhaseEnded
			{Key: "CALL#27", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2037 strconv.FormatUint
			{Key: "CALL#28", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :2037 uint64
			{Key: "CALL#29", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :2039
			{Key: "MUT#9", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
			{Key: "MUT#10", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).checkIdentifierExclusivity",
		"a nil error meaning the identifier is exclusively held, a conclusion reachable by "+
			"skipping an exchange whose read failed",
		"outbound_transition.go:(mqttJSA).commitAuthorityTransition#32",
	)

}

// The last of the direct primitive callers: two withheld handoff checks, the marker loaders,
// the enumerating walks whose short results a failed read cannot be told from, and the
// three-read resource boundary.
func init() {
	// ---- handoff_metadata.go:(mqttJSA).checkHandoffPredecessorSession
	prCensusRegister("handoff_metadata.go:(mqttJSA).checkHandoffPredecessorSession", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth: "nothing: EVERY read error is returned, with the message that the handoff \"names a " +
			"predecessor session record that could not be read\". A genuine absence is refused " +
			"here alongside a failed read",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLive,
		Repair: "no false-absence repair is owed. It is recorded because withholding is a mapping " +
			"value and not a terminal, and because the node dereferences smsg.Sequence " +
			"immediately after the nil-error check - a response carrying neither an error nor a " +
			"message would panic rather than be classified. Whether that response is reachable is " +
			"NOT established by this census and is recorded as an observation only",
		Edges: []prCensusEdge{
			{Key: "CALLER handoff_metadata.go:(mqttJSA).commitMetadataAttempt#3", Disp: edgeExpand, To: "handoff_metadata.go:(mqttJSA).commitMetadataAttempt"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :485 mqttSessSubject
			{Key: "CALL#1", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :486
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "ERR-WRAP"},                               // :488
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :492
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :496 json.Unmarshal
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :497
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :501
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :506
		},
	})

	prFormerFrontierCallers("handoff_metadata.go:(mqttJSA).checkHandoffPredecessorSession",
		"an error wrapping the read failure with %w, or a comparison against the frozen "+
			"attempt's base sequence",
		"handoff_metadata.go:(mqttJSA).commitMetadataAttempt#3",
	)

	// ---- handoff_metadata.go:(mqttJSA).resolveMetadataAttempt
	prCensusRegister("handoff_metadata.go:(mqttJSA).resolveMetadataAttempt", prCensusNode{
		Mapping: mapWithheld,
		Batch:   batchNA,
		Auth: "nothing: every read error becomes mqttMetadataUnresolved, which is the verdict " +
			"meaning the attempt's outcome is not known. Absence and failure are treated alike, " +
			"and both are unresolved rather than committed or aborted",
		ClassEv: classSound,
		ConsEv:  consNA,
		Reach:   reachLive,
		Repair: "no false-absence repair is owed; this is one of the few supported sites whose " +
			"mapping is SOUND under F-109, because it never converts a read failure into a " +
			"positive fact. What it costs is availability: a handoff whose outcome record is " +
			"genuinely absent is also unresolved",
		Edges: []prCensusEdge{
			{Key: "CALLER handoff_metadata.go:(mqttJSA).settleProposal#0", Disp: edgeExpand, To: "handoff_metadata.go:(mqttJSA).settleProposal"},
			{Key: "CALLER handoff_metadata.go:(mqttJSA).settleProposal#2", Disp: edgeExpand, To: "handoff_metadata.go:(mqttJSA).settleProposal"},
			{Key: "CALLER outbound_transition.go:(mqttJSA).checkMetadataVerdict#5", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).checkMetadataVerdict"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :164 mqttValidProposalKind
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :167 mqttHandoffOutcomeSubject
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :167 getHash
			{Key: "CALL#3", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :168
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :172 mqttDecodeHandoffOutcome
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :176 mqttOutcomeAnswersProposal
		},
	})

	prFormerFrontierCallers("handoff_metadata.go:(mqttJSA).resolveMetadataAttempt",
		"an mqttMetadataVerdict of mqttMetadataUnresolved on every read failure, which is a "+
			"withholding verdict rather than an absence-derived fact",
		"handoff_metadata.go:(mqttJSA).settleProposal#0",
		"handoff_metadata.go:(mqttJSA).settleProposal#2",
		"outbound_transition.go:(mqttJSA).checkMetadataVerdict#5",
	)

	// ---- inflight_marker.go:(mqttAccountSessionManager).loadInflightMarker
	prCensusRegister("inflight_marker.go:(mqttAccountSessionManager).loadInflightMarker", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "mqttInflightResponseAbsent for the unsent marker beside a register. The file's own " +
			"comment says the three kinds are kept apart because \"an indeterminate read is never " +
			"an absence\", and the verdict a resume consumes at inflight_register.go:9850 is " +
			"built from this kind",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLive,
		Repair: "the marker is written create-only beside the register, so a settlement re-attempt " +
			"inherits F-110. What the consuming resume actually needs is an authoritative " +
			"absence, because the marker's whole purpose is to say that a delivery was NOT " +
			"transmitted - and an absence that might be a failed read cannot carry that. The " +
			"consequence is SUSPECTED: the classification is demonstrated and the resume's onward " +
			"decision is traced, but no runtime witness exists",
		Edges: []prCensusEdge{
			{Key: "CALLER inflight_marker.go:(mqttAccountSessionManager).reapMarkerCaptureFirst#0", Disp: edgeExpand, To: "inflight_marker.go:(mqttAccountSessionManager).reapMarkerCaptureFirst"},
			{Key: "CALLER inflight_marker.go:(mqttAccountSessionManager).reapMarkerNoneHeld#0", Disp: edgeExpand, To: "inflight_marker.go:(mqttAccountSessionManager).reapMarkerNoneHeld"},
			{Key: "CALLER inflight_register.go:(mqttSession).mqttSlotEvidenceFromRecord#2", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).mqttSlotEvidenceFromRecord"},
			{Key: "CALLER inflight_register.go:(mqttSession).storeInflightRegister#44", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).storeInflightRegister"},
			{Key: "CALLER inflight_register.go:(mqttSession).storeInflightRegister#50", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).storeInflightRegister"},
			{Key: "CALLER inflight_register.go:(mqttSession).settleLandedRegister#1", Disp: edgeExpand, To: "inflight_register.go:(mqttSession).settleLandedRegister"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :779 mqttTestMarkerReloadHookV.Load
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :780 (*h).<expr>
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"}, // :782 as.mu.RLock
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"}, // :788 as.mu.RUnlock
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "this loadInflightRecord call is inside the TEST-GHOST branch, reached only when a " +
					"fixture has installed a store hook and the hook reported failure. On every " +
					"production path it does not execute; where a test does reach it, the register it " +
					"reads is accounted by the loadInflightRecord node this edge names"},
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "SEAM",
				Why: "the instance-scoped marker-load seam, which stands exactly where the real read does"},
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "SEAM",
				Why: "builds the ghost marker body the seam returns, and runs only on the test-ghost path"},
			{Key: "CALL#7", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :804
			{Key: "CALL#8", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :807
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :812 mqttDecodeInflightMarker
		},
	})

	prFormerFrontierCallers("inflight_marker.go:(mqttAccountSessionManager).loadInflightMarker",
		"an mqttMarkerLoad whose kind is mqttInflightResponseAbsent, asserting that no unsent "+
			"marker stands beside the register, which a failed store read also produces",
		"inflight_marker.go:(mqttAccountSessionManager).reapMarkerCaptureFirst#0",
		"inflight_marker.go:(mqttAccountSessionManager).reapMarkerNoneHeld#0",
		"inflight_register.go:(mqttSession).mqttSlotEvidenceFromRecord#2",
		"inflight_register.go:(mqttSession).storeInflightRegister#44",
		"inflight_register.go:(mqttSession).storeInflightRegister#50",
		"inflight_register.go:(mqttSession).settleLandedRegister#1",
	)

	// ---- outbound_cleanup_authz.go:(mqttAccountSessionManager).enumerateGenerationResources
	prCensusRegister("outbound_cleanup_authz.go:(mqttAccountSessionManager).enumerateGenerationResources", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "skipping a backing resource whose record was not read, so the enumeration reports a " +
			"resource set that may be short. The file says a record that VANISHED between the " +
			"enumeration and the load is left to a later pass and nothing is inferred from its " +
			"absence - which is the right rule, and is the rule a failed read now also satisfies",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "this row needs F-108 as well as F-109: the resource set is produced by a walk, and a " +
			"short set is either an omitted enumeration entry or a per-subject read that failed. " +
			"The count of unreadable records the function keeps is what makes the shortfall " +
			"visible, and it is already carried to the caller",
		Edges: []prCensusEdge{
			{Key: "CALLER sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration#5", Disp: edgeExpand, To: "sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :175 getHash
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :184 as.jsa.streamSubjectsComplete
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"}, // :185 mqttResourceWalkFilter
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "this isErrorOtherThan classifies the ENUMERATION that produced the subject list, not " +
					"this node's point read. Enumeration completeness is F-108's question and is excluded " +
					"here by the boundary rule, with its own ruling in the exclusions table"}, // :187
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :188 fmt.Errorf
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :193 fmt.Errorf
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :198 strings.Split
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :198 len
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :198 mqttF105SubjectPrefix
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :199 len
			{Key: "CALL#10", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :203
			{Key: "CALL#11", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :205
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :206
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :212 mqttDecodeOutboundResource
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :217 append
			{Key: "MUT#6", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
			{Key: "MUT#11", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
		},
	})

	prFormerFrontierCallers("outbound_cleanup_authz.go:(mqttAccountSessionManager).enumerateGenerationResources",
		"a resource slice that may be SHORT, together with a count of records that could not "+
			"be decoded - a failed read increments neither and simply omits the resource",
		"sess_teardown_partition.go:(mqttAccountSessionManager).classifyTeardownGeneration#5",
	)

	// ---- outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations
	// TWO point reads, at :249 and :289. Both are READ-SOURCE links and both have their own
	// mapping; the generator's one-read-per-node assumption does not hold here and the
	// occurrences are named explicitly.
	prCensusRegister("outbound_completion.go:(mqttAccountSessionManager).enumerateGenerationObligations", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "skipping an exchange or an action whose record was not read while enumerating a " +
			"generation's outstanding obligations - a generation can therefore look COMPLETE " +
			"because a record could not be read",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "as for enumerateGenerationResources, and more sharply: the function maintains " +
			"unreadableExchanges and unreadableOther counters precisely so a short enumeration is " +
			"not mistaken for a complete one, and a read failure increments neither. A repair " +
			"should make the failed read land in those counters, which is a smaller change than " +
			"an authoritative absence - but it needs the classification to distinguish the two in " +
			"the first place, which is exactly what F-109 denies",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_abandon_delivery.go:(mqttSession).abandonCurrentOutputVeto#1", Disp: edgeExpand, To: "outbound_abandon_delivery.go:(mqttSession).abandonCurrentOutputVeto"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"}, // :225 getHash
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"}, // :231 as.jsa.streamSubjectsComplete
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"}, // :232 mqttExchangeWalkFilter
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ",
				Why: "this isErrorOtherThan classifies the ENUMERATION that produced the subject list, not " +
					"this node's point read. Enumeration completeness is F-108's question and is excluded " +
					"here by the boundary rule, with its own ruling in the exclusions table"}, // :234
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :235 fmt.Errorf
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :240 fmt.Errorf
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :244 strings.Split
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :244 len
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :244 mqttF105SubjectPrefix
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :245 len
			{Key: "CALL#10", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :249
			{Key: "CALL#11", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :251
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :252
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :258 mqttDecodeOutboundExchange
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :263 append
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :266 as.jsa.streamSubjectsComplete
			{Key: "CALL#16", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :267 mqttActionWalkFilter
			{Key: "CALL#17", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :269
			{Key: "CALL#18", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :270
			{Key: "CALL#19", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :275
			{Key: "CALL#20", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :279 strings.Split
			{Key: "CALL#21", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :279 len
			{Key: "CALL#22", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :279 mqttF105SubjectPrefix
			{Key: "CALL#23", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :280 len
			{Key: "CALL#24", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :284 mqttParseOrdinalToken
			{Key: "CALL#25", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :289
			{Key: "CALL#26", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :291
			{Key: "CALL#27", Disp: edgeDischarge, Exp: "ERR-WRAP"},     // :292
			{Key: "CALL#28", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :296 mqttDecodeOutboundAction
			{Key: "CALL#29", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :301 append
			{Key: "MUT#6", Disp: edgeDischarge, Exp: "COUNTER-LOCAL", Dests: 1},
			{Key: "MUT#11", Disp: edgeDischarge, Exp: "COUNTER-LOCAL", Dests: 1},
			{Key: "MUT#12", Disp: edgeDischarge, Exp: "LOCAL-RECORD-FIELD", Dests: 2},
			{Key: "MUT#17", Disp: edgeDischarge, Exp: "COUNTER-LOCAL", Dests: 0},
			{Key: "MUT#20", Disp: edgeDischarge, Exp: "COUNTER-LOCAL", Dests: 0},
			{Key: "MUT#25", Disp: edgeDischarge, Exp: "COUNTER-LOCAL", Dests: 0},
			// Dests 2 since F-105 row 5: abandonCurrentOutputVeto reads the census's actions, positively
			// only (an ordinal it does not find refuses).
			{Key: "MUT#26", Disp: edgeDischarge, Exp: "LOCAL-RECORD-FIELD", Dests: 2},
		},
	})

	// ---- outbound_directory.go:(mqttJSA).sweepOneBatchCharged
	prCensusRegister("outbound_directory.go:(mqttJSA).sweepOneBatchCharged", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "recording a duty ordinal as SWEPT with the reason \"the entry is missing\". The file " +
			"is explicit that a missing or malformed entry is OUTSTANDING and an inconsistency " +
			"rather than completed work, and that a read failing for any other reason STOPS the " +
			"batch rather than reporting a partial one as whole - and a store-read failure takes " +
			"the missing arm, not the stopping one",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the sweep already has the discriminating structure: it distinguishes stopping from " +
			"sweeping, and it reports only the ordinals actually examined. What it lacks is a " +
			"classification that puts a failed read on the stopping side, which is F-109's repair " +
			"rather than a change to the sweep",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_directory.go:(mqttJSA).sweepOneBatch#0", Disp: edgeExpand, To: "outbound_directory.go:(mqttJSA).sweepOneBatch"},
			{Key: "CALLER outbound_recovery_pass.go:(mqttAccountSessionManager).visitRecoveryRoot#7", Disp: edgeExpand, To: "outbound_recovery_pass.go:(mqttAccountSessionManager).visitRecoveryRoot"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :692 fmt.Errorf
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :694 mqttSweepFinished
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :697 getHash
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :703 make
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :706 charge
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"},                               // :713 mqttDutySubject
			{Key: "CALL#6", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :714
			{Key: "CALL#7", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :716
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},           // :721
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "ABSENT-ARM-INTO-RETURN"}, // :724 append
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},          // :726 mqttDecodeDutyEntry
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},          // :728 append
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},          // :729
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},          // :733 mqttDutyResolved
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},          // :736 append
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},          // :738
			{Key: "MUT#7", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
			{Key: "MUT#8", Disp: edgeDischarge, Exp: "COUNTER-LOCAL", Dests: 2},
			{Key: "MUT#17", Disp: edgeDischarge, Exp: "LOCAL-RECORD-FIELD", Dests: 8},
		},
	})

	prFormerFrontierCallers("outbound_directory.go:(mqttJSA).sweepOneBatchCharged",
		"a swept-duty list and a cursor, in which an ordinal whose read failed is recorded as "+
			"missing rather than as unexamined",
		"outbound_directory.go:(mqttJSA).sweepOneBatch#0",
		"outbound_recovery_pass.go:(mqttAccountSessionManager).visitRecoveryRoot#7",
	)

	// ---- outbound_inventory_boundary.go:(mqttJSA).checkDutyContinuation
	prCensusRegister("outbound_inventory_boundary.go:(mqttJSA).checkDutyContinuation", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "refusing the continuation with the message that nothing stands at the entry - \"a " +
			"continuation over an absent entry would invent the duty it claims to advance\". Here " +
			"the absent reading REFUSES rather than permits, so a false absence costs " +
			"availability rather than safety; it is still a false fact, and the census records it " +
			"as one",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the continuation CASes at the exact sequence it read, so the present path is already " +
			"exact-sequence. The absent arm needs an authoritative absence to avoid refusing a " +
			"legal continuation",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_inventory_boundary.go:(mqttJSA).checkInventoryMembers#16", Disp: edgeExpand, To: "outbound_inventory_boundary.go:(mqttJSA).checkInventoryMembers"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :304
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :305
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "ERR-WRAP"},      // :306
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :312
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :316
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :320 mqttDecodeDutyEntry
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :322
			{Key: "CALL#7", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},  // :330
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :349 mqttDutyResolved
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},  // :349 mqttDutyResolved
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"}, // :350
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :365 mqttDutyResolved
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :365 mqttDutyResolved
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :366 jsa.checkDutyEndingIsEarned
		},
	})

	prFormerFrontierCallers("outbound_inventory_boundary.go:(mqttJSA).checkDutyContinuation",
		"an error refusing the transition because the duty entry it continues does not stand "+
			"- a refusal a failed read also produces",
		"outbound_inventory_boundary.go:(mqttJSA).checkInventoryMembers#16",
	)

	// ---- outbound_replace_register.go:(mqttJSA).loadOccupantMarkerLoad
	prCensusRegister("outbound_replace_register.go:(mqttJSA).loadOccupantMarkerLoad", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "mqttInflightResponseAbsent for the unsent marker beside a register being REPLACED. " +
			"The file states the three kinds are kept apart so that \"an indeterminate read is " +
			"neither - it is handed to mqttDecideMarker as an observation rather than folded into " +
			"an absence, which is what stops a batch's shape being taken for the store's " +
			"answer\". A store-read failure is folded into the absence anyway",
		ClassEv: classDefective,
		ConsEv:  consSuspected,
		Reach:   reachLiveNoDuty,
		Repair: "the replacement boundary re-derives rather than trusting the composer, so what it " +
			"needs is a marker observation it can trust. A create-only settlement inherits F-110; " +
			"an exact-sequence re-attempt needs the sequence the absent arm does not have. The " +
			"consequence is SUSPECTED, with the consuming decision named",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_replace_register.go:(mqttJSA).checkRegisterReplacements#22", Disp: edgeExpand, To: "outbound_replace_register.go:(mqttJSA).checkRegisterReplacements"},
			{Key: "CALLER outbound_replace_register.go:(mqttJSA).checkWithdrawalRegister#12", Disp: edgeExpand, To: "outbound_replace_register.go:(mqttJSA).checkWithdrawalRegister"},
			{Key: "CALLER outbound_withdraw_unsent.go:(mqttJSA).opWithdrawUnsent#6", Disp: edgeExpand, To: "outbound_withdraw_unsent.go:(mqttJSA).opWithdrawUnsent"},
			{Key: "CALL#0", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :483
			{Key: "CALL#1", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :485
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRESENT-ONLY"}, // :490 mqttDecodeInflightMarker
		},
	})

	prFormerFrontierCallers("outbound_replace_register.go:(mqttJSA).loadOccupantMarkerLoad",
		"an mqttMarkerLoad whose kind is mqttInflightResponseAbsent for the occupant's "+
			"marker, which a failed store read also produces",
		"outbound_replace_register.go:(mqttJSA).checkRegisterReplacements#22",
	)

	// ---- outbound_transition.go:(mqttJSA).checkResourceOverwrites
	// The key collision consult round 1 found: two calls in this function are identical under
	// (function, callee, arguments), which is why the site key carries an occurrence index.
	prCensusRegister("outbound_transition.go:(mqttJSA).checkResourceOverwrites", prCensusNode{
		Mapping: mapClassifiedAbsent,
		Batch:   batchNA,
		Auth: "two different things at its three reads, which is why this node was the collision " +
			"that forced an occurrence-keyed site key: the reads at :2304 and :2417 REFUSE on any " +
			"error, while the read at :2368 maps a 404 to \"the material is absent\" and returns " +
			"nil - accepting the batch",
		ClassEv: classDefective,
		ConsEv:  consNoneFound,
		Reach:   reachLiveNoDuty,
		Repair: "the accepting arm at :2368 is the one that needs repair, and its premise is the same " +
			"as every other absent arm here. The two refusing arms cost availability only",
		Edges: []prCensusEdge{
			{Key: "CALLER outbound_transition.go:(mqttJSA).commitAuthorityTransition#42", Disp: edgeExpand, To: "outbound_transition.go:(mqttJSA).commitAuthorityTransition"},
			{Key: "CALL#0", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :2287 mqttF105ObligationClass
			{Key: "CALL#1", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :2288 strings.Split
			{Key: "CALL#2", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :2288 len
			{Key: "CALL#3", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :2288 mqttF105SubjectPrefix
			{Key: "CALL#4", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :2289 len
			{Key: "CALL#5", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :2303 mqttExchangeSubject
			{Key: "CALL#6", Disp: edgeDischarge, Exp: "PRE-READ"},                                // :2303 getHash
			{Key: "CALL#7", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"},  // :2304
			{Key: "CALL#8", Disp: edgeDischarge, Exp: "ERR-WRAP"},                                // :2306
			{Key: "CALL#9", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                            // :2308 mqttDecodeOutboundExchange
			{Key: "CALL#10", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2310
			{Key: "CALL#11", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2312 mqttExchangePhaseEnded
			{Key: "CALL#12", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2313
			{Key: "CALL#13", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2340 jsa.loadAffineReadiness
			{Key: "CALL#14", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2342
			{Key: "CALL#15", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2345
			{Key: "CALL#16", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2350
			{Key: "CALL#17", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2367 mqttManifestSubject
			{Key: "CALL#18", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2367 getHash
			{Key: "CALL#19", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :2368
			{Key: "CALL#20", Disp: edgeDischarge,
				Why: "the local mapping itself - the isErrorOtherThan guard the file relies on to keep an " +
					"indeterminate read out of the absence arm, defeated because a store-read failure " +
					"arrives already normalized to JSNoMessageFoundErr. Accounted on axis A"}, // :2369
			{Key: "CALL#21", Disp: edgeDischarge, Exp: "ERR-WRAP"},                               // :2372
			{Key: "CALL#22", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2379 mqttDecodeRetainedManifest
			{Key: "CALL#23", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2381
			{Key: "CALL#24", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2393
			{Key: "CALL#25", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2397
			{Key: "CALL#26", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2404 mqttF105ObligationClass
			{Key: "CALL#27", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2411 strings.Split
			{Key: "CALL#28", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2411 len
			{Key: "CALL#29", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2411 mqttF105SubjectPrefix
			{Key: "CALL#30", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2412 len
			{Key: "CALL#31", Disp: edgeDischarge, Exp: "ERR-WRAP"},                               // :2413
			{Key: "CALL#32", Disp: edgeLink, Exp: "READ-SOURCE", To: "PRIMITIVE loadLastMsgFor"}, // :2417
			{Key: "CALL#33", Disp: edgeDischarge, Exp: "ERR-WRAP"},                               // :2419
			{Key: "CALL#34", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2423
			{Key: "CALL#35", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2426 mqttDecodeOutboundResource
			{Key: "CALL#36", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2428
			{Key: "CALL#37", Disp: edgeDischarge, Exp: "PRESENT-ONLY"},                           // :2431 mqttDecodeOutboundResource
			{Key: "CALL#38", Disp: edgeDischarge, Exp: "ERR-MSG-ONLY"},                           // :2433
			{Key: "CALL#39", Disp: edgeExpand,
				To: "outbound_transition.go:mqttCheckResourceStep",
				Why: "the CALLBACK-MEDIATED instance, and the one a lexical sweep of this function " +
					"misses. The sealable closure this call hands over performs the read at :2368, and " +
					"its absent arm returns nil at :2377 - which returns to mqttCheckResourceStep, NOT " +
					"from checkResourceOverwrites. mqttCheckResourceStep consumes that " +
					"absence-derived success at :2577 and permits SEALING the resource. A present " +
					"resource record does not establish a present manifest, so the seal rests on an " +
					"absence this census cannot vouch for"},
			{Key: "MUT#4", Disp: edgeDischarge, Exp: "LOCAL-CONTAINER"},
		},
	})

	prFormerFrontierCallers("outbound_transition.go:(mqttJSA).checkResourceOverwrites",
		"either an error refusing the transition, or nil accepting it - and the nil is "+
			"reachable from a read that failed",
		"outbound_transition.go:(mqttJSA).commitAuthorityTransition#42",
	)

}

// The frontier entries for the three ABSENT-ARM consequences a holistic review round found
// discharged as present-only. Each is a call the absent arm actually reaches, so each owes a
// destination — and staging them here is what turns hidden expansion debt into recorded,
// countable debt.
func init() {
	prFormerFrontierEntry("outbound_manifest.go:(mqttJSA).checkManifestAgainstStore", "CALL#7",
		"outbound_manifest.go:(mqttJSA).checkManifestOwnerExists",
		"the reservation's cleanup-owner establishment, reached with the manifest read as "+
			"ABSENT. What it receives is the licence to bring a payload into existence "+
			"create-only on a subject that may already carry a manifest")
	prFormerFrontierEntry("outbound_ops.go:(mqttJSA).opCommitMetadata", "CALL#17",
		"handoff_metadata.go:mqttFreezeMetadataAttempt",
		"a base sequence of ZERO, meaning the metadata half names no predecessor session "+
			"record as what it displaces — which is also what a genuine first record produces, "+
			"so the two are indistinguishable at this boundary")
	prFormerFrontierEntry("outbound_ops.go:(mqttJSA).opCommitMetadata", "CALL#20",
		"handoff_metadata.go:(mqttJSA).commitMetadataAttempt",
		"the frozen attempt carrying that zero base sequence, committed")
}

// The fourth ABSENT-ARM instance, found by holistic round 2 after the family sweep missed it.
//
// THE CLASS IS NOW CLOSED MECHANICALLY, not by another reading. An AST pass over every non-test
// file for a per-subject read lexically inside a FuncLit finds exactly TWO in the package, both
// in checkResourceOverwrites: the `sealable` closure at :2368, which is this instance, and the
// `terminal` closure at :2304, which returns an error for EVERY read failure including the 404
// and so lets no absence-derived fact escape. That is the whole population of callback-hosted
// reads at this revision, so the class has one defective member and one sound one.
// It is worth naming why the sweep missed it: the absent arm's `return nil` does not return
// from the node at all, it returns from a CLOSURE the node hands to a helper, so the
// consequence leaves through an argument rather than through this function's own control flow.
// The rule the sweep applied - "a node whose absent arm does not return early" - is about the
// node, and this instance is about the callback.
func init() {
	prFormerFrontierEntry("outbound_transition.go:(mqttJSA).checkResourceOverwrites", "CALL#39",
		"outbound_transition.go:mqttCheckResourceStep",
		"a sealable callback whose absent arm reports SUCCESS, which the helper consumes at "+
			"outbound_transition.go:2577 as the licence to seal the resource's provisioning - "+
			"and a seal is what a later deletion authorization rests on")
}
