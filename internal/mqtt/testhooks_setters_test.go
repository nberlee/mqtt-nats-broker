package mqtt

import (
	"time"
)

// ---- from ephemeral_identity.go ----

// setMQTTTestEphemeralDraw arms the token-draw seam and returns its restore function.
func setMQTTTestEphemeralDraw(h func(which string) (string, bool)) func() {
	prev := mqttTestEphemeralDrawV.Load()
	mqttTestEphemeralDrawV.Store(&h)
	return mqttTestRestoreOnce(func() { mqttTestEphemeralDrawV.Store(prev) })
}

// setMQTTTestConsumerDeleteHook arms the delete seam and returns its restore function.
func setMQTTTestConsumerDeleteHook(h func(stream, name string, attempt int) (send bool, observed error, override bool)) func() {
	prev := mqttTestConsumerDeleteHookV.Load()
	mqttTestConsumerDeleteHookV.Store(&h)
	return mqttTestRestoreOnce(func() { mqttTestConsumerDeleteHookV.Store(prev) })
}

// ---- from inflight_marker.go ----

func setMQTTTestMarkerDispositionHook(h func(caller string, v mqttMarkerVerdict)) func() {
	prev := mqttTestMarkerDispositionHookV.Load()
	if h == nil {
		mqttTestMarkerDispositionHookV.Store(nil)
	} else {
		mqttTestMarkerDispositionHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestMarkerDispositionHookV.Store(prev) })
}

func setMQTTTestMarkerDeleteHook(h func(caller, subject string, seq uint64) error) func() {
	prev := mqttTestMarkerDeleteHookV.Load()
	if h == nil {
		mqttTestMarkerDeleteHookV.Store(nil)
	} else {
		mqttTestMarkerDeleteHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestMarkerDeleteHookV.Store(prev) })
}

func setMQTTTestMarkerReloadHook(h func(caller, subject string)) func() {
	prev := mqttTestMarkerReloadHookV.Load()
	if h == nil {
		mqttTestMarkerReloadHookV.Store(nil)
	} else {
		mqttTestMarkerReloadHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestMarkerReloadHookV.Store(prev) })
}

func setMQTTTestMarkerSendWindowHook(h func(sess *mqttSession, pi uint16, window string) bool) func() {
	prev := mqttTestMarkerSendWindowHookV.Load()
	if h == nil {
		mqttTestMarkerSendWindowHookV.Store(nil)
	} else {
		mqttTestMarkerSendWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestMarkerSendWindowHookV.Store(prev) })
}

func setMQTTTestInflightPreSendHook(h func(jsDur, reply string) bool) func() {
	prev := mqttTestInflightPreSendHookV.Load()
	if h == nil {
		mqttTestInflightPreSendHookV.Store(nil)
	} else {
		mqttTestInflightPreSendHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestInflightPreSendHookV.Store(prev) })
}

// ---- from inflight_register.go ----

func setMQTTTestVoidHandoffWindowHook(h func(sess *mqttSession, window string)) func() {
	prev := mqttTestVoidHandoffWindowHookV.Load()
	if h == nil {
		mqttTestVoidHandoffWindowHookV.Store(nil)
	} else {
		mqttTestVoidHandoffWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestVoidHandoffWindowHookV.Store(prev) })
}

func setMQTTTestVoidHandoffPreAttemptHook(h func(sess *mqttSession)) func() {
	prev := mqttTestVoidHandoffPreAttemptHookV.Load()
	if h == nil {
		mqttTestVoidHandoffPreAttemptHookV.Store(nil)
	} else {
		mqttTestVoidHandoffPreAttemptHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestVoidHandoffPreAttemptHookV.Store(prev) })
}

// ---- from sess_accept_fact.go ----

// ---- from sess_clear.go ----

func setMQTTTestClearWindowHook(h func(sess *mqttSession, window string)) func() {
	prev := mqttTestClearWindowHookV.Load()
	if h == nil {
		mqttTestClearWindowHookV.Store(nil)
	} else {
		mqttTestClearWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestClearWindowHookV.Store(prev) })
}

// ---- from sess_expiry.go ----

func setMQTTTestExpireDecisionHook(h func(sess *mqttSession, loadedSeq, seq uint64, superseded bool)) func() {
	prev := mqttTestExpireDecisionHookV.Load()
	if h == nil {
		mqttTestExpireDecisionHookV.Store(nil)
	} else {
		mqttTestExpireDecisionHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestExpireDecisionHookV.Store(prev) })
}

// ---- from sess_fork_repair.go ----

func setMQTTTestForkRepairMergeHook(h func(sess *mqttSession)) func() {
	prev := mqttTestForkRepairMergeHookV.Load()
	if h == nil {
		mqttTestForkRepairMergeHookV.Store(nil)
	} else {
		mqttTestForkRepairMergeHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestForkRepairMergeHookV.Store(prev) })
}

// ---- from sess_gone_retire.go ----

// setMQTTTestGoneRetireProofEntryHook installs (or, with nil, clears) the proof-entry
// hook and returns a function that restores the previous value.
func setMQTTTestGoneRetireProofEntryHook(h func(sess *mqttSession)) func() {
	prev := mqttTestGoneRetireProofEntryHookV.Load()
	if h == nil {
		mqttTestGoneRetireProofEntryHookV.Store(nil)
	} else {
		mqttTestGoneRetireProofEntryHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestGoneRetireProofEntryHookV.Store(prev) })
}

// setMQTTTestGoneRetireIdentityHook installs (or, with nil, clears) the pre-recheck hook
// and returns a function that restores the previous value.
func setMQTTTestGoneRetireIdentityHook(h func(sess *mqttSession)) func() {
	prev := mqttTestGoneRetireIdentityHookV.Load()
	if h == nil {
		mqttTestGoneRetireIdentityHookV.Store(nil)
	} else {
		mqttTestGoneRetireIdentityHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestGoneRetireIdentityHookV.Store(prev) })
}

// ---- from sess_manager.go ----

// setMQTTSessionExpirySweepInterval installs a new interval and returns the restore
// func. The value it guards stays in sess_manager.go (F-054).
func setMQTTSessionExpirySweepInterval(d time.Duration) func() {
	old := mqttSessionExpirySweepIntervalNS.Swap(int64(d))
	return func() { mqttSessionExpirySweepIntervalNS.Store(old) }
}

// ---- from sess_record_watch.go ----

// setMQTTTestConsumerCreateResultHook arms that seam and returns its restore function.
func setMQTTTestConsumerCreateResultHook(h func(ccr *CreateConsumerRequest, err error) error) func() {
	prev := mqttTestConsumerCreateResultHookV.Load()
	mqttTestConsumerCreateResultHookV.Store(&h)
	return mqttTestRestoreOnce(func() { mqttTestConsumerCreateResultHookV.Store(prev) })
}

// setMQTTTestWatchConsumerCreateFail arms the seam and returns its restore function.
func setMQTTTestWatchConsumerCreateFail(fail bool) func() {
	prev := mqttTestWatchConsumerCreateFailV.Load()
	mqttTestWatchConsumerCreateFailV.Store(&fail)
	return mqttTestRestoreOnce(func() { mqttTestWatchConsumerCreateFailV.Store(prev) })
}

// ---- from sess_recordtx.go ----

func setMQTTTestRecordTxReleaseHook(h func(sess *mqttSession)) func() {
	prev := mqttTestRecordTxReleaseHookV.Load()
	if h == nil {
		mqttTestRecordTxReleaseHookV.Store(nil)
	} else {
		mqttTestRecordTxReleaseHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRecordTxReleaseHookV.Store(prev) })
}

func setMQTTTestRecordTxPostReleaseHook(h func(sess *mqttSession)) func() {
	prev := mqttTestRecordTxPostReleaseHookV.Load()
	if h == nil {
		mqttTestRecordTxPostReleaseHookV.Store(nil)
	} else {
		mqttTestRecordTxPostReleaseHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRecordTxPostReleaseHookV.Store(prev) })
}

func setMQTTTestRecordTxWaitHook(h func(sess *mqttSession)) func() {
	prev := mqttTestRecordTxWaitHookV.Load()
	if h == nil {
		mqttTestRecordTxWaitHookV.Store(nil)
	} else {
		mqttTestRecordTxWaitHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRecordTxWaitHookV.Store(prev) })
}

func setMQTTTestRecordTxWithdrawGuardHook(h func(sess *mqttSession)) func() {
	prev := mqttTestRecordTxWithdrawGuardHookV.Load()
	if h == nil {
		mqttTestRecordTxWithdrawGuardHookV.Store(nil)
	} else {
		mqttTestRecordTxWithdrawGuardHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRecordTxWithdrawGuardHookV.Store(prev) })
}

func setMQTTTestTryBeginPreLockHook(h func(sess *mqttSession)) func() {
	prev := mqttTestTryBeginPreLockHookV.Load()
	if h == nil {
		mqttTestTryBeginPreLockHookV.Store(nil)
	} else {
		mqttTestTryBeginPreLockHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestTryBeginPreLockHookV.Store(prev) })
}

// ---- from sess_rejected_write.go ----

func setMQTTTestRejectionWindowHook(h func(sess *mqttSession, window string)) func() {
	prev := mqttTestRejectionWindowHookV.Load()
	if h == nil {
		mqttTestRejectionWindowHookV.Store(nil)
	} else {
		mqttTestRejectionWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRejectionWindowHookV.Store(prev) })
}

// ---- from sess_restore_gate.go ----

func setMQTTTestRestoreGateBlocked(h func(kind string)) func() {
	prev := mqttTestRestoreGateBlockedV.Load()
	if h == nil {
		mqttTestRestoreGateBlockedV.Store(nil)
	} else {
		mqttTestRestoreGateBlockedV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRestoreGateBlockedV.Store(prev) })
}

func setMQTTTestRestorePause(h func(at string)) func() {
	prev := mqttTestRestorePauseV.Load()
	if h == nil {
		mqttTestRestorePauseV.Store(nil)
	} else {
		mqttTestRestorePauseV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRestorePauseV.Store(prev) })
}

// ---- from sess_takeover_redrive.go ----

func setMQTTTestTeardownAdoptionWindowHook(h func(clientID, idHash string, markerSeq uint64)) func() {
	prev := mqttTestTeardownAdoptionWindowHookV.Load()
	if h == nil {
		mqttTestTeardownAdoptionWindowHookV.Store(nil)
	} else {
		mqttTestTeardownAdoptionWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestTeardownAdoptionWindowHookV.Store(prev) })
}

func setMQTTTestReDriveClock(h func() time.Time) func() {
	prev := mqttTestReDriveClockV.Load()
	if h == nil {
		mqttTestReDriveClockV.Store(nil)
	} else {
		mqttTestReDriveClockV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestReDriveClockV.Store(prev) })
}

// ---- from sess_teardown.go ----

// setMQTTTestRetirementLoadHook installs (or, with nil, clears) the watermark load
// hook and returns a function that restores the previous value.
func setMQTTTestRetirementLoadHook(h func(idHash string) (*mqttGenRetirement, uint64, mqttRetirementStatus)) func() {
	prev := mqttTestRetirementLoadHookV.Load()
	if h == nil {
		mqttTestRetirementLoadHookV.Store(nil)
	} else {
		mqttTestRetirementLoadHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRetirementLoadHookV.Store(prev) })
}

// setMQTTTestRetirementStoreHook installs (or, with nil, clears) the watermark store
// hook and returns a function that restores the previous value.
func setMQTTTestRetirementStoreHook(h func(subject string, expectSeq uint64, r *mqttGenRetirement) (uint64, error)) func() {
	prev := mqttTestRetirementStoreHookV.Load()
	if h == nil {
		mqttTestRetirementStoreHookV.Store(nil)
	} else {
		mqttTestRetirementStoreHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRetirementStoreHookV.Store(prev) })
}

// ---- from sess_uncertain.go ----

func setMQTTTestResolveReattemptHook(h func(sess *mqttSession)) func() {
	prev := mqttTestResolveReattemptHookV.Load()
	if h == nil {
		mqttTestResolveReattemptHookV.Store(nil)
	} else {
		mqttTestResolveReattemptHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestResolveReattemptHookV.Store(prev) })
}

func setMQTTTestWillPhaseProbe(h func(sess *mqttSession, asm *mqttAccountSessionManager)) func() {
	prev := mqttTestWillPhaseProbeV.Load()
	if h == nil {
		mqttTestWillPhaseProbeV.Store(nil)
	} else {
		mqttTestWillPhaseProbeV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestWillPhaseProbeV.Store(prev) })
}

func setMQTTTestDischargeWindowHook(h func(sess *mqttSession)) func() {
	prev := mqttTestDischargeWindowHookV.Load()
	if h == nil {
		mqttTestDischargeWindowHookV.Store(nil)
	} else {
		mqttTestDischargeWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDischargeWindowHookV.Store(prev) })
}

// ---- from sub_operation_gate.go ----

func setMQTTTestGateAboutToWait(h func(sess *mqttSession)) func() {
	prev := mqttTestGateAboutToWaitV.Load()
	if h == nil {
		mqttTestGateAboutToWaitV.Store(nil)
	} else {
		mqttTestGateAboutToWaitV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestGateAboutToWaitV.Store(prev) })
}

// ---- from testhooks.go ----

// setMQTTTestSessSaveHook installs (or, with nil, clears) the save hook and
// returns a function that restores the previous value.
func setMQTTTestSessSaveHook(h func(sess *mqttSession) error) func() {
	prev := mqttTestSessSaveHookV.Load()
	if h == nil {
		mqttTestSessSaveHookV.Store(nil)
	} else {
		mqttTestSessSaveHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestSessSaveHookV.Store(prev) })
}

// setMQTTTestQOS12SubFailHook installs (or, with nil, clears) the delivery-subscription
// failure hook and returns a function that restores the previous value.
func setMQTTTestQOS12SubFailHook(h func(durable string) error) func() {
	prev := mqttTestQOS12SubFailHookV.Load()
	if h == nil {
		mqttTestQOS12SubFailHookV.Store(nil)
	} else {
		mqttTestQOS12SubFailHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestQOS12SubFailHookV.Store(prev) })
}

// setMQTTTestDeliveryPreLockHook installs (or, with nil, clears) the delivery pre-lock hook
// and returns a function that restores the previous value.
func setMQTTTestDeliveryPreLockHook(h func(sub *subscription)) func() {
	prev := mqttTestDeliveryPreLockHookV.Load()
	if h == nil {
		mqttTestDeliveryPreLockHookV.Store(nil)
	} else {
		mqttTestDeliveryPreLockHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDeliveryPreLockHookV.Store(prev) })
}

// setMQTTTestDeliveryDoneHook installs (or, with nil, clears) the delivery completion hook
// and returns a function that restores the previous value.
func setMQTTTestDeliveryDoneHook(h func(sub *subscription)) func() {
	prev := mqttTestDeliveryDoneHookV.Load()
	if h == nil {
		mqttTestDeliveryDoneHookV.Store(nil)
	} else {
		mqttTestDeliveryDoneHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDeliveryDoneHookV.Store(prev) })
}

// setMQTTTestDrainDeleteHook installs (or, with nil, clears) the retired-drain delete seam
// and returns a function that restores the previous value.
func setMQTTTestDrainDeleteHook(h func(durable string, noWait bool) (send bool, observed error, override bool)) func() {
	prev := mqttTestDrainDeleteHookV.Load()
	if h == nil {
		mqttTestDrainDeleteHookV.Store(nil)
	} else {
		mqttTestDrainDeleteHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDrainDeleteHookV.Store(prev) })
}

// setMQTTTestDrainRetirementUnlockedHook installs (or, with nil, clears) the post-unlock
// retirement pause and returns a function that restores the previous value.
func setMQTTTestDrainRetirementUnlockedHook(h func(durable string)) func() {
	prev := mqttTestDrainRetirementUnlockedHookV.Load()
	if h == nil {
		mqttTestDrainRetirementUnlockedHookV.Store(nil)
	} else {
		mqttTestDrainRetirementUnlockedHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDrainRetirementUnlockedHookV.Store(prev) })
}

// setMQTTTestDrainRestorePreInstallHook installs (or, with nil, clears) the restore
// pre-install pause and returns a function that restores the previous value.
func setMQTTTestDrainRestorePreInstallHook(h func(durable string)) func() {
	prev := mqttTestDrainRestorePreInstallHookV.Load()
	if h == nil {
		mqttTestDrainRestorePreInstallHookV.Store(nil)
	} else {
		mqttTestDrainRestorePreInstallHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDrainRestorePreInstallHookV.Store(prev) })
}

// setMQTTTestTeardownClaimedHook installs (or, with nil, clears) the post-claim
// hook and returns a function that restores the previous value.
func setMQTTTestTeardownClaimedHook(h func(sess *mqttSession, nonce string, markerSeq uint64)) func() {
	prev := mqttTestTeardownClaimedHookV.Load()
	if h == nil {
		mqttTestTeardownClaimedHookV.Store(nil)
	} else {
		mqttTestTeardownClaimedHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestTeardownClaimedHookV.Store(prev) })
}

// setMQTTTestLoadLastMsgHook installs (or, with nil, clears) the loadLastMsgFor
// hook and returns a function that restores the previous value.
func setMQTTTestLoadLastMsgHook(h func(stream, subject string) error) func() {
	prev := mqttTestLoadLastMsgHookV.Load()
	if h == nil {
		mqttTestLoadLastMsgHookV.Store(nil)
	} else {
		mqttTestLoadLastMsgHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestLoadLastMsgHookV.Store(prev) })
}

func setMQTTTestSubsBarrierSampledHook(h func(b *mqttSubsBarrier, sampled int)) func() {
	prev := mqttTestSubsBarrierSampledHookV.Load()
	if h == nil {
		mqttTestSubsBarrierSampledHookV.Store(nil)
	} else {
		mqttTestSubsBarrierSampledHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestSubsBarrierSampledHookV.Store(prev) })
}

func setMQTTTestSubsBarrierHeldHook(h func(filters []*mqttFilter)) func() {
	prev := mqttTestSubsBarrierHeldHookV.Load()
	if h == nil {
		mqttTestSubsBarrierHeldHookV.Store(nil)
	} else {
		mqttTestSubsBarrierHeldHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestSubsBarrierHeldHookV.Store(prev) })
}

func setMQTTTestSessStoreResultHook(h func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)) func() {
	prev := mqttTestSessStoreResultHookV.Load()
	if h == nil {
		mqttTestSessStoreResultHookV.Store(nil)
	} else {
		mqttTestSessStoreResultHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestSessStoreResultHookV.Store(prev) })
}

func setMQTTTestPubRelWriteBlock(h func()) func() {
	prev := mqttTestPubRelWriteBlockV.Load()
	if h == nil {
		mqttTestPubRelWriteBlockV.Store(nil)
	} else {
		mqttTestPubRelWriteBlockV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestPubRelWriteBlockV.Store(prev) })
}

func setMQTTTestRetainedFlushBlock(h func([]*subscription)) func() {
	prev := mqttTestRetainedFlushBlockV.Load()
	if h == nil {
		mqttTestRetainedFlushBlockV.Store(nil)
	} else {
		mqttTestRetainedFlushBlockV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestRetainedFlushBlockV.Store(prev) })
}

func setMQTTTestLoadNextMsgHook(h func(stream, subject string, seq uint64) error) func() {
	prev := mqttTestLoadNextMsgHookV.Load()
	if h == nil {
		mqttTestLoadNextMsgHookV.Store(nil)
	} else {
		mqttTestLoadNextMsgHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestLoadNextMsgHookV.Store(prev) })
}

func setMQTTTestDeleteMsgHook(h func(stream string, seq uint64) error) func() {
	prev := mqttTestDeleteMsgHookV.Load()
	if h == nil {
		mqttTestDeleteMsgHookV.Store(nil)
	} else {
		mqttTestDeleteMsgHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDeleteMsgHookV.Store(prev) })
}

// ---- from unsub_drain.go ----

func setMQTTTestDrainingAllowedHook(h func()) func() {
	prev := mqttTestDrainingAllowedV.Load()
	if h == nil {
		mqttTestDrainingAllowedV.Store(nil)
	} else {
		mqttTestDrainingAllowedV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDrainingAllowedV.Store(prev) })
}
