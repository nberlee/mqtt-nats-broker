//go:build mqttport_fulltests

package mqtt

import (
	"time"
)

// ---- from outbound_duty_exec.go ----

func setMQTTTestRecoveryReadinessHook(h func()) func() {
	prev := mqttTestRecoveryReadinessHookV.Load()
	if h == nil {
		mqttTestRecoveryReadinessHookV.Store(nil)
	} else {
		mqttTestRecoveryReadinessHookV.Store(&h)
	}
	return func() { mqttTestRecoveryReadinessHookV.Store(prev) }
}

// ---- from outbound_recovery_pass.go ----

func setMQTTTestRecoveryRootVisitHook(h func()) func() {
	prev := mqttTestRecoveryRootVisitHookV.Load()
	if h == nil {
		mqttTestRecoveryRootVisitHookV.Store(nil)
	} else {
		mqttTestRecoveryRootVisitHookV.Store(&h)
	}
	return func() { mqttTestRecoveryRootVisitHookV.Store(prev) }
}

// ---- from qos2_shared_affine.go ----

func setMQTTTestAssignTailHook(h func()) func() {
	prev := mqttTestAssignTailHookV.Load()
	if h == nil {
		mqttTestAssignTailHookV.Store(nil)
	} else {
		mqttTestAssignTailHookV.Store(&h)
	}
	return func() { mqttTestAssignTailHookV.Store(prev) }
}

// ---- from testhooks.go ----

// setMQTTTestSessExpirySweepPassHook installs (or, with nil, clears) the sweep-pass
// hook and returns a function that restores the previous value.
func setMQTTTestSessExpirySweepPassHook(h func(as *mqttAccountSessionManager)) func() {
	prev := mqttTestSessExpirySweepPassHookV.Load()
	if h == nil {
		mqttTestSessExpirySweepPassHookV.Store(nil)
	} else {
		mqttTestSessExpirySweepPassHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestSessExpirySweepPassHookV.Store(prev) })
}

// setMQTTTestConnAckAttemptHook installs (or, with nil, clears) the CONNACK attempt counter and
// returns a function that restores the previous value.
func setMQTTTestConnAckAttemptHook(h func(rc byte)) func() {
	prev := mqttTestConnAckAttemptHookV.Load()
	if h == nil {
		mqttTestConnAckAttemptHookV.Store(nil)
	} else {
		mqttTestConnAckAttemptHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestConnAckAttemptHookV.Store(prev) })
}

// setMQTTTestForcedLeavePreSettleHook installs (or, with nil, clears) the pre-settlement hook and
// returns a function that restores the previous value.
func setMQTTTestForcedLeavePreSettleHook(h func(sess *mqttSession)) func() {
	prev := mqttTestForcedLeavePreSettleHookV.Load()
	if h == nil {
		mqttTestForcedLeavePreSettleHookV.Store(nil)
	} else {
		mqttTestForcedLeavePreSettleHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestForcedLeavePreSettleHookV.Store(prev) })
}

// setMQTTTestForcedLeavePostSettleHook installs (or, with nil, clears) the post-settlement hook
// and returns a function that restores the previous value.
func setMQTTTestForcedLeavePostSettleHook(h func(sess *mqttSession)) func() {
	prev := mqttTestForcedLeavePostSettleHookV.Load()
	if h == nil {
		mqttTestForcedLeavePostSettleHookV.Store(nil)
	} else {
		mqttTestForcedLeavePostSettleHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestForcedLeavePostSettleHookV.Store(prev) })
}

// setMQTTTestForcedLeaveReleaseHook installs (or, with nil, clears) the phase-3 entry hook and
// returns a function that restores the previous value.
func setMQTTTestForcedLeaveReleaseHook(h func()) func() {
	prev := mqttTestForcedLeaveReleaseHookV.Load()
	if h == nil {
		mqttTestForcedLeaveReleaseHookV.Store(nil)
	} else {
		mqttTestForcedLeaveReleaseHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestForcedLeaveReleaseHookV.Store(prev) })
}

// setMQTTTestForcedLeaveCommitHook installs (or, with nil, clears) the forced-leave commit
// hook and returns a function that restores the previous value.
func setMQTTTestForcedLeaveCommitHook(h func()) func() {
	prev := mqttTestForcedLeaveCommitHookV.Load()
	if h == nil {
		mqttTestForcedLeaveCommitHookV.Store(nil)
	} else {
		mqttTestForcedLeaveCommitHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestForcedLeaveCommitHookV.Store(prev) })
}

// setMQTTTestPubRelSubFailHook installs (or, with nil, clears) the PUBREL
// subscription failure hook and returns a function that restores the previous value.
func setMQTTTestPubRelSubFailHook(h func() error) func() {
	prev := mqttTestPubRelSubFailHookV.Load()
	if h == nil {
		mqttTestPubRelSubFailHookV.Store(nil)
	} else {
		mqttTestPubRelSubFailHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestPubRelSubFailHookV.Store(prev) })
}

// setMQTTTestDeliveryOfferedHook installs (or, with nil, clears) the delivery offer hook and
// returns a function that restores the previous value.
func setMQTTTestDeliveryOfferedHook(h func(sub *subscription, reply string)) func() {
	prev := mqttTestDeliveryOfferedHookV.Load()
	if h == nil {
		mqttTestDeliveryOfferedHookV.Store(nil)
	} else {
		mqttTestDeliveryOfferedHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDeliveryOfferedHookV.Store(prev) })
}

// setMQTTTestDeliverySkippedHook installs (or, with nil, clears) the closed-arm skip hook
// and returns a function that restores the previous value.
func setMQTTTestDeliverySkippedHook(h func(sub *subscription, reply string)) func() {
	prev := mqttTestDeliverySkippedHookV.Load()
	if h == nil {
		mqttTestDeliverySkippedHookV.Store(nil)
	} else {
		mqttTestDeliverySkippedHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDeliverySkippedHookV.Store(prev) })
}

func setMQTTTestSubPause(h func(at string)) func() {
	prev := mqttTestSubPauseV.Load()
	if h == nil {
		mqttTestSubPauseV.Store(nil)
	} else {
		mqttTestSubPauseV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestSubPauseV.Store(prev) })
}

// setMQTTTestDeferredDeleteHook installs (or, with nil, clears) the deferred-teardown
// counter seam and returns a function that restores the previous value.
func setMQTTTestDeferredDeleteHook(h func(sid, durable string)) func() {
	prev := mqttTestDeferredDeleteHookV.Load()
	if h == nil {
		mqttTestDeferredDeleteHookV.Store(nil)
	} else {
		mqttTestDeferredDeleteHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestDeferredDeleteHookV.Store(prev) })
}

// setMQTTTestExpiryNow installs (or, with nil, clears) the expiry clock and returns a
// restore func. Process-global: a test using it is not parallel.
func setMQTTTestExpiryNow(h func() time.Time) func() {
	prev := mqttTestExpiryNowV.Load()
	if h == nil {
		mqttTestExpiryNowV.Store(nil)
	} else {
		mqttTestExpiryNowV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestExpiryNowV.Store(prev) })
}

func setMQTTTestSessRecordBodyHook(h func(sess *mqttSession, body []byte)) func() {
	prev := mqttTestSessRecordBodyHookV.Load()
	if h == nil {
		mqttTestSessRecordBodyHookV.Store(nil)
	} else {
		mqttTestSessRecordBodyHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestSessRecordBodyHookV.Store(prev) })
}

func setMQTTTestPubRelPreForwardHook(h func()) func() {
	prev := mqttTestPubRelPreForwardHookV.Load()
	if h == nil {
		mqttTestPubRelPreForwardHookV.Store(nil)
	} else {
		mqttTestPubRelPreForwardHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestPubRelPreForwardHookV.Store(prev) })
}

func setMQTTTestPublishDrainedPause(h func(pi uint16)) func() {
	prev := mqttTestPublishDrainedPauseV.Load()
	if h == nil {
		mqttTestPublishDrainedPauseV.Store(nil)
	} else {
		mqttTestPublishDrainedPauseV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestPublishDrainedPauseV.Store(prev) })
}

func setMQTTTestPubRelDeliveryDelay(h func()) func() {
	prev := mqttTestPubRelDeliveryDelayV.Load()
	if h == nil {
		mqttTestPubRelDeliveryDelayV.Store(nil)
	} else {
		mqttTestPubRelDeliveryDelayV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestPubRelDeliveryDelayV.Store(prev) })
}

func setMQTTTestPubRelDeliveryDoneHook(h func(sub *subscription, reply string)) func() {
	prev := mqttTestPubRelDeliveryDoneHookV.Load()
	if h == nil {
		mqttTestPubRelDeliveryDoneHookV.Store(nil)
	} else {
		mqttTestPubRelDeliveryDoneHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestPubRelDeliveryDoneHookV.Store(prev) })
}

func setMQTTTestMixedSiteFrame(h func(c *client, site string, frame []byte)) func() {
	prev := mqttTestMixedSiteFrameV.Load()
	if h == nil {
		mqttTestMixedSiteFrameV.Store(nil)
	} else {
		mqttTestMixedSiteFrameV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestMixedSiteFrameV.Store(prev) })
}

// setMQTTTestStoreMsgResultHook installs (or, with nil, clears) the post-store
// hook and returns a function that restores the previous value.
func setMQTTTestStoreMsgResultHook(h func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error)) func() {
	prev := mqttTestStoreMsgResultHookV.Load()
	if h == nil {
		mqttTestStoreMsgResultHookV.Store(nil)
	} else {
		mqttTestStoreMsgResultHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestStoreMsgResultHookV.Store(prev) })
}

// setMQTTTestStoreMsgHook installs (or, with nil, clears) the storeMsg hook and
// returns a function that restores the previous value.
func setMQTTTestStoreMsgHook(h func(subject string, hdrLen int, msg []byte) error) func() {
	prev := mqttTestStoreMsgHookV.Load()
	if h == nil {
		mqttTestStoreMsgHookV.Store(nil)
	} else {
		mqttTestStoreMsgHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestStoreMsgHookV.Store(prev) })
}

// ---- from unsub_process.go ----

func setMQTTTestUnsubAckWindowHook(h func(c *client)) func() {
	prev := mqttTestUnsubAckWindowHookV.Load()
	if h == nil {
		mqttTestUnsubAckWindowHookV.Store(nil)
	} else {
		mqttTestUnsubAckWindowHookV.Store(&h)
	}
	return mqttTestRestoreOnce(func() { mqttTestUnsubAckWindowHookV.Store(prev) })
}
