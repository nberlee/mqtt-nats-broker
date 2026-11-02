package mqtt

import (
	"fmt"
	"time"
)

// The client-ID gate wait: a CONNECT that finds asm.sessLocked[cid] held waits for the
// RELEASE, not for a poll interval, and for no longer than its own absolute ownership
// deadline. See docs/ARCHITECTURE.md, "Session incarnations and teardown".
//
// THE LOST-WAKEUP RULE is that a waiter registers in the same as.mu critical section in which
// it observed the gate held, and every release closes the registered channel in the same
// critical section that deletes the gate. A release can therefore never fall between the
// observation and the registration: either the waiter saw the gate free, or its channel was
// registered before the release ran and is closed by it.
//
// ONE CHANNEL PER CLIENT ID, closed and dropped by the release: every waiter of that release
// wakes, re-runs the gate check, and the one that finds it free takes it; the others register
// again on the fresh channel the next observation creates. The deadline is absolute and never
// restarted by a wakeup.

// mqttGateWaitOutcome is why a gate wait ended.
type mqttGateWaitOutcome int

const (
	// mqttGateWaitReleased: the gate was released; re-check it.
	mqttGateWaitReleased mqttGateWaitOutcome = iota
	// mqttGateWaitExpired: the CONNECT's absolute ownership deadline passed while waiting.
	mqttGateWaitExpired
	// mqttGateWaitAborted: the arriving connection's transport was closed, by the peer
	// path, a deadline or a server shutdown. Nothing is owed to it.
	mqttGateWaitAborted
)

// sessGateWaiterLocked returns the channel the next release of cid's gate closes,
// registering one if none is registered.
//
// as.mu must be held, and must be the same critical section that observed the gate held.
func (as *mqttAccountSessionManager) sessGateWaiterLocked(cid string) <-chan struct{} {
	if as.sessGateWaiters == nil {
		as.sessGateWaiters = make(map[string]chan struct{})
	}
	ch, ok := as.sessGateWaiters[cid]
	if !ok {
		ch = make(chan struct{})
		as.sessGateWaiters[cid] = ch
	}
	return ch
}

// releaseSessGateLocked releases cid's gate and wakes every CONNECT waiting on it. It is the
// only way a gate is released, so no release site can forget the wakeup.
//
// as.mu must be held.
func (as *mqttAccountSessionManager) releaseSessGateLocked(cid string) {
	delete(as.sessLocked, cid)
	if ch, ok := as.sessGateWaiters[cid]; ok {
		delete(as.sessGateWaiters, cid)
		close(ch)
	}
}

// mqttWaitSessGate waits on a channel sessGateWaiterLocked returned, until the release closes it,
// the deadline passes, or the arriving connection is aborted.
//
// No lock held on entry.
func mqttWaitSessGate(released <-chan struct{}, abort <-chan struct{}, deadline time.Time) mqttGateWaitOutcome {
	if h := mqttTestGateWaitRegisteredHookV.Load(); h != nil {
		(*h)()
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return mqttGateWaitExpired
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-released:
		return mqttGateWaitReleased
	case <-timer.C:
		return mqttGateWaitExpired
	case <-abort:
		return mqttGateWaitAborted
	}
}

// mqttReasonStrGateHeld is the v5 CONNACK Reason String of a CONNECT whose client-ID gate
// stayed held past its deadline.
const mqttReasonStrGateHeld = "session is still completing the previous connection's teardown"

// mqttRefuseGateHeld answers a CONNECT whose client-ID gate stayed held until its absolute
// ownership deadline. A v5 client is told 0x89 Server Busy, because the disposition is
// "retry", which a bare close would leave to the client's guess about a transport error.
// 3.1.1 has no such code and keeps the close.
//
// The gate's holder is usually the previous connection's close, not another CONNECT, so the
// error names it as such.
//
// No lock held on entry.
func mqttRefuseGateHeld(c *client, cid string, sendConnAck func(rc byte, sessp bool, reasonStr string) bool) error {
	if c.mqtt.proto == mqttProtoLevel5 {
		sendConnAck(mqttReasonServerBusy, false, mqttReasonStrGateHeld)
	}
	return fmt.Errorf("client ID %q is still held by its previous connection's teardown or another CONNECT", cid)
}
