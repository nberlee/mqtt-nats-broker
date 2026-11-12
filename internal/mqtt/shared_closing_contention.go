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
	"errors"
	"math/rand"
	"time"
)

// A CLOSING SETTLES AGAINST A COMPETING CLOSER (issue #178).
//
// One discarded generation has several closers, all writing the same target states: the inline
// teardown, the recovery visit it kicks, the periodic pass, a peer's pass and the orphan sweep's
// adoption. The seal (T2) is reached identically by any of them — a standing seal reads as already
// sealed — and so is a release (T3): an unlisted acquisition reads as released. The loser settles
// by the re-read that sees the winner's write.
//
// On a clustered stream that re-read cannot see the winner while its proposal is in process:
// JetStream refuses an expected-last-subject-sequence write for the subject with 10164 until the
// entry is applied (or the leader changes), and meanwhile the ledger still reads as it was. A
// refusal of that kind decided nothing, so it spends no settlement attempt: it spaces the next
// re-read, within one ALLOWANCE per closing shared by the seal and every release episode. The
// allowance bounds this broker's patience, not the store: a JS API timeout abandons a reply, not
// the submitted write. Once it is spent a contention refusal counts again, so every episode still
// ends, and a closing that ends unsettled leaves the record marked exactly as before.

// mqttSharedContentionAllowance is one closing's patience with contention: the time one request to
// the JetStream API is allowed.
const mqttSharedContentionAllowance = mqttDefaultJSAPITimeout

// The per-attempt prices of the two closing writers, in recovery-pass work units: what one more
// attempt dispatches. The recovery visit's up-front prices multiply them out, and a contention
// retry the allowance absorbs is charged at the same price as it is taken.
const (
	// A seal attempt: the ledger read, the commit's prior re-read and one batch.
	mqttSharedSealAttemptUnits = 3
	// A release attempt at its widest, L1 over a sealed ledger: the ledger and group reads, the
	// commit's two prior re-reads and one batch.
	mqttSharedReleaseAttemptUnits = 5
)

// errMQTTSharedPriorStale marks the write boundary's refusal of a batch whose prior record moved,
// appeared or vanished since it was composed: another writer committed first, and nothing was sent.
var errMQTTSharedPriorStale = errors.New("a record the shared batch was composed against has changed")

// mqttIsSharedContention reports whether a shared-lifecycle write was refused for contention alone:
// the store's CAS rejection (10071, 10164) or the boundary's stale prior. An unreadable prior, a
// refused shape and an ambiguous outcome are not contention.
func mqttIsSharedContention(res mqttSharedWriteResult) bool {
	if res.Outcome != mqttSharedWriteRejected || res.Err == nil {
		return false
	}
	return errors.Is(res.Err, errMQTTSharedPriorStale) || mqttIsRecordCASRejection(res.Err)
}

// mqttTestSharedContentionAllowanceV, when positive, replaces the allowance.
var mqttTestSharedContentionAllowanceV = new(mqttKnob[time.Duration])

// mqttTestSharedContentionClockV, when set, replaces the allowance's clock.
var mqttTestSharedContentionClockV = new(mqttKnob[func() time.Time])

// mqttTestSharedContentionWaitV, when set, observes each contention wait instead of sleeping.
var mqttTestSharedContentionWaitV = new(mqttKnob[func(d time.Duration)])

func mqttSharedContentionNow() time.Time {
	if now := mqttTestSharedContentionClockV.Get(); now != nil {
		return now()
	}
	return time.Now()
}

// mqttSharedContention is one closing's contention allowance. A nil one absorbs nothing: the
// verdict executors on a client's path keep their counted episodes, and an unsettled verdict stays
// for the next executor.
type mqttSharedContention struct {
	deadline time.Time
	// pace, when set, is consulted before each absorbed retry with its price; false ends the
	// episode unsettled. The recovery visit charges its budget and honours its stop here.
	pace func(units int) bool
	// admit, when set, is asked before EVERY ledger write the closing sends, first attempts and
	// retries alike: each one rewrites the client's authority (shared_ledger_fence.go), so a
	// background closing yields to announced foreground work per attempt (issue #133).
	admit func() bool
}

// errMQTTSharedNotAdmitted: a background closing's ledger write was refused admission because
// foreground work of the client is announced. Nothing was sent; the next visit continues.
var errMQTTSharedNotAdmitted = errors.New("the shared closing's write was not admitted: foreground work is announced")

// mqttSharedAdmissionOnly is a background verdict executor's allowance: it absorbs no
// contention (its deadline is past, so every refusal is counted, as a nil allowance's is) and
// admits every ledger write it sends, the release fence included, through admit.
func mqttSharedAdmissionOnly(admit func() bool) *mqttSharedContention {
	return &mqttSharedContention{admit: admit}
}

// admits asks the closing's admission before one ledger write. A nil allowance, or one with no
// admission, is a client's own path and admits.
func (c *mqttSharedContention) admits() bool { return c == nil || c.admit == nil || c.admit() }

func newMqttSharedContention(pace func(units int) bool) *mqttSharedContention {
	allowance := mqttSharedContentionAllowance
	if d := mqttTestSharedContentionAllowanceV.Get(); d > 0 {
		allowance = d
	}
	return &mqttSharedContention{deadline: mqttSharedContentionNow().Add(allowance), pace: pace}
}

// mqttSharedContentionVerdict is what a refused attempt costs its episode.
type mqttSharedContentionVerdict uint8

const (
	// mqttSharedContentionCounted: the attempt is spent, as for any failure.
	mqttSharedContentionCounted mqttSharedContentionVerdict = iota
	// mqttSharedContentionAbsorbed: contention within the allowance; the wait is taken and no
	// attempt is spent.
	mqttSharedContentionAbsorbed
	// mqttSharedContentionStopped: the pace refused the retry; the episode ends unsettled.
	mqttSharedContentionStopped
)

// absorb classifies one refused attempt and, when it is absorbed, takes the wait before the
// re-read that settles it. No lock is held across the wait.
func (cw *mqttSharedContention) absorb(res mqttSharedWriteResult, units int) mqttSharedContentionVerdict {
	if cw == nil || !mqttIsSharedContention(res) || !mqttSharedContentionNow().Before(cw.deadline) {
		return mqttSharedContentionCounted
	}
	if cw.pace != nil && !cw.pace(units) {
		return mqttSharedContentionStopped
	}
	d := mqttAuthorityContentionBackoff(rand.Float64())
	if h := mqttTestSharedContentionWaitV.Get(); h != nil {
		h(d)
	} else {
		time.Sleep(d)
	}
	return mqttSharedContentionAbsorbed
}
