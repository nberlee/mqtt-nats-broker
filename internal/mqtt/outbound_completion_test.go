//go:build mqttport_fulltests

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

import "testing"

func testF105Exchange(qos byte, phase mqttExchangePhase) *mqttOutboundExchange {
	return &mqttOutboundExchange{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Nonce: "xch-1",
		PI: 7, QoS: qos, Phase: phase, AdmitTxn: "txn-1", Tenure: "tenure-0",
	}
}

func testF105Authority(state mqttAuthorityState) *mqttOutboundAuthority {
	a := &mqttOutboundAuthority{
		ID: testF105ID, Epoch: testF105Epoch, Gen: testF105Gen, Tenure: "tenure-0",
		State: state, Rev: 1, Txn: "txn-0", OwnerTxn: "txn-0",
	}
	if state == mqttAuthorityPreparing {
		a.Intent = testF105PreparingAuthority().Intent
		a.Intent.PrevGen, a.Intent.PrevTenure, a.Intent.PrevOwnerTxn = a.Gen, a.Tenure, a.OwnerTxn
		a.Intent.Retire.Gen, a.Intent.Retire.Tenure, a.Intent.Retire.OwnerTxn = a.Gen, a.Tenure, a.OwnerTxn
	}
	return a
}

// TestF105CompletionDoesNotNeedServingAuthority is the central rule of the completion model,
// and the one a later change is most likely to break by "tightening" it.
//
// Completing an obligation that was ALREADY ADMITTED must not require current serving
// authority. If it did, transfer and closure would themselves destroy the completion route
// of every obligation admitted before them — which is the defect F-105 exists to remove.
// Beginning new work is the only thing the authority governs.
func TestF105CompletionDoesNotNeedServingAuthority(t *testing.T) {
	for name, state := range map[string]mqttAuthorityState{
		"serving":   mqttAuthorityServing,
		"preparing": mqttAuthorityPreparing,
		"closed":    mqttAuthorityClosed,
	} {
		auth := testF105Authority(state)

		// Executing an already-acquired action, and recording the client's answer, proceed
		// under every authority state — including a closed one.
		for _, op := range []mqttCompletionOp{mqttOpExecuteAcquired, mqttOpObserveAck} {
			got, err := mqttCompletionAllowed(mqttCompletionInput{
				Op: op, Auth: auth, Exchange: testF105Exchange(2, mqttExchangePublish),
			})
			if err != nil {
				t.Fatalf("%s/%d: %v", name, op, err)
			}
			if got != mqttCompletionProceed {
				t.Fatalf("under a %s authority, operation %d on an admitted exchange was %d, "+
					"want PROCEED: completing admitted work must not need serving authority",
					name, op, got)
			}
		}

		// Materializing an owed release is a durable duty with no socket effect, so it too
		// survives closure.
		got, err := mqttCompletionAllowed(mqttCompletionInput{
			Op: mqttOpMaterializeOwed, Auth: auth,
			Exchange: testF105Exchange(2, mqttExchangePubRecObserved),
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != mqttCompletionProceed {
			t.Fatalf("under a %s authority, materializing an owed release was %d, want PROCEED", name, got)
		}

		// And a terminal exchange with nothing outstanding releases.
		got, err = mqttCompletionAllowed(mqttCompletionInput{
			Op: mqttOpReleaseTerminal, Auth: auth,
			Exchange: testF105Exchange(2, mqttExchangeTerminal),
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != mqttCompletionProceed {
			t.Fatalf("under a %s authority, releasing a drained terminal exchange was %d, want PROCEED", name, got)
		}
	}
}

// TestF105NewWorkNeedsServingAuthority is the other half: the authority governs beginning,
// and only beginning.
func TestF105NewWorkNeedsServingAuthority(t *testing.T) {
	for name, state := range map[string]mqttAuthorityState{
		"preparing": mqttAuthorityPreparing,
		"closed":    mqttAuthorityClosed,
	} {
		got, err := mqttCompletionAllowed(mqttCompletionInput{
			Op: mqttOpAcquireMore, Auth: testF105Authority(state),
			Exchange: testF105Exchange(2, mqttExchangePublish),
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != mqttCompletionRefuse {
			t.Fatalf("a %s authority permitted NEW work (%d), want REFUSE", name, got)
		}
	}
	got, err := mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpAcquireMore, Auth: testF105Authority(mqttAuthorityServing),
		Exchange: testF105Exchange(2, mqttExchangePublish),
	})
	if err != nil {
		t.Fatalf("serving: %v", err)
	}
	if got != mqttCompletionProceed {
		t.Fatalf("a serving authority refused new work (%d)", got)
	}
	// No authority at all is not permission.
	got, _ = mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpAcquireMore, Auth: nil, Exchange: testF105Exchange(2, mqttExchangePublish),
	})
	if got != mqttCompletionRefuse {
		t.Fatalf("a missing authority permitted new work (%d): absence is not permission", got)
	}
}

// TestF105OutstandingOutputWithholdsRelease pins the drain.
//
// Terminal evidence observed while output is outstanding must not release the exchange: an
// action acquired earlier may still be executing, and releasing here would let a late first
// transmission escape after its own release boundary.
func TestF105OutstandingOutputWithholdsRelease(t *testing.T) {
	auth := testF105Authority(mqttAuthorityServing)

	for name, phase := range map[string]mqttExchangePhase{
		"terminal":               mqttExchangeTerminal,
		"terminal pending drain": mqttExchangeTerminalPendingDrain,
	} {
		got, err := mqttCompletionAllowed(mqttCompletionInput{
			Op: mqttOpReleaseTerminal, Auth: auth,
			Exchange: testF105Exchange(2, phase), Outstanding: true,
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != mqttCompletionAwaitDrain {
			t.Fatalf("a %s exchange with outstanding output was %d, want AWAIT DRAIN", name, got)
		}
	}

	// A retransmission must not race the transmission it repeats, and the release after a
	// PUBREC waits for the PUBLISH to drain.
	for name, in := range map[string]mqttCompletionInput{
		"a retransmission": {
			Op: mqttOpAcquireMore, Auth: auth,
			Exchange: testF105Exchange(2, mqttExchangePublish), Outstanding: true,
		},
		"a release after PUBREC": {
			Op: mqttOpAcquireMore, Auth: auth,
			Exchange: testF105Exchange(2, mqttExchangePubRecObserved), Outstanding: true,
		},
	} {
		got, err := mqttCompletionAllowed(in)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got != mqttCompletionAwaitDrain {
			t.Fatalf("%s with outstanding output was %d, want AWAIT DRAIN", name, got)
		}
	}

	// A non-terminal exchange is not releasable at all, drained or not.
	got, _ := mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpReleaseTerminal, Auth: auth,
		Exchange: testF105Exchange(2, mqttExchangePublish),
	})
	if got != mqttCompletionRefuse {
		t.Fatalf("a non-terminal exchange was releasable (%d)", got)
	}
}

// TestF105PastPubRecNoFurtherPublish pins that a successful PUBREC ends this broker's right
// to send the PUBLISH again: the client has taken the message, and what remains is the
// release.
func TestF105PastPubRecNoFurtherPublish(t *testing.T) {
	got, err := mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpAcquireMore, Auth: testF105Authority(mqttAuthorityServing),
		Exchange: testF105Exchange(2, mqttExchangePubRecObserved),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != mqttCompletionProceed {
		t.Fatalf("the release after a drained PUBREC was %d, want PROCEED", got)
	}

	// A QoS 1 exchange owes no release at all.
	got, err = mqttCompletionAllowed(mqttCompletionInput{
		Op: mqttOpMaterializeOwed, Auth: testF105Authority(mqttAuthorityServing),
		Exchange: testF105Exchange(1, mqttExchangePublish),
	})
	if err == nil && got == mqttCompletionProceed {
		t.Fatalf("a QoS 1 exchange was permitted to materialize a release")
	}
}
