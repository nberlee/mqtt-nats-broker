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

// joinSharedForTest performs the production J1 acquisition for one group (ShareName) and subject,
// under a minted shared tenure, and records the membership the delivery callback selects from, so
// a shared delivery can be admitted the way production admits one. It returns the acquisition.
//
// A key's level-up companion is NOT joined here: callers join each group they deliver on.
func (e *testInflightEnv) joinSharedForTest(t *testing.T, key, group, subject string, up bool) mqttSharedAcqRef {
	t.Helper()
	// A unit env never accepted a CONNECT, so it installs a well-formed tenure the way every
	// acceptance does before its first join (F-105 8b).
	testInstallFixtureTenure(t, e.sess)
	acq, out, err := e.sess.acquireShared(e.c, group, subject, 1024)
	if err != nil || out != mqttSharedAcquired {
		t.Fatalf("acquireShared(%s, %s): outcome %v, err %v", group, subject, out, err)
	}
	e.sess.mu.Lock()
	if e.sess.sharedSubs == nil {
		e.sess.sharedSubs = make(map[string]mqttSharedMembership)
	}
	m := e.sess.sharedSubs[key]
	m.QoS = 1
	if up {
		m.Up = &mqttSharedAcquisition{Inc: acq.Inc, Acq: acq.Acq}
	} else {
		m.Main = acq
	}
	e.sess.sharedSubs[key] = m
	e.sess.mu.Unlock()
	return mqttSharedAcqRef{Group: mqttSharedGroupKey(group, subject), Inc: acq.Inc, Acq: acq.Acq}
}

// trackSharedAdmitted is trackAdmitted for a SHARED QoS 1 delivery (7a): the serving authority and
// the connection's entitlement, the group acquisition already joined (joinSharedForTest), then the
// allocation and its admission through admitSharedInflightPublish with the dependency the delivery
// callback would have selected. The register stands beside an exchange naming that acquisition.
func (e *testInflightEnv) trackSharedAdmitted(t *testing.T, dur string, sseq uint64) uint16 {
	t.Helper()
	e.installServingAuthority(t)
	reply := testInflightAckSubject(dur, sseq, 1)
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, reply)
	var dep *mqttAdmitShared
	if acq, ok := e.sess.sharedAcqForDeliveryLocked(dur); ok {
		dep = &mqttAdmitShared{Acq: acq, Tenure: e.sess.sharedTenure}
	}
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish(%s/%d): pi=%d isNew=%v", dur, sseq, pi, isNew)
	}
	if dep == nil {
		t.Fatalf("no held acquisition selects %q: join the group first", dur)
	}
	e.acceptUnderServingAuthority(t)
	if !e.sess.admitSharedInflightPublish(e.c, pi, 1, dur, sseq, e.nonceOf(pi), dep) {
		t.Fatalf("admitSharedInflightPublish(%d, %s/%d) refused", pi, dur, sseq)
	}
	return pi
}

// testConfirmAckRaw is a test's own confirmed acknowledgement, below the positive source-ACK gate:
// the fixtures that stand in for JetStream's confirmation call it from inside a seam the gate has
// already been consulted for. Production has no ungated confirmed acknowledgement.
func (jsa *mqttJSA) testConfirmAckRaw(ackSubject string) error {
	p := jsa.prepareAckConfirm(ackSubject)
	p.enqueue()
	return p.wait()
}

// trackSharedByGroupKey is track for a SHARED durable named only by its group key and incarnation:
// it joins that group for this generation (once per incarnation, through the production join
// composer), then admits the delivery through the authority with that acquisition as its
// dependency. Supported shared delivery is QoS 1.
func (e *testInflightEnv) trackSharedByGroupKey(t *testing.T, dur string, sseq uint64, qos byte) uint16 {
	t.Helper()
	if qos != 1 {
		t.Fatalf("fixture error: a shared delivery at QoS %d is not a supported shape", qos)
	}
	tail := dur[len(mqttSharedConsumerDurablePrefix):]
	qkey, inc, ok := mqttParseSharedIncName(tail)
	if !ok {
		t.Fatalf("fixture error: %q is not a well-formed group durable", dur)
	}
	e.installServingAuthority(t)
	e.acceptUnderServingAuthority(t)
	testInstallFixtureTenure(t, e.sess)
	e.sess.mu.Lock()
	tenure, id, gen, epoch := e.sess.sharedTenure, e.sess.id, e.sess.gen, e.sess.sharedEpoch()
	e.sess.mu.Unlock()
	var ref mqttSharedAcqRef
	if l := e.sess.jsa.loadSharedLedger(id, epoch, gen); l.status == mqttOutboundValid {
		for _, a := range l.rec.Acqs {
			if a.Group == qkey && a.Inc == inc {
				ref = a
			}
		}
	}
	if ref.Acq == _EMPTY_ {
		g := e.sess.jsa.loadSharedGroup(epoch, qkey)
		l := e.sess.jsa.loadSharedLedger(id, epoch, gen)
		b, err := mqttComposeSharedJoin(epoch, id, gen, qkey, mqttNewInflightNonce(), tenure, g, l, 64)
		if err != nil {
			t.Fatalf("composing the join of %q: %v", qkey, err)
		}
		if res := e.sess.jsa.commitSharedLifecycleBatch(b); res.Outcome != mqttSharedWriteLanded {
			t.Fatalf("joining %q: %v", qkey, res.Err)
		}
		if b.Acq.Inc != inc {
			t.Fatalf("fixture error: the group serves incarnation %d, the durable names %d", b.Acq.Inc, inc)
		}
		ref = b.Acq
	}
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish(%s/%d): pi=%d isNew=%v", dur, sseq, pi, isNew)
	}
	if !e.sess.admitSharedInflightPublish(e.c, pi, 1, dur, sseq, e.nonceOf(pi), &mqttAdmitShared{Acq: ref, Tenure: tenure}) {
		t.Fatalf("admitSharedInflightPublish(%d, %s/%d) refused", pi, dur, sseq)
	}
	return pi
}

// trackUnsupportedSharedShape writes a shared register through the PRE-7a writer, with no exchange —
// a shape the broker no longer writes. Only for tests that pin a still-present arm over a population
// 7a made impossible (a retired shared QoS 2 register); every other shared fixture uses track.
func (e *testInflightEnv) trackUnsupportedSharedShape(t *testing.T, dur string, sseq uint64, qos byte) uint16 {
	t.Helper()
	e.sess.mu.Lock()
	pi, isNew := e.sess.trackPublish(dur, testInflightAckSubject(dur, sseq, 1))
	e.sess.mu.Unlock()
	if pi == 0 || !isNew {
		t.Fatalf("trackPublish(%s/%d): pi=%d isNew=%v", dur, sseq, pi, isNew)
	}
	testOpenStoreCustody(t, e.sess, pi)
	if !e.sess.storeInflightRegister(e.c, pi, qos, dur, sseq, e.nonceOf(pi), _EMPTY_, _EMPTY_) {
		t.Fatalf("storeInflightRegister(%d, %s/%d) refused", pi, dur, sseq)
	}
	return pi
}
