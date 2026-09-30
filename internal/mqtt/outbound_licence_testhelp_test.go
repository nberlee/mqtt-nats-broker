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

// Test rigs that drive a destructive or cancelling path directly, without a CONNECT, need the
// licence a real connection would carry (F-105 7b): the entitlement the accept loop records on the
// connection and, beside it, on the session object (boundEnt). These helpers bind a rig exactly as
// the accept loop does and return that connection's licence. They never widen a licence: a rig
// whose connection already carries an entitlement gets THAT one; otherwise the serving authority's
// generation and tenure; otherwise (a rig with no authority at all) a synthetic entitlement, which
// the owner-dependent ops then refuse at their own authority read, exactly as they refused before
// without one.

// mqttTestLicence returns the licence of the rig's accepted connection, and records it as the
// session object's boundEnt when that is not yet set. It never BLOCKS on sess.mu: several helpers
// the rigs call are invoked with the lock already held by the test (stageDrain, for one), so the
// entitlement is resolved from the connection and the store, and boundEnt is recorded only when
// the lock is free.
func mqttTestLicence(sess *mqttSession, c *client) mqttDestroyLicence {
	ent := mqttTestBindEntitlement(sess, c)
	if sess.mu.TryLock() {
		if !sess.boundEntOK {
			sess.boundEnt, sess.boundEntOK = ent, true
		}
		sess.mu.Unlock()
	}
	return mqttEntitlementLicence(ent, true)
}

// mqttTestLicenceLocked is mqttTestLicence for a caller that holds sess.mu.
func mqttTestLicenceLocked(sess *mqttSession, c *client) mqttDestroyLicence {
	ent := mqttTestBindEntitlement(sess, c)
	if !sess.boundEntOK {
		sess.boundEnt, sess.boundEntOK = ent, true
	}
	return mqttEntitlementLicence(sess.boundEnt, true)
}

// mqttTestBindEntitlement resolves the entitlement a rig's connection holds or would have been
// accepted under, and records it on c when c has none. Session lock state irrelevant: it reads
// only the connection and the store.
func mqttTestBindEntitlement(sess *mqttSession, c *client) mqttOutboundEntitlement {
	if ent, ok := c.outboundEntitlement(); ok {
		return ent
	}
	ent := mqttOutboundEntitlement{Gen: sess.gen, Tenure: "test-tenure"}
	if sess.jsa != nil && sess.asm != nil {
		if load := sess.jsa.loadOutboundAuthority(sess.id, sess.asm.sessStreamEpoch); load.status == mqttOutboundValid &&
			load.rec != nil && load.rec.State == mqttAuthorityServing {
			ent = mqttOutboundEntitlement{Gen: load.rec.Gen, Tenure: load.rec.Tenure}
		}
	}
	if ent.Gen == _EMPTY_ {
		ent.Gen = "test-gen"
	}
	c.setOutboundEntitlement(ent)
	return ent
}

// testHookAcceptedClient is, for a rig that drives the whole store through the account manager's
// hooks (no JetStream behind sess.jsa), the connection a CONNECT would have accepted: it carries an
// entitlement, the session object records the same one as accepted, and the resume's durable
// authority read (testResumeAuthorityLoad) reports that entitlement as the serving one — as the
// accept loop's own finalize would have left it.
func testHookAcceptedClient(as *mqttAccountSessionManager, sess *mqttSession) *client {
	ent := mqttOutboundEntitlement{Gen: sess.gen, Tenure: "hook-tenure"}
	if ent.Gen == _EMPTY_ {
		ent.Gen = "hook-gen"
	}
	c := &client{mqtt: &mqtt{asm: as, sess: sess}}
	c.setOutboundEntitlement(ent)
	sess.boundEnt, sess.boundEntOK = ent, true
	as.mu.Lock()
	as.testResumeAuthorityLoad = func(id string) mqttAuthorityLoad {
		return mqttAuthorityLoad{status: mqttOutboundValid, rec: &mqttOutboundAuthority{
			ID: id, Gen: ent.Gen, Tenure: ent.Tenure, State: mqttAuthorityServing,
		}}
	}
	as.mu.Unlock()
	return c
}

// testHookSeedCustody gives a hook-driven rig the custody ledger its resume census would have
// built from the exchanges beside recs (F-105 7b): each record's lineage held, charged, on its
// identifier. A rig with no JetStream cannot run the inventory traversal, and the resume's install
// gate admits only a lineage the ledger holds, exactly as it does in production.
func testHookSeedCustody(sess *mqttSession, recs ...*mqttInflightRecord) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.custody == nil {
		sess.custody = newMQTTCustodyLedger(sess.gen)
	}
	for _, r := range recs {
		sess.custody.ensureExchangeLocked(r.Lin, r.PI)
	}
}
