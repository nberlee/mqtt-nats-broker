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
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nuid"
)

// The immutable connection binding (F-105).
//
// THE SOCKET EFFECT CANNOT BE FENCED BY A JETSTREAM CAS. Everything else in F-105 is
// arbitrated durably, but bytes already handed to a transport are not: an action acquired
// before a retirement may still be executing when the retirement wins, and no metadata
// protocol can reach into another process and stop a write. The invariant is therefore
// stated over DURABLE state — retirement must not destroy the representation or completion
// route of an obligation admitted before it — and a previously acquired action is
// explicitly permitted to execute afterwards, on its ORIGINAL connection, with its original
// exchange and packet-identifier binding.
//
// What this file supplies is the part that IS reachable: a way to say, positively, that one
// connection can produce no further output. That is the only thing "the old owner has
// stopped" can honestly mean here, and it is deliberately narrow.
//
//   - It is not `c.closed`, which records an intent to tear down.
//   - It is not a write deadline expiring, which says one write failed.
//   - It is not read EOF, which says the peer stopped sending.
//   - It is not disappearance from a registry, which says this process lost track.
//   - Only the process that OWNS the connection can certify it. A restarted broker cannot
//     certify another run's sockets merely because it cannot find them, and a timeout is
//     not closure. Where no certificate is obtainable the obligation stays outstanding,
//     which is a leak and is the correct trade against destroying a live exchange.
//
// A certificate says the connection can emit nothing more. It does NOT say the bytes
// already written were unreceived, and it does NOT say the resource duties that exchange
// owns have finished.

// mqttOutputGate is one connection's permanently sealable output permission.
//
// Sealing is one-way. A gate that could reopen would make the certificate a statement about
// a moment rather than about the rest of the connection's life, and every reader of it would
// have to re-check — which is the read-then-act shape this item exists to remove.
type mqttOutputGate struct {
	sealed atomic.Bool
}

// open reports whether output is still permitted.
func (g *mqttOutputGate) open() bool { return !g.sealed.Load() }

// seal closes the gate permanently and reports whether this call was the one that closed it.
//
// The caller that seals is the caller that owes the certificate, so the distinction matters:
// a second sealer must not conclude it has established anything.
func (g *mqttOutputGate) seal() bool { return g.sealed.CompareAndSwap(false, true) }

// mqttConnBinding is the immutable identity of one connection's output side.
//
// Immutable is the whole point. A resident reconnect REBINDS the session object, so a
// completion captured against "the session's current connection" would follow the rebind and
// resolve an old acknowledgement against a successor's state. Every durable action captures
// this value at acquisition and keeps it; nothing here is ever reassigned.
type mqttConnBinding struct {
	// run is this broker run's identity, and conn this connection's. Together they name a
	// connection across the cluster, which a pointer cannot.
	run  string
	conn string

	gate mqttOutputGate

	mu sync.Mutex
	// cert is the durable closure certificate, present once sealing has completed its
	// barrier. Nil means no certificate has been established — NOT that the connection is
	// alive.
	cert *mqttCloseCert
	// established is closed when cert is recorded, for an observer that selects on the fact.
	// No certifier waits on it: completion is owner-free (completeCertification).
	established chan struct{}
	// persist is where this connection's certificate is made durable (F-105 8c): the client
	// generation it first attempted an acquisition under, captured BEFORE that acquisition's
	// batch was sent, so an acquisition whose answer was lost still has its certificate
	// persisted. Nil for a connection that never attempted one — a refused CONNECT, a
	// subscribe-only client — which can have left no token for a certificate to clear.
	persist *mqttCertPersistCtx
	// persistLaunched marks that the one persistence attempt has been started, by whichever of
	// the certifier and a late registrar saw both the certificate and the context first. It
	// suppresses a duplicate; nobody waits on it.
	persistLaunched bool
}

// mqttCloseCert records that one connection can produce no further output.
type mqttCloseCert struct {
	Run  string `json:"run"`
	Conn string `json:"conn"`
	// Reason is why the connection was closed, for diagnosis only. It carries no
	// authority: a certificate is established by the barrier, never by its reason.
	Reason string `json:"reason"`
}

// mqttBrokerRunID identifies THIS PROCESS's run, minted once at start.
//
// Not the node name, which survives a restart. A certificate says "this connection can emit
// nothing more", and only the process holding the socket can establish that; a run token is
// what keeps a restarted broker's certificate from being confusable with its predecessor's
// for the same node and connection number.
//
// It names the run in a persisted certificate (F-105 8c) for diagnosis. It is NOT what makes the
// certificate's connection unique: one process may construct several servers, each numbering
// its connections from one, so the connection token is minted unique on its own
// (mqttBindConnection).
var mqttBrokerRunID = nuid.Next()

// mqttNewConnBinding mints the binding for one accepted connection.
func mqttNewConnBinding(run, conn string) *mqttConnBinding {
	return &mqttConnBinding{run: run, conn: conn, established: make(chan struct{})}
}

// mqttBindConnection mints this run's binding for one accepted connection.
//
// THE CONNECTION TOKEN IS MINTED, NOT DERIVED. A persisted certificate ends the live token of
// every action naming its connection (F-105 8c), so two sockets must never share a name: the
// connection number is unique only within one server, and a second server in the same process
// — or a replacement server in a long-lived one — numbers its connections from one again. The
// number is kept as a prefix for diagnosis; the minted suffix is what makes the name unique.
func mqttBindConnection(cid uint64) *mqttConnBinding {
	return mqttNewConnBinding(mqttBrokerRunID, strconv.FormatUint(cid, 10)+"-"+nuid.Next())
}

// mayWrite reports whether an action holding this binding may still emit bytes.
//
// Checked by the writer under its own write lock, so a sealer cannot slip between the check
// and the write: see certifyClosed, which crosses that lock precisely to establish that
// every delayed writer has observed the seal.
func (b *mqttConnBinding) mayWrite() bool { return b.gate.open() }

// sealOutput seals a binding's output gate, and NOTHING else. It is the first half of
// certification, split out for the ONE caller that already holds wmu — writeAuthorizedFrame,
// torn mid-frame. That caller cannot cross the writer barrier while it holds wmu, so it seals
// here, releases wmu, then crosses the barrier and calls completeCertification itself. An
// external certifier that does NOT hold wmu uses mqttCertifyClosed instead, which seals,
// closes the transport outside wmu, crosses the barrier and completes. The two differ only in
// who holds wmu at the seal; both converge on completeCertification.
func sealOutput(b *mqttConnBinding) {
	if b == nil {
		return
	}
	b.gate.seal()
}

// completeCertification records the closure certificate if none stands yet, and returns the
// standing one. IDEMPOTENT and OWNER-FREE: whoever reaches it first — the torn writer, an
// external teardown, a second concurrent certifier — records the fact, and every later caller
// adopts the same certificate. Nobody WAITS. The earlier design had a caller that lost the
// seal block on `established` for the winner to record it, but a bare gate.seal() (a torn
// writer sealing under wmu) left no caller obligated to record anything, so that wait
// deadlocked. Here the caller that finds no certificate IS the one that records it.
//
// The caller MUST have crossed the writer barrier first: a certificate asserts "no write is
// in progress on this connection", which only the barrier establishes. Both entry paths cross
// it before calling. `established` is closed exactly once, under the same lock that records
// the certificate, so an observer selecting on it still unblocks.
//
// Whoever records the certificate also starts its persistence when the connection has
// attempted an acquisition, outside the lock and off the caller's path (mqttPersistCloseCert),
// so the torn writer and an external certifier persist alike and neither waits on the other.
func completeCertification(b *mqttConnBinding, reason string) *mqttCloseCert {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.cert == nil {
		b.cert = &mqttCloseCert{Run: b.run, Conn: b.conn, Reason: reason}
		close(b.established)
	}
	cert := b.cert
	ctx, joined := b.claimPersistLocked()
	b.mu.Unlock()
	if ctx != nil {
		if h := mqttTestCertClaimedV.Load(); h != nil {
			(*h)(b)
		}
	}
	mqttPersistCloseCert(ctx, joined, b.run, b.conn)
	return cert
}

// claimPersistLocked returns the persistence context exactly once, and only once both the
// certificate and the context stand. b.mu held.
//
// The shutdown join's token is taken HERE, in the same critical section that marks the
// attempt launched, and not where its goroutine starts: a certifier that claimed the attempt
// and paused before launching it would otherwise hold a write no join can see, and every
// other certifier finds the attempt already claimed. joined reports whether the token was
// taken; it is not once the join has closed, and the attempt then runs unjoined.
func (b *mqttConnBinding) claimPersistLocked() (ctx *mqttCertPersistCtx, joined bool) {
	if b.cert == nil || b.persist == nil || b.persistLaunched {
		return nil, false
	}
	b.persistLaunched = true
	return b.persist, b.persist.srv.admitCloseCertWrite()
}

// registerAcquisition records, BEFORE an acquisition's batch is sent, where this connection's
// certificate is to be persisted (F-105 8c).
//
// Before the send, not on its answer: an acquisition whose response is lost may have installed
// its token, and that token needs the certificate exactly as much as one seen to land. ONE
// generation per connection — the entitlement an acquisition runs under is set once — so a second
// context naming another is refused rather than adopted. A connection already certified when it
// registers persists at once: the late registration must not be the one that misses.
func (b *mqttConnBinding) registerAcquisition(ctx *mqttCertPersistCtx) error {
	if b == nil || ctx == nil {
		return fmt.Errorf("an acquisition registers its connection's certificate context, and none was given")
	}
	b.mu.Lock()
	if b.persist != nil && (b.persist.id != ctx.id || b.persist.epoch != ctx.epoch || b.persist.gen != ctx.gen) {
		cur := b.persist
		b.mu.Unlock()
		return fmt.Errorf("connection %s/%s acquires under %q/%q/%q and may not also acquire under %q/%q/%q",
			b.run, b.conn, cur.id, cur.epoch, cur.gen, ctx.id, ctx.epoch, ctx.gen)
	}
	if b.persist == nil {
		b.persist = ctx
	}
	launch, joined := b.claimPersistLocked()
	b.mu.Unlock()
	mqttPersistCloseCert(launch, joined, b.run, b.conn)
	return nil
}

// mqttCertifyClosed is the EXTERNAL entry path — a teardown or a refused CONNECT that does not
// hold wmu. It performs the sequence that makes a certificate true, and returns it.
//
// The order is the proof, and each step is load-bearing:
//
//  1. SEAL THE GATE. After this no writer may begin. Idempotent: it does not matter whether
//     this call or an earlier sealOutput (a torn writer) won the seal.
//  2. CLOSE THE TRANSPORT, outside the write lock. A writer blocked in a socket write holds
//     that lock, so closing under it would deadlock against the very writer being stopped;
//     closing outside it makes that write return.
//  3. CROSS THE WRITER BARRIER by taking the write lock. Any writer that began before the
//     seal has returned by the time this is acquired, and any writer that starts afterwards
//     tests the sealed gate under the same lock and declines. That is what upgrades "no new
//     writes are permitted" into "no write is in progress".
//  4. COMPLETE — record the certificate, or adopt one an earlier caller already recorded.
//
// Owner-free completion is what makes this safe beside a torn writer: the two converge on one
// certificate instead of one blocking on the other. Sealing the socket alone is not a
// substitute for the barrier: it stops future writes and says nothing about the one currently
// executing.
func mqttCertifyClosed(c *client, b *mqttConnBinding, reason string) *mqttCloseCert {
	if b == nil {
		return nil
	}
	b.gate.seal()
	// Outside wmu: a writer blocked in Write holds it, and closing the socket is what makes
	// that write return.
	if c != nil {
		c.closeTransport()
	}
	mqttCrossWriterBarrier(c)
	return completeCertification(b, reason)
}

// mqttCrossWriterBarrier returns once no write is in progress on c.
//
// Acquiring the writer's own lock IS the barrier: a writer that began before the gate was
// sealed has returned by the time this is acquired, and one that starts afterwards tests the
// sealed gate under the same lock and declines. The critical section is empty on purpose —
// the acquisition is the whole operation.
func mqttCrossWriterBarrier(c *client) {
	if c == nil {
		return
	}
	c.wmu.Lock()
	//nolint:staticcheck // SA2001: the acquisition is the barrier; there is nothing to do inside it.
	c.wmu.Unlock()
}
