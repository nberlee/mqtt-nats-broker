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

import (
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
	// established is closed when cert is recorded. A caller that LOSES the seal race must
	// wait on it: crossing the writer barrier proves no write is in progress, but says
	// nothing about whether the winner has finished recording the fact, and a loser that
	// returned nil there would report "no certificate" for a connection that is certified
	// a microsecond later. That is the same absence-is-not-a-fact error in a new place.
	established chan struct{}
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
// TODAY THE CERTIFICATE IS PROCESS-LOCAL and is not persisted, so the confusion is not
// reachable — a restarted broker holds no bindings at all and can certify nothing. The token
// is here because persisting certificates is WP2 work tied to the dependency inventory, and
// a certificate that reached durable state without naming its run would be unattributable
// exactly when it started to matter.
var mqttBrokerRunID = nuid.Next()

// mqttNewConnBinding mints the binding for one accepted connection.
func mqttNewConnBinding(run, conn string) *mqttConnBinding {
	return &mqttConnBinding{run: run, conn: conn, established: make(chan struct{})}
}

// mqttBindConnection mints this run's binding for one accepted connection.
func mqttBindConnection(cid uint64) *mqttConnBinding {
	return mqttNewConnBinding(mqttBrokerRunID, strconv.FormatUint(cid, 10))
}

// mayWrite reports whether an action holding this binding may still emit bytes.
//
// Checked by the writer under its own write lock, so a sealer cannot slip between the check
// and the write: see certifyClosed, which crosses that lock precisely to establish that
// every delayed writer has observed the seal.
func (b *mqttConnBinding) mayWrite() bool { return b.gate.open() }

// closeCert returns the established certificate, or nil.
//
// NIL IS NOT "STILL OPEN". It means no certificate has been established here, which is the
// state a broker is in for every connection it does not own. A caller that read nil and
// concluded the connection was alive would be inferring from an absence.
func (b *mqttConnBinding) closeCert() *mqttCloseCert {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cert
}

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
func completeCertification(b *mqttConnBinding, reason string) *mqttCloseCert {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cert == nil {
		b.cert = &mqttCloseCert{Run: b.run, Conn: b.conn, Reason: reason}
		close(b.established)
	}
	return b.cert
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
	if c != nil && c.rawConn != nil {
		_ = c.rawConn.Close()
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

// mqttBindingCertifies reports whether cert is a certificate FOR this binding.
//
// A certificate names a run and a connection, and a caller holding one must check that it
// describes the connection whose output it is reasoning about. Accepting any certificate
// would let one connection's closure be read as another's — the cross-identity mistake this
// project has paid for repeatedly in other shapes.
func mqttBindingCertifies(b *mqttConnBinding, cert *mqttCloseCert) bool {
	return b != nil && cert != nil && cert.Run == b.run && cert.Conn == b.conn
}
