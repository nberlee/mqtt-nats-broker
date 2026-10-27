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
	"sync"
	"time"
)

// THE IDENTIFIER WALK REMEMBERS WHAT A DUTY POSITION IS ABOUT, never what state it is in.
//
// Identifier exclusivity walks a generation's inventory [Lo, Hi] at the write boundary of every
// admission (mqttTraverseInventoryForIdentifiers), which made each admission cost one duty read
// per open position: O(span) during a burst. A position's IDENTITY — its duty kind, the exchange
// or resource it names, and an exchange duty's identifier — is written once, where the position
// is allocated, and the boundary refuses any continuation that re-identifies it
// (checkDutyContinuation) or any floor batch that does (checkFloorAdvance); a relocation writes a
// NEW ordinal. So an identity validated once stays that position's identity while the position is
// in the inventory, and the walk reads only positions it has not validated before. Everything
// else is read fresh on every walk: the ledger (its sequence is what the admission's ledger CAS
// is checked against), and the exchange of every duty naming an identifier asked about. Nothing
// about a duty's state, an exchange's phase, an absence or an availability is cached.
//
// The cost, stated as a contract change (docs/ARCHITECTURE.md): a cached position is not
// re-validated, so a later out-of-protocol rewrite or corruption of that duty record is not
// re-detected by this walk. A restore of the stream underneath running brokers would invalidate
// it the same way; a restore requires every broker stopped, which drops every cache.
//
// Scoped by (client, account recovery epoch, generation) and ordinal, so another epoch's or
// generation's positions never answer; bounded (generations, positions per generation, idle
// age); pruned below the floor (pruning is never completion: a miss is a read); and every fill
// is conditional on the generation's version, which a prune bumps and an eviction removes, so a
// walk in flight across either cannot repopulate what was dropped.

const (
	// mqttDutyIdentityGens bounds the generations cached per account; the least recently walked
	// is evicted first.
	mqttDutyIdentityGens = 64
	// mqttDutyIdentityPositions bounds the positions cached per generation; past it, positions are
	// read rather than cached.
	mqttDutyIdentityPositions = 1 << 16
	// mqttDutyIdentityIdle evicts a generation not walked for this long: one whose client moved
	// to another broker or whose generation was discarded elsewhere is never walked here again.
	mqttDutyIdentityIdle = 10 * time.Minute
)

// mqttDutyIdentity is what a duty position is about: written once, at allocation.
type mqttDutyIdentity struct {
	kind mqttDutyKind
	ref  string
	pi   uint16
}

// mqttDutyIdentityGen is one generation's cached positions.
type mqttDutyIdentityGen struct {
	version uint64
	walked  time.Time
	pos     map[uint64]mqttDutyIdentity
}

// mqttDutyIdentityCache is the account's cache, shared by every view of its assistant.
type mqttDutyIdentityCache struct {
	mu   sync.Mutex
	gens map[string]*mqttDutyIdentityGen
}

// mqttDutyIdentityKey scopes a generation's positions.
func mqttDutyIdentityKey(id, epoch, gen string) string {
	return id + "\x00" + epoch + "\x00" + gen
}

// mqttDutyIdentityWalk is one walk's use of the cache: the identities it may use, and the ones it
// validated and offers back.
type mqttDutyIdentityWalk struct {
	key     string
	version uint64
	known   map[uint64]mqttDutyIdentity
	fills   map[uint64]mqttDutyIdentity
}

// begin opens a walk over [lo, hi]: positions below lo are pruned (bumping the version), the
// cached identities inside the range are copied out, and the version a fill must still find is
// captured.
func (c *mqttDutyIdentityCache) begin(id, epoch, gen string, lo, hi uint64, now time.Time) *mqttDutyIdentityWalk {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gens == nil {
		c.gens = make(map[string]*mqttDutyIdentityGen)
	}
	for k, g := range c.gens {
		if now.Sub(g.walked) > mqttDutyIdentityIdle {
			delete(c.gens, k)
		}
	}
	key := mqttDutyIdentityKey(id, epoch, gen)
	g := c.gens[key]
	if g == nil {
		if len(c.gens) >= mqttDutyIdentityGens {
			var oldest string
			for k, og := range c.gens {
				if oldest == _EMPTY_ || og.walked.Before(c.gens[oldest].walked) {
					oldest = k
				}
			}
			delete(c.gens, oldest)
		}
		g = &mqttDutyIdentityGen{pos: make(map[uint64]mqttDutyIdentity)}
		c.gens[key] = g
	}
	g.walked = now
	pruned := false
	for ord := range g.pos {
		if ord < lo {
			delete(g.pos, ord)
			pruned = true
		}
	}
	if pruned {
		g.version++
	}
	w := &mqttDutyIdentityWalk{key: key, version: g.version, known: make(map[uint64]mqttDutyIdentity)}
	for ord, d := range g.pos {
		if ord <= hi {
			w.known[ord] = d
		}
	}
	return w
}

// lookup answers a cached identity of position ord; a nil walk caches nothing.
func (w *mqttDutyIdentityWalk) lookup(ord uint64) (mqttDutyIdentity, bool) {
	if w == nil {
		return mqttDutyIdentity{}, false
	}
	d, ok := w.known[ord]
	return d, ok
}

// offer records an identity the walk read and validated itself.
func (w *mqttDutyIdentityWalk) offer(ord uint64, d *mqttDutyEntry) {
	if w == nil || d == nil {
		return
	}
	if w.fills == nil {
		w.fills = make(map[uint64]mqttDutyIdentity)
	}
	w.fills[ord] = mqttDutyIdentity{kind: d.Kind, ref: d.Ref, pi: d.PI}
}

// finish stores the walk's validated identities, only if nothing was pruned or evicted since it
// began.
func (c *mqttDutyIdentityCache) finish(w *mqttDutyIdentityWalk) {
	if w == nil || len(w.fills) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.gens[w.key]
	if g == nil || g.version != w.version {
		return
	}
	for ord, d := range w.fills {
		if len(g.pos) >= mqttDutyIdentityPositions {
			return
		}
		g.pos[ord] = d
	}
}
