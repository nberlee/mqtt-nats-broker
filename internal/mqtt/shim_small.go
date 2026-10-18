// Small behavioral shims that replace NATS-server internal types the MQTT core
// depends on. These are re-implemented (not aliased) because the core uses their
// unexported members. See docs/PORTING.md.

package mqtt

import "sync"

// msgHandler mirrors nats-server's delivery-callback signature. The MQTT core
// registers these on subscriptions; the connection layer invokes them when a
// NATS message is delivered for a subscribed subject.
type msgHandler func(sub *subscription, c *client, acc *Account, subject, reply string, rmsg []byte)

// subscription is the adapter the MQTT core threads through its subscribe and
// delivery paths. In the standalone broker it wraps the routing primitive
// (a nats.go subscription, attached by the connection layer) and carries the
// MQTT-specific metadata in mqtt (*mqttSub, defined in sub_process.go).
type subscription struct {
	client  *client
	subject []byte
	queue   []byte
	sid     []byte
	icb     msgHandler
	shadow  []*subscription
	mqtt    *mqttSub

	// nsub is the underlying routing handle set by the connection layer; kept
	// as any here to avoid importing nats.go into the protocol core.
	nsub any
}

// Account identifies the NATS account the broker operates within. The standalone
// broker uses a single account (see docs/PORTING.md), so this is a thin holder.
type Account struct {
	name string
}

// GetName returns the account name.
func (a *Account) GetName() string {
	if a == nil {
		return _EMPTY_
	}
	return a.name
}

// hasMappings reports whether the account has subject mappings configured.
// The standalone broker does not use account mappings (v1).
func (a *Account) hasMappings() bool { return false }

// ipQueue is a minimal multi-producer/single-consumer queue matching the subset
// of the nats-server ipQueue API used by the MQTT core: push, pop, recycle, len,
// and a notification channel (ch). It is intentionally simpler than upstream
// (no size accounting or recycle pool tuning) while preserving semantics.
type ipQueue[T any] struct {
	mu   sync.Mutex
	ch   chan struct{}
	elts []T
	name string
}

func newIPQueue[T any](s *Server, name string) *ipQueue[T] {
	q := &ipQueue[T]{ch: make(chan struct{}, 1), name: name}
	if s != nil {
		s.ipQueues.Store(name, q)
	}
	return q
}

// push appends an element and signals the notification channel. It returns the
// resulting queue length.
func (q *ipQueue[T]) push(e T) int {
	q.mu.Lock()
	q.elts = append(q.elts, e)
	n := len(q.elts)
	q.mu.Unlock()
	if n == 1 {
		select {
		case q.ch <- struct{}{}:
		default:
		}
	}
	return n
}

// pop returns all currently-queued elements and clears the queue.
func (q *ipQueue[T]) pop() []T {
	q.mu.Lock()
	elts := q.elts
	q.elts = nil
	q.mu.Unlock()
	return elts
}

// recycle allows the caller to hand back a drained slice. The simplified queue
// does not pool, so this just clears the slice header the caller holds.
func (q *ipQueue[T]) recycle(elts *[]T) {
	if elts != nil {
		*elts = (*elts)[:0]
	}
}
