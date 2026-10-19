package mqtt

import (
	"strconv"
	"testing"
)

func benchRecord(n int) *mqttPersistedSession {
	ps := &mqttPersistedSession{Subs: make(map[string]byte, n), Cons: make(map[string]*ConsumerConfig, n)}
	for i := 0; i < n; i++ {
		k := "bench.filter." + strconv.Itoa(i)
		ps.Subs[k] = 1
		ps.Cons[k] = &ConsumerConfig{Durable: "d" + strconv.Itoa(i), DeliverSubject: "x", MaxAckPending: 1024}
	}
	return ps
}

// BenchmarkGrantMaterialise is why the grant is materialised on the FORK path and never on
// the save path.
//
// An earlier revision built this on every save ATTEMPT — rejected attempts and retries
// included — while holding sess.mu. The cost is O(grant size), and with no per-session
// subscription limit the grant is unbounded: at 1000 filters it is milliseconds and megabytes per attempt, which is
// a production defect and not merely a slow test. Measured, so a future change that moves
// materialisation back onto a hot path has a number to answer.
func BenchmarkGrantMaterialise(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		ps := benchRecord(n)
		b.Run("filters="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = mqttGrantFromRecord(ps)
			}
		})
	}
}

// BenchmarkGrantBodyRotation is what a landed save actually pays: two slice assignments,
// independent of grant size, with no allocation. The bytes are already marshalled by the save
// that is landing.
func BenchmarkGrantBodyRotation(b *testing.B) {
	sess := &mqttSession{}
	body := make([]byte, 4096)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sess.adoptGrantBodyLocked(body)
	}
}
