//go:build mqtt_fulltests

package mqtt

import (
	"bufio"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// The inbound acknowledgement pipeline's decision boundaries (inbound_pipeline.go). Each test
// holds the work its decision depends on at a named seam, positively observes the competing
// event, and asserts on the DECISION RECORD the deciding code made — a record the correct code
// and its mutant both produce — rather than on silence. They arm process-global hooks, so none
// runs in parallel. The paired mutations are listed in docs/ROADMAP.md (the implementation
// bullet) and were each run against these tests.

// inboundPipeFix is one fresh broker with interest on the fixture's subjects and one raw v5
// client.
type inboundPipeFix struct {
	t      *testing.T
	s      *Server
	o      *Options
	asm    *mqttAccountSessionManager
	cid    string
	prefix string
	nc     net.Conn
	br     *bufio.Reader
}

func newInboundPipeFix(t *testing.T, cid string, opt func(*Options)) *inboundPipeFix {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	if opt != nil {
		opt(o)
	}
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	f := &inboundPipeFix{t: t, s: s, o: o, asm: asmOf(t, s), cid: cid, prefix: mqttStreamSubjectPrefix + "pipe."}
	nc, js := jsClientConnect(t, s)
	t.Cleanup(nc.Close)
	if _, err := js.AddConsumer(mqttStreamName, &nats.ConsumerConfig{
		Durable: "inbound-pipe", FilterSubject: f.prefix + ">", AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("interest consumer: %v", err)
	}
	f.nc, f.br = f.dial(cid, nil)
	return f
}

func (f *inboundPipeFix) dial(cid string, will *mqttWill) (net.Conn, *bufio.Reader) {
	f.t.Helper()
	c, mr := testMQTTConnectV5(f.t, &mqttV5ConnInfo{clientID: cid, cleanStart: true, will: will}, f.o.MQTT.Host, f.o.MQTT.Port)
	f.t.Cleanup(func() { c.Close() })
	if _, reason, _ := testMQTTReadConnAckV5(f.t, mr); reason != mqttConnAckRCConnectionAccepted {
		f.t.Fatalf("CONNACK reason %#x", reason)
	}
	return c, bufio.NewReader(c)
}

func (f *inboundPipeFix) topic(pi uint16) string   { return "pipe/" + strconv.Itoa(int(pi)) }
func (f *inboundPipeFix) subject(pi uint16) string { return f.prefix + strconv.Itoa(int(pi)) }

func (f *inboundPipeFix) pub(qos byte, pi uint16) []byte {
	return inboundAckBenchPublish(qos, f.topic(pi), pi, []byte("pipe"))
}

// pubExpiring is a PUBLISH carrying a Message Expiry Interval of secs.
func (f *inboundPipeFix) pubExpiring(qos byte, pi uint16, secs uint32) []byte {
	topic := f.topic(pi)
	props := []byte{mqttPropMessageExpiry, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(props[1:], secs)
	payload := []byte("pipe")
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub | qos<<1)
	w.WriteVarInt(2 + len(topic) + 2 + 1 + len(props) + len(payload))
	w.WriteString(topic)
	w.WriteUint16(pi)
	w.WriteByte(byte(len(props)))
	w.Write(props)
	w.Write(payload)
	return w.Bytes()
}

func (f *inboundPipeFix) dup(b []byte) []byte {
	out := append([]byte(nil), b...)
	out[0] |= mqttPubFlagDup
	return out
}

func (f *inboundPipeFix) rel(pis ...uint16) []byte {
	var b []byte
	for _, pi := range pis {
		b = append(b, mqttPacketPubRel|0x2, 0x2, byte(pi>>8), byte(pi))
	}
	return b
}

func (f *inboundPipeFix) send(pkts ...[]byte) {
	f.t.Helper()
	var all []byte
	for _, b := range pkts {
		all = append(all, b...)
	}
	if _, err := f.nc.Write(all); err != nil {
		f.t.Fatalf("write: %v", err)
	}
}

// next reads the next packet.
func (f *inboundPipeFix) next() (byte, []byte) {
	f.t.Helper()
	_ = f.nc.SetReadDeadline(time.Now().Add(15 * time.Second))
	pt, body, err := inboundAckBenchReadPacket(f.br)
	if err != nil {
		f.t.Fatalf("reading the next packet: %v", err)
	}
	return pt & mqttPacketMask, body
}

// expect requires the next packet to be pt for pi with reason.
func (f *inboundPipeFix) expectReason(pt byte, pi uint16, reason byte) {
	f.t.Helper()
	got, body := f.next()
	if got != pt {
		f.t.Fatalf("received packet %#x (body %x) when %#x %d was owed next", got, body, pt, pi)
	}
	if len(body) < 2 || uint16(body[0])<<8|uint16(body[1]) != pi {
		f.t.Fatalf("received %#x with body %x when %#x %d was owed next", got, body, pt, pi)
	}
	r := mqttReasonSuccess
	if len(body) > 2 {
		r = body[2]
	}
	if r != reason {
		f.t.Fatalf("%#x %d carries reason %#x, want %#x", pt, pi, r, reason)
	}
}

func (f *inboundPipeFix) expect(pt byte, pi uint16) {
	f.t.Helper()
	f.expectReason(pt, pi, mqttReasonSuccess)
}

func (f *inboundPipeFix) expectPingResp() {
	f.t.Helper()
	if got, body := f.next(); got != mqttPacketPingResp {
		f.t.Fatalf("received %#x (body %x) when the PINGRESP was owed", got, body)
	}
}

// expectDisconnect requires a server DISCONNECT and returns its reason.
func (f *inboundPipeFix) expectDisconnect() byte {
	f.t.Helper()
	got, body := f.next()
	if got != mqttPacketDisconnect {
		f.t.Fatalf("received %#x (body %x) when a DISCONNECT was expected", got, body)
	}
	if len(body) == 0 {
		return mqttReasonSuccess
	}
	return body[0]
}

// expectEOF reads until the connection ends and fails on any packet.
func (f *inboundPipeFix) expectEOF(within time.Duration) {
	f.t.Helper()
	_ = f.nc.SetReadDeadline(time.Now().Add(within))
	pt, body, err := inboundAckBenchReadPacket(f.br)
	if err == nil {
		f.t.Fatalf("packet %#x (body %x) arrived where the connection should have ended", pt, body)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		f.t.Fatalf("the connection did not end within %v", within)
	}
}

func (f *inboundPipeFix) mine(j *mqttInboundJob) bool {
	return j != nil && j.p != nil && j.p.c.mqtt != nil && j.p.c.mqtt.cid == f.cid
}

func setInboundHook[T any](t *testing.T, v *atomic.Pointer[T], fn T) {
	v.Store(&fn)
	t.Cleanup(func() { v.Store(nil) })
}

// inboundGate parks one step until opened, and reports the job that reached it.
type inboundGate struct {
	reached chan *mqttInboundJob
	open    chan struct{}
	once    sync.Once
}

func newInboundGate() *inboundGate {
	return &inboundGate{reached: make(chan *mqttInboundJob, 16), open: make(chan struct{})}
}

func (g *inboundGate) park(j *mqttInboundJob) {
	g.reached <- j
	<-g.open
}

func (g *inboundGate) release() { g.once.Do(func() { close(g.open) }) }

func (g *inboundGate) wait(t *testing.T, what string) *mqttInboundJob {
	t.Helper()
	select {
	case j := <-g.reached:
		return j
	case <-time.After(15 * time.Second):
		t.Fatalf("%s was never reached", what)
		return nil
	}
}

// pauseAt parks f's jobs of kind (and identifier, when non-zero) at step.
func (f *inboundPipeFix) pauseAt(kind mqttInboundKind, pi uint16, step string) *inboundGate {
	g := newInboundGate()
	f.t.Cleanup(g.release)
	setInboundHook(f.t, &mqttTestInboundPauseV, func(j *mqttInboundJob, s string) {
		if f.mine(j) && j.kind == kind && s == step && (pi == 0 || j.pi == pi) {
			g.park(j)
		}
	})
	return g
}

// holdStoreResult parks the await of the $MQTT_msgs stores of the given identifiers until
// released, and returns their gate. fail, when set, makes the released result an error.
func (f *inboundPipeFix) holdStoreResult(fail error, pis ...uint16) *inboundGate {
	g := newInboundGate()
	f.t.Cleanup(g.release)
	want := map[string]bool{}
	for _, pi := range pis {
		want[f.subject(pi)] = true
	}
	setInboundHook(f.t, &mqttTestStoreMsgResultHookV, func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if want[subject] {
			g.park(nil)
			if fail != nil {
				return nil, fail
			}
		}
		return resp, err
	})
	return g
}

// recordSubmissions records the $MQTT_msgs subjects of f, in submission order.
func (f *inboundPipeFix) recordSubmissions() func() []string {
	var mu sync.Mutex
	var got []string
	setInboundHook(f.t, &mqttTestStoreMsgHookV, func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, f.prefix) {
			mu.Lock()
			got = append(got, subject)
			mu.Unlock()
		}
		return nil
	})
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...)
	}
}

type inboundRecorded struct {
	p *mqttInboundPipe
	d mqttInboundDecision
}

func (f *inboundPipeFix) recordDecisions(v *atomic.Pointer[func(p *mqttInboundPipe, d mqttInboundDecision)]) chan inboundRecorded {
	ch := make(chan inboundRecorded, 4096)
	setInboundHook(f.t, v, func(p *mqttInboundPipe, d mqttInboundDecision) {
		if p.c.mqtt != nil && p.c.mqtt.cid == f.cid {
			select {
			case ch <- inboundRecorded{p, d}:
			default:
			}
		}
	})
	return ch
}

// awaitDecision returns the first recorded decision satisfying pred.
func awaitDecision(t *testing.T, ch chan inboundRecorded, what string, pred func(mqttInboundDecision) bool) inboundRecorded {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case r := <-ch:
			if pred(r.d) {
				return r
			}
		case <-deadline:
			t.Fatalf("no decision %s was recorded", what)
		}
	}
}

func inboundHas(xs []uint64, x uint64) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// pokeEmitter asks for one emission evaluation of the pipe's existing state, from the test
// goroutine, and returns its record. It selects nothing itself. If another goroutine holds
// the emission, it retries until this call is the one that evaluates.
func pokeEmitter(t *testing.T, p *mqttInboundPipe, ch chan inboundRecorded) mqttInboundDecision {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for len(ch) > 0 {
			<-ch
		}
		p.emit()
		select {
		case r := <-ch:
			return r.d
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("the emitter could not be asked for an evaluation")
	return mqttInboundDecision{}
}

// TestMQTTInboundPipeIssuerTakesTheCursor: with the cursor job (a release whose fences are
// held) unready and a later QoS 1 publication eligible, the issuer takes nothing; the later
// store is submitted only after the release's onward store. Mutation M1 (take a ready
// non-cursor job) records a decision that chose the later job.
func TestMQTTInboundPipeIssuerTakesTheCursor(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-cursor", nil)
	f.send(f.pub(2, 1))
	f.expect(mqttPacketPubRec, 1)

	submitted := f.recordSubmissions()
	fences := f.pauseAt(mqttInboundPubRel, 1, "fences")
	decisions := f.recordDecisions(&mqttTestInboundIssuerDecisionV)
	f.send(f.rel(1), f.pub(1, 2))
	a := fences.wait(t, "the release's fences")
	b := a.seq + 1
	r := awaitDecision(t, decisions, "with the release at the cursor and the publication eligible", func(d mqttInboundDecision) bool {
		return d.Cursor == a.seq && inboundHas(d.Ready, b)
	})
	if r.d.Chosen != 0 {
		t.Fatalf("the issuer took job %d while job %d, unready, was its cursor", r.d.Chosen, a.seq)
	}
	// One more evaluation of the same state, and nothing submitted for the later job.
	r.p.mu.Lock()
	r.p.broadcastLocked()
	r.p.mu.Unlock()
	r = awaitDecision(t, decisions, "after the poke", func(mqttInboundDecision) bool { return true })
	if r.d.Chosen != 0 && r.d.Chosen != a.seq {
		t.Fatalf("the issuer took job %d while job %d, unready, was its cursor", r.d.Chosen, a.seq)
	}
	for _, s := range submitted() {
		if s == f.subject(2) {
			t.Fatal("the later publication's store was submitted before the release's")
		}
	}
	fences.release()
	f.expect(mqttPacketPubComp, 1)
	f.expect(mqttPacketPubAck, 2)
	if got := strings.Join(submitted(), " "); got != f.subject(1)+" "+f.subject(2) {
		t.Fatalf("stores submitted in order %q, want the release's then the publication's", got)
	}
}

// TestMQTTInboundPipeEmissionWritesTheHead: three QoS 1 publications, the first's store
// answer held, the other two complete: the emission decision chooses nothing until the head
// is ready, then writes all three in order. Mutation M2 records a choice of a non-head job.
func TestMQTTInboundPipeEmissionWritesTheHead(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-head", nil)
	hold := f.holdStoreResult(nil, 1)
	readyCh := make(chan *mqttInboundJob, 16)
	setInboundHook(t, &mqttTestInboundResponseReadyV, func(j *mqttInboundJob) {
		if f.mine(j) {
			readyCh <- j
		}
	})
	emits := f.recordDecisions(&mqttTestInboundEmitDecisionV)
	f.send(f.pub(1, 1), f.pub(1, 2), f.pub(1, 3))
	hold.wait(t, "the first store's answer")
	var p *mqttInboundPipe
	for seen := map[uint16]bool{}; !seen[2] || !seen[3]; {
		select {
		case j := <-readyCh:
			seen[j.pi] = true
			p = j.p
		case <-time.After(15 * time.Second):
			t.Fatal("the later publications never completed")
		}
	}
	d := pokeEmitter(t, p, emits)
	if d.Chosen != 0 {
		t.Fatalf("the emitter chose job %d while the head %d was not ready (ready %v)", d.Chosen, d.Cursor, d.Ready)
	}
	hold.release()
	for pi := uint16(1); pi <= 3; pi++ {
		f.expect(mqttPacketPubAck, pi)
	}
}

// recordWrites records, per packet identifier, every response of f's handed to the writer.
func (f *inboundPipeFix) recordWrites() func(pi uint16) bool {
	var mu sync.Mutex
	seen := map[uint16]bool{}
	setInboundHook(f.t, &mqttTestInboundEmitOutcomeV, func(j *mqttInboundJob, _ bool) {
		if f.mine(j) {
			mu.Lock()
			seen[j.pi] = true
			mu.Unlock()
		}
	})
	return func(pi uint16) bool {
		mu.Lock()
		defer mu.Unlock()
		return seen[pi]
	}
}

// TestMQTTInboundPipeResponsesFollowDurability: each kind of response, pipelined and at the
// head, is not chosen for emission while its durable effect has not returned. Mutations M3
// (PUBACK ready at submission), M4 (PUBREC before the hold CAS returns) and M5 (PUBCOMP before
// the terminal CAS returns) make the response ready early. Each mutant marks it ready, and so
// writes it, synchronously BEFORE the held seam is reached, so "not written by the time the
// seam is reached" is a completed checkpoint, not a wait on an absence; the emission poke then
// shows the head is not eligible.
func TestMQTTInboundPipeResponsesFollowDurability(t *testing.T) {
	t.Run("PUBACK after the store", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-dur-1", nil)
		hold := f.holdStoreResult(nil, 1)
		emits := f.recordDecisions(&mqttTestInboundEmitDecisionV)
		var pipe atomic.Pointer[mqttInboundPipe]
		setInboundHook(t, &mqttTestInboundSwitchV, func(c *client, _ mqttInboundKind, _ uint16, _ bool) {
			if c.mqtt != nil && c.mqtt.cid == f.cid && c.mqtt.inPipe != nil {
				pipe.Store(c.mqtt.inPipe)
			}
		})
		written := f.recordWrites()
		f.send(f.pub(1, 1), f.pub(1, 2))
		hold.wait(t, "the store's answer")
		if written(1) {
			t.Fatal("PUBACK 1 was written while its store's answer was held")
		}
		d := pokeEmitter(t, pipe.Load(), emits)
		if d.Chosen != 0 {
			t.Fatalf("PUBACK %d was chosen for emission while its store's answer was held", d.Chosen)
		}
		hold.release()
		f.expect(mqttPacketPubAck, 1)
		f.expect(mqttPacketPubAck, 2)
	})
	t.Run("PUBREC after the hold CAS", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-dur-2", nil)
		g := newInboundGate()
		t.Cleanup(g.release)
		setInboundHook(t, &mqttTestStoreMsgResultHookV, func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			if strings.HasPrefix(subject, mqttQoS2IncomingMsgsStreamSubjectPrefix) && strings.HasSuffix(subject, ".1") {
				g.park(nil)
			}
			return resp, err
		})
		emits := f.recordDecisions(&mqttTestInboundEmitDecisionV)
		holdJob := f.pauseAt(mqttInboundQoS2Pub, 1, "hold")
		written := f.recordWrites()
		f.send(f.pub(2, 1), f.pub(1, 2))
		j := holdJob.wait(t, "the hold step")
		holdJob.release()
		g.wait(t, "the hold CAS's answer")
		if written(1) {
			t.Fatal("PUBREC 1 was written while its hold CAS's answer was held")
		}
		d := pokeEmitter(t, j.p, emits)
		if d.Chosen != 0 {
			t.Fatalf("PUBREC (job %d) was chosen for emission while its hold CAS's answer was held", d.Chosen)
		}
		g.release()
		f.expect(mqttPacketPubRec, 1)
		f.expect(mqttPacketPubAck, 2)
	})
	t.Run("PUBCOMP after the terminal CAS", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-dur-3", nil)
		f.send(f.pub(2, 1))
		f.expect(mqttPacketPubRec, 1)
		g := newInboundGate()
		t.Cleanup(g.release)
		asm := f.asm
		asm.mu.Lock()
		asm.testQoS2FinalizeStore = func(subject, gen string, expectSeq uint64) (uint64, error) {
			g.park(nil)
			return asm.jsa.storeQoS2DoneCAS(subject, gen, expectSeq)
		}
		asm.mu.Unlock()
		t.Cleanup(func() {
			asm.mu.Lock()
			asm.testQoS2FinalizeStore = nil
			asm.mu.Unlock()
		})
		emits := f.recordDecisions(&mqttTestInboundEmitDecisionV)
		fences := f.pauseAt(mqttInboundPubRel, 1, "fences")
		written := f.recordWrites()
		f.send(f.rel(1), f.pub(1, 2))
		j := fences.wait(t, "the release's fences")
		fences.release()
		g.wait(t, "the terminal CAS")
		if written(1) {
			t.Fatal("PUBCOMP 1 was written while its terminal CAS was held")
		}
		d := pokeEmitter(t, j.p, emits)
		if d.Chosen != 0 {
			t.Fatalf("PUBCOMP (job %d) was chosen for emission while its terminal CAS was held", d.Chosen)
		}
		g.release()
		f.expect(mqttPacketPubComp, 1)
		f.expect(mqttPacketPubAck, 2)
	})
}

// TestMQTTInboundPipeReleasesTakeTurnsClassifying: two releases of one connection, the first
// paused inside its retirement classification holding the session record transaction. The
// second waits its turn instead of being refused the transaction (an Unresolved verdict closes
// the connection). Mutation M6 (no per-pipe serialization) records the refusal. The client's
// own acknowledgement arriving meanwhile drains behind the pipe (M8b).
func TestMQTTInboundPipeReleasesTakeTurnsClassifying(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-classify", nil)
	f.send(f.pub(2, 1), f.pub(2, 2))
	f.expect(mqttPacketPubRec, 1)
	f.expect(mqttPacketPubRec, 2)

	inTx := newInboundGate()
	t.Cleanup(inTx.release)
	var first atomic.Bool
	setInboundHook(t, &mqttTestQoS2RetireInTxV, func(*mqttSession) {
		if first.CompareAndSwap(false, true) {
			inTx.park(nil)
		}
	})
	type cls struct {
		j     *mqttInboundJob
		phase string
		v     mqttQoS2RetireVerdict
	}
	clsCh := make(chan cls, 16)
	setInboundHook(t, &mqttTestInboundClassifyV, func(j *mqttInboundJob, phase string, v mqttQoS2RetireVerdict) {
		if f.mine(j) {
			clsCh <- cls{j, phase, v}
		}
	})
	dispatch := make(chan [2]bool, 16)
	setInboundHook(t, &mqttTestInboundDispatchV, func(c *client, pt byte, drain, pending bool) {
		if c.mqtt != nil && c.mqtt.cid == f.cid && pt == mqttPacketPubAck {
			dispatch <- [2]bool{drain, pending}
		}
	})
	f.send(f.rel(1, 2))
	inTx.wait(t, "the first classification holding the record transaction")
	// The two releases' fences run concurrently, so either may classify first; the other's
	// first record must be its wait for the turn, not a verdict the held transaction forced.
	select {
	case r := <-clsCh:
		if r.phase != "waiting" {
			t.Fatalf("release %d recorded %q (verdict %v) while the other release held the record transaction; want it waiting its turn",
				r.j.pi, r.phase, r.v)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the second release never reached its classification")
	}
	// The client acknowledges something (an identifier the server never used, which the
	// handler ignores) while the releases are outstanding: it must drain behind them.
	f.send([]byte{mqttPacketPubAck, 2, 0, 9})
	select {
	case d := <-dispatch:
		if !d[0] || !d[1] {
			t.Fatalf("the client's PUBACK was dispatched with drain=%v over a pending pipe=%v; it must drain", d[0], d[1])
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the client's PUBACK was never dispatched")
	}
	inTx.release()
	f.expect(mqttPacketPubComp, 1)
	f.expect(mqttPacketPubComp, 2)
}

// TestMQTTInboundPipeRejectionGateDrainsFirst: a PUBLISH arriving while the pipe holds work
// and the session's record-rejection marker is armed makes the gate drain the pipe before it
// settles the marker under the record transaction (which a queued release's classifier would
// otherwise be refused). The dispatch seam records the drain-first decision; a gate that settled
// anyway would be seen settling with the pipe still pending (implementation-time test, M19).
func TestMQTTInboundPipeRejectionGateDrainsFirst(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-gate", nil)
	hold := f.holdStoreResult(nil, 1)
	f.send(f.pub(1, 1), f.pub(1, 2))
	hold.wait(t, "store 1")
	sc := clientByID(t, f.s, f.cid)
	sess := sc.mqtt.sess
	events := make(chan string, 8)
	setInboundHook(t, &mqttTestInboundDispatchV, func(c *client, pt byte, drain, pending bool) {
		if c == sc && pt == mqttPacketPub && drain && pending {
			events <- "drain-first"
		}
	})
	setInboundHook(t, &mqttTestRejectionWindowHookV, func(s *mqttSession, window string) {
		if s == sess && window == mqttRejectionWindowSettle {
			if p := sc.mqtt.inPipe; p != nil && p.pending() {
				events <- "settled over a pending pipe"
			} else {
				events <- "settled"
			}
		}
	})
	sess.mu.Lock()
	sess.markWriteRejectedLocked(sess.seq)
	sess.mu.Unlock()
	f.send(f.pub(1, 3))
	select {
	case e := <-events:
		if e != "drain-first" {
			t.Fatalf("the gate %s; it must drain the pipe first", e)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the gate never decided")
	}
	hold.release()
	select {
	case e := <-events:
		if e != "settled" {
			t.Fatalf("after the drain the gate %s", e)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the gate never settled after the drain")
	}
	f.expect(mqttPacketPubAck, 1)
	f.expect(mqttPacketPubAck, 2)
	f.expect(mqttPacketPubAck, 3)
}

// TestMQTTInboundPipeErrorExitCompletesEarlierPackets: a packet that ends the connection does
// not take the packets before it down with it. A QoS 1 publication is queued with its store's
// answer held, and the same read carries a QoS 0 publication whose Payload Format Indicator
// lies, which closes the connection with 0x99. The earlier publication is completed and
// answered before the DISCONNECT, as the serial path answered it before parsing the next
// packet at all (implementation-time test, M20 aborts it instead).
func TestMQTTInboundPipeErrorExitCompletesEarlierPackets(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-error-exit", nil)
	hold := f.holdStoreResult(nil, 1)
	// A QoS 0 PUBLISH with Payload Format Indicator 1 and a payload that is not UTF-8.
	topic := f.topic(2)
	props := []byte{mqttPropPayloadFormat, 1}
	payload := []byte{0xff, 0xfe}
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub)
	w.WriteVarInt(2 + len(topic) + 1 + len(props) + len(payload))
	w.WriteString(topic)
	w.WriteByte(byte(len(props)))
	w.Write(props)
	w.Write(payload)
	errored := make(chan error, 2)
	sc := clientByID(t, f.s, f.cid)
	setInboundHook(t, &mqttTestReadLoopErrorV, func(c *client, err error) {
		if c == sc {
			errored <- err
		}
	})
	f.send(f.pub(1, 1), w.Bytes())
	// The error exit is reported before the earlier packet is completed, so it is the
	// checkpoint; the store's answer stays held (if it was submitted) until after it.
	select {
	case err := <-errored:
		if !errors.Is(err, errMQTTPayloadFormatInvalid) {
			t.Fatalf("the connection ended on %v, want the Payload Format violation", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the violation never ended the connection")
	}
	hold.release()
	f.expect(mqttPacketPubAck, 1)
	if got := f.expectDisconnect(); got != mqttReasonPayloadFormatInvalid {
		t.Fatalf("DISCONNECT reason %#x, want payload format invalid", got)
	}
	f.expectEOF(15 * time.Second)
}

// TestMQTTInboundPipeAbortKeepsAClaimedRelease: a connection that aborts while the issuer is
// inside a release's ordered step (its onward store already submitted) does not drop the
// release: the release keeps its lease and release op and completes, awaiting the store and
// committing the terminal state (implementation-time test, holistic round H1; M22 drops it).
func TestMQTTInboundPipeAbortKeepsAClaimedRelease(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-abort-claimed", nil)
	f.send(f.pub(2, 1))
	f.expect(mqttPacketPubRec, 1)
	issued := newInboundGate()
	t.Cleanup(issued.release)
	events := make(chan string, 8)
	setInboundHook(t, &mqttTestInboundPauseV, func(j *mqttInboundJob, step string) {
		if !f.mine(j) || j.kind != mqttInboundPubRel {
			return
		}
		switch step {
		case "issued":
			issued.park(j)
		case "kept":
			select {
			case events <- "kept":
			default:
			}
		}
	})
	setInboundHook(t, &mqttTestInboundResponseReadyV, func(j *mqttInboundJob) {
		if f.mine(j) && j.kind == mqttInboundPubRel {
			if errors.Is(j.err, errMQTTInboundAborted) {
				events <- "dropped"
			} else {
				events <- "completed"
			}
		}
	})
	submitted := f.recordSubmissions()
	f.send(f.rel(1), f.pub(1, 2))
	issued.wait(t, "the release's issuance")
	if got := submitted(); len(got) == 0 || got[0] != f.subject(1) {
		t.Fatalf("the release's onward store was not submitted before the pause: %v", got)
	}
	clientByID(t, f.s, f.cid).closeTransport()
	select {
	case e := <-events:
		if e != "kept" {
			t.Fatalf("the release was %s while the issuer still owned its submitted store", e)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the release never decided what the abort meant")
	}
	issued.release()
	for {
		select {
		case e := <-events:
			if e == "kept" {
				continue
			}
			if e != "completed" {
				t.Fatalf("after its issuance the release was %s", e)
			}
			rec, _, err := f.asm.loadQoS2Record(clientByID(t, f.s, f.cid).mqttQoS2InternalSubject(1))
			if err != nil || (rec != nil && rec.state == mqttQoS2Held) {
				t.Fatalf("the release did not commit its terminal state (%v)", err)
			}
			return
		case <-time.After(15 * time.Second):
			t.Fatal("the claimed release never completed")
		}
	}
}

// TestMQTTInboundPipeOversizedPacketCompletesTheAcceptedPrefix: a packet whose header claims
// more than the Maximum Packet Size ends the connection (DISCONNECT 0x95), but the publication
// accepted before it in the same read is completed and answered first (implementation-time
// test, holistic round H1; M23 closes before draining). The 3.1.1 refusal of a publication too
// large once encoded closes the same way (M24).
func TestMQTTInboundPipeOversizedPacketCompletesTheAcceptedPrefix(t *testing.T) {
	small := func(o *Options) { o.MaxPayload = 2048 }
	t.Run("an oversized header (v5)", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-oversized-v5", small)
		hold := f.holdStoreResult(nil, 1)
		errored := make(chan error, 4)
		sc := clientByID(t, f.s, f.cid)
		setInboundHook(t, &mqttTestReadLoopErrorV, func(c *client, err error) {
			if c == sc {
				errored <- err
			}
		})
		w := newMQTTWriter(0)
		w.WriteByte(mqttPacketPub | 1<<1)
		w.WriteVarInt(4000)
		f.send(f.pub(1, 1), w.Bytes())
		select {
		case err := <-errored:
			if !errors.Is(err, ErrMaxPayload) {
				t.Fatalf("the connection ended on %v, want the packet-size violation", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the oversized header never ended the connection")
		}
		hold.release()
		f.expect(mqttPacketPubAck, 1)
		if got := f.expectDisconnect(); got != mqttReasonPacketTooLarge {
			t.Fatalf("DISCONNECT reason %#x, want packet too large", got)
		}
		f.expectEOF(15 * time.Second)
	})
	t.Run("a publication too large once encoded (3.1.1)", func(t *testing.T) {
		o := testMQTTDefaultOptions()
		small(o)
		s := testMQTTRunServer(t, o)
		t.Cleanup(func() { testMQTTShutdownServer(s) })
		f := &inboundPipeFix{t: t, s: s, o: o, asm: asmOf(t, s), cid: "pipe-oversized-311", prefix: mqttStreamSubjectPrefix + "pipe."}
		nc, js := jsClientConnect(t, s)
		t.Cleanup(nc.Close)
		if _, err := js.AddConsumer(mqttStreamName, &nats.ConsumerConfig{
			Durable: "inbound-pipe", FilterSubject: f.prefix + ">", AckPolicy: nats.AckExplicitPolicy,
		}); err != nil {
			t.Fatalf("interest consumer: %v", err)
		}
		// The helper has already read the CONNACK into its own reader.
		c, mr := testMQTTConnect(t, &mqttConnInfo{clientID: f.cid, cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
		t.Cleanup(func() { c.Close() })
		testMQTTCheckConnAck(t, mr, mqttConnAckRCConnectionAccepted, false)
		f.nc, f.br = c, bufio.NewReader(c)
		hold := f.holdStoreResult(nil, 1)
		errored := make(chan error, 4)
		sc := clientByID(t, s, f.cid)
		setInboundHook(t, &mqttTestReadLoopErrorV, func(cc *client, err error) {
			if cc == sc {
				errored <- err
			}
		})
		// Within the packet bound (max payload plus the connected overhead), but too large for
		// max payload once its NATS headers are added.
		big := inboundAckBenchPublish(1, f.topic(2), 2, make([]byte, 2040))
		f.send(f.pub(1, 1), big)
		select {
		case err := <-errored:
			if !errors.Is(err, ErrMaxPayload) {
				t.Fatalf("the connection ended on %v, want the max-payload refusal", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the oversized publication never ended the connection")
		}
		hold.release()
		f.expect(mqttPacketPubAck, 1)
		f.expectEOF(15 * time.Second)
	})
}

// TestMQTTInboundPipeSwitch: a client that publishes and waits for each answer is always on
// the direct path (M7b disables it); a publication arriving alone while the pipe holds work is
// pipelined and answered after it (M7 takes the direct path anyway, overtaking).
func TestMQTTInboundPipeSwitch(t *testing.T) {
	t.Run("one at a time is direct", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-switch-direct", nil)
		var direct, piped atomic.Int64
		setInboundHook(t, &mqttTestInboundSwitchV, func(c *client, _ mqttInboundKind, _ uint16, d bool) {
			if c.mqtt != nil && c.mqtt.cid == f.cid {
				if d {
					direct.Add(1)
				} else {
					piped.Add(1)
				}
			}
		})
		for pi := uint16(1); pi <= 200; pi++ {
			f.send(f.pub(1, pi))
			f.expect(mqttPacketPubAck, pi)
		}
		if direct.Load() != 200 || piped.Load() != 0 {
			t.Fatalf("%d exchanges direct, %d pipelined; a client that waits for each answer must be direct every time",
				direct.Load(), piped.Load())
		}
	})
	t.Run("a lone packet behind queued work is pipelined", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-switch-lone", nil)
		hold := f.holdStoreResult(nil, 1)
		choices := make(chan bool, 64)
		setInboundHook(t, &mqttTestInboundSwitchV, func(c *client, _ mqttInboundKind, _ uint16, d bool) {
			if c.mqtt != nil && c.mqtt.cid == f.cid {
				choices <- d
			}
		})
		f.send(f.pub(1, 1), f.pub(1, 2), f.pub(1, 3), f.pub(1, 4), f.pub(1, 5))
		hold.wait(t, "the first store's answer")
		for i := 0; i < 5; i++ {
			select {
			case <-choices:
			case <-time.After(15 * time.Second):
				t.Fatal("the burst was never dispatched")
			}
		}
		f.send(f.pub(1, 6)) // its own write, so its own read
		select {
		case d := <-choices:
			if d {
				t.Fatal("a publication arriving alone while the pipe held work took the direct path")
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the lone publication was never dispatched")
		}
		hold.release()
		for pi := uint16(1); pi <= 6; pi++ {
			f.expect(mqttPacketPubAck, pi)
		}
	})
}

// TestMQTTInboundPipePingReqDrains: a PINGREQ behind queued work is dispatched only after
// the pipe drains, so its PINGRESP follows every owed acknowledgement (M8).
func TestMQTTInboundPipePingReqDrains(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-ping", nil)
	hold := f.holdStoreResult(nil, 1)
	dispatch := make(chan [2]bool, 4)
	setInboundHook(t, &mqttTestInboundDispatchV, func(c *client, pt byte, drain, pending bool) {
		if c.mqtt != nil && c.mqtt.cid == f.cid && pt == mqttPacketPing {
			dispatch <- [2]bool{drain, pending}
		}
	})
	f.send(f.pub(1, 1), f.pub(1, 2), f.pub(1, 3))
	hold.wait(t, "the first store's answer")
	f.send([]byte{mqttPacketPing, 0})
	select {
	case d := <-dispatch:
		if !d[0] || !d[1] {
			t.Fatalf("the PINGREQ was dispatched with drain=%v over a pending pipe=%v", d[0], d[1])
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the PINGREQ was never dispatched")
	}
	hold.release()
	for pi := uint16(1); pi <= 3; pi++ {
		f.expect(mqttPacketPubAck, pi)
	}
	f.expectPingResp()
}

// TestMQTTInboundPipeNoResponseAfterServerDisconnect: a completion that tries to answer after
// the server's DISCONNECT has been written, while the transport is still open (shutdown paused
// between the two), is refused by the write itself (M9 writes it). A takeover cannot produce
// this order: its retirement drains the job's lease before it notifies.
func TestMQTTInboundPipeNoResponseAfterServerDisconnect(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-after-disconnect", nil)
	hold := f.holdStoreResult(nil, 1)
	written := newInboundGate()
	t.Cleanup(written.release)
	setInboundHook(t, &mqttTestShutdownDisconnectV, func(c *client, phase string) {
		if c.mqtt != nil && c.mqtt.cid == f.cid && phase == "written" {
			written.park(nil)
		}
	})
	outcome := make(chan bool, 4)
	setInboundHook(t, &mqttTestInboundEmitOutcomeV, func(j *mqttInboundJob, w bool) {
		if f.mine(j) && j.pi == 1 {
			outcome <- w
		}
	})
	f.send(f.pub(1, 1), f.pub(1, 2))
	hold.wait(t, "the store's answer")
	done := f.s.BeginShutdown()
	written.wait(t, "the shutdown DISCONNECT")
	hold.release()
	select {
	case w := <-outcome:
		if w {
			t.Fatal("PUBACK 1 was written after the server's DISCONNECT")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the completion never attempted its PUBACK")
	}
	written.release()
	if got := f.expectDisconnect(); got != mqttReasonServerShuttingDown {
		t.Fatalf("DISCONNECT reason %#x, want server shutting down", got)
	}
	f.expectEOF(15 * time.Second)
	<-done
}

// TestMQTTInboundPipeRegistrationsAndJoin covers the reply registrations (M11) and the
// readLoop's join of the pipe before teardown (M10).
func TestMQTTInboundPipeRegistrationsAndJoin(t *testing.T) {
	countReplies := func(asm *mqttAccountSessionManager) int {
		n := 0
		asm.jsa.replies.Range(func(any, any) bool { n++; return true })
		return n
	}
	t.Run("a timed-out store releases its registration", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-reg-timeout", func(o *Options) { o.MQTT.JSAPITimeout = time.Second })
		base := countReplies(f.asm)
		drop := func(subject string, _ []byte) bool { return strings.HasSuffix(subject, f.subject(1)) }
		f.asm.jsa.testDropRequest.Store(&drop)
		t.Cleanup(func() { f.asm.jsa.testDropRequest.Store(nil) })
		f.send(f.pub(1, 1), f.pub(1, 2))
		// The dropped store times out: the connection fails at that position.
		if got := f.expectDisconnect(); got == mqttReasonSuccess {
			t.Fatal("a timed-out store answered success")
		}
		f.expectEOF(15 * time.Second)
		deadline := time.Now().Add(10 * time.Second)
		for countReplies(f.asm) > base {
			if time.Now().After(deadline) {
				t.Fatalf("%d reply registrations outlive the timed-out store (baseline %d)", countReplies(f.asm), base)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
	t.Run("a store abandoned at shutdown releases its registration", func(t *testing.T) {
		o := testMQTTDefaultOptionsV5()
		// Long enough that the quit arm, not the timeout, ends the wait however long the
		// shutdown takes under -race.
		o.MQTT.JSAPITimeout = 25 * time.Second // below the delayed-Will claim lease (30 s)
		s := testMQTTRunServer(t, o)
		asm := asmOf(t, s)
		drop := func(string, []byte) bool { return true }
		asm.jsa.testDropRequest.Store(&drop)
		t.Cleanup(func() { asm.jsa.testDropRequest.Store(nil) })
		p := asm.jsa.submitStoreMsg(mqttStreamSubjectPrefix+"pipe.quit", 0, []byte("x"))
		asm.jsa.testDropRequest.Store(nil)
		got := inboundAwaitAsync(&p)
		testMQTTShutdownServer(s)
		if err := <-got; !errors.Is(err, ErrServerNotRunning) {
			t.Fatalf("await returned %v at shutdown, want ErrServerNotRunning", err)
		}
		if _, ok := asm.jsa.replies.Load(p.reply); ok {
			t.Fatal("the abandoned store's reply registration outlived the shutdown")
		}
	})
	t.Run("the readLoop joins the pipe before teardown", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-join", nil)
		hold := f.holdStoreResult(nil, 1)
		var c *client
		entry := make(chan struct{}, 1)
		setInboundHook(t, &mqttTestClosedClientEntryV, func(cc *client) {
			if cc.mqtt != nil && cc.mqtt.cid == f.cid {
				entry <- struct{}{}
			}
		})
		joined := make(chan struct{}, 1)
		setInboundHook(t, &mqttTestInboundJoinedV, func(cc *client) {
			if cc.mqtt != nil && cc.mqtt.cid == f.cid {
				joined <- struct{}{}
			}
		})
		f.send(f.pub(1, 1), f.pub(1, 2))
		hold.wait(t, "the store's answer")
		c = clientByID(t, f.s, f.cid)
		latch := c.mqtt.abortLatch()
		f.nc.Close()
		select {
		case <-latch:
		case <-entry:
			t.Fatal("the teardown began while an inbound job was still in flight")
		case <-time.After(15 * time.Second):
			t.Fatal("the readLoop never noticed the close")
		}
		hold.release()
		select {
		case <-joined:
		case <-entry:
			t.Fatal("the teardown began before the pipe was joined")
		case <-time.After(15 * time.Second):
			t.Fatal("the pipe was never joined")
		}
		select {
		case <-entry:
		case <-time.After(15 * time.Second):
			t.Fatal("the teardown never ran")
		}
	})
}

// TestMQTTInboundPipeShutdownAbortsEveryWait: with the account's subs barrier held by a
// synthetic entry, a direct publication waiting in it and a pipelined connection whose window
// is full are both released by shutdown's socket closes, so Phase 1 completes while the entry
// is still held (M18: the close does not fire the abort latch). The service-account
// invalidation's close fires it too.
func TestMQTTInboundPipeShutdownAbortsEveryWait(t *testing.T) {
	prevWindow := mqttInboundWindow
	mqttInboundWindow = 2
	t.Cleanup(func() { mqttInboundWindow = prevWindow })

	t.Run("shutdown", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-shut-a", nil)
		ncB, _ := f.dial("pipe-shut-b", nil)
		entry := f.asm.jsa.subsBarrier.enter()
		var leaveOnce sync.Once
		leave := func() { leaveOnce.Do(func() { f.asm.jsa.subsBarrier.leave(entry) }) }
		t.Cleanup(leave)
		waiting := make(chan struct{}, 8)
		setInboundHook(t, &mqttTestSubsBarrierSampledHookV, func(b *mqttSubsBarrier, n int) {
			if b == &f.asm.jsa.subsBarrier && n > 0 {
				waiting <- struct{}{}
			}
		})
		f.send(f.pub(1, 1)) // direct: the readLoop waits in the barrier
		<-waiting
		var b []byte
		for pi := uint16(1); pi <= 4; pi++ {
			b = append(b, f.pub(1, pi)...)
		}
		if _, err := ncB.Write(b); err != nil {
			t.Fatal(err)
		}
		<-waiting // B's issuer waits in the barrier; B's readLoop fills the window behind it
		cA, cB := clientByID(t, f.s, f.cid), clientByID(t, f.s, "pipe-shut-b")
		closed := make(chan *client, 4)
		setInboundHook(t, &mqttTestShutdownDisconnectV, func(c *client, phase string) {
			if phase == "closed" && (c == cA || c == cB) {
				closed <- c
			}
		})
		phase1 := make(chan struct{})
		setInboundHook(t, &mqttTestShutdownClientsJoinedV, func() { close(phase1) })
		done := f.s.BeginShutdown()
		for i := 0; i < 2; i++ {
			select {
			case c := <-closed:
				if !mqttLatchFired(c.mqtt.abortLatch()) {
					t.Fatalf("client %q's transport was closed without firing its abort latch", c.mqtt.cid)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("shutdown never closed both transports")
			}
		}
		select {
		case <-phase1:
		case <-time.After(15 * time.Second):
			t.Fatal("shutdown Phase 1 did not complete while the barrier entry was held")
		}
		leave()
		<-done
	})
	t.Run("service-account invalidation", func(t *testing.T) {
		_, s := authzBroker(t, authzConfOneAccount)
		asm := asmOf(t, s)
		c, mr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pipe-inval", cleanStart: true, user: "alice", pass: "pw"},
			"127.0.0.1", s.Addr().(*net.TCPAddr).Port)
		t.Cleanup(func() { c.Close() })
		if _, reason, _ := testMQTTReadConnAckV5(t, mr); reason != mqttConnAckRCConnectionAccepted {
			t.Fatalf("CONNACK reason %#x", reason)
		}
		entry := asm.jsa.subsBarrier.enter()
		var leaveOnce sync.Once
		t.Cleanup(func() { leaveOnce.Do(func() { asm.jsa.subsBarrier.leave(entry) }) })
		waiting := make(chan struct{}, 8)
		setInboundHook(t, &mqttTestSubsBarrierSampledHookV, func(b *mqttSubsBarrier, n int) {
			if b == &asm.jsa.subsBarrier && n > 0 {
				waiting <- struct{}{}
			}
		})
		if _, err := c.Write(inboundAckBenchPublish(1, "pipe/1", 1, []byte("x"))); err != nil {
			t.Fatal(err)
		}
		<-waiting
		sc := clientByID(t, s, "pipe-inval")
		s.invalidateAuthPassthroughClients()
		if !mqttLatchFired(sc.mqtt.abortLatch()) {
			t.Fatal("the invalidation closed the transport without firing the abort latch")
		}
	})
}

// TestMQTTInboundPipeReceiveMaximum: the server's inbound Receive Maximum counts the distinct
// identifiers it has not answered or that are held between PUBREC and PUBCOMP.
func TestMQTTInboundPipeReceiveMaximum(t *testing.T) {
	limit2 := func(o *Options) { o.MQTT.ReceiveMaximum = 2 }
	t.Run("answered identifiers leave the count", func(t *testing.T) { // M12
		f := newInboundPipeFix(t, "pipe-rm-a", limit2)
		f.send(f.pub(1, 1), f.pub(1, 2))
		f.expect(mqttPacketPubAck, 1)
		f.expect(mqttPacketPubAck, 2)
		hold := f.holdStoreResult(nil, 3, 4)
		outcome := f.refusedOrAdmitted(4)
		f.send(f.pub(1, 3), f.pub(1, 4))
		if outcome() {
			t.Fatal("a fresh identifier was refused after the earlier ones were answered")
		}
		f.send([]byte{mqttPacketPing, 0})
		hold.release()
		f.expect(mqttPacketPubAck, 3)
		f.expect(mqttPacketPubAck, 4)
		f.expectPingResp()
	})
	t.Run("unanswered QoS 1 identifiers count", func(t *testing.T) { // M12c
		f := newInboundPipeFix(t, "pipe-rm-b", limit2)
		hold := f.holdStoreResult(nil, 1, 2)
		outcome := f.refusedOrAdmitted(3)
		// Stores 1 and 2 are held, so nothing is answered before the third identifier is checked.
		f.send(f.pub(1, 1), f.pub(1, 2), f.pub(1, 3))
		if !outcome() {
			t.Fatal("a third unanswered identifier at Receive Maximum 2 was admitted")
		}
		hold.release()
		// The two accepted publications are completed and answered before the DISCONNECT.
		f.expect(mqttPacketPubAck, 1)
		f.expect(mqttPacketPubAck, 2)
		if got := f.expectDisconnect(); got != mqttReasonReceiveMaxExceeded {
			t.Fatalf("DISCONNECT reason %#x, want receive maximum exceeded", got)
		}
	})
	t.Run("a DUP of an outstanding identifier does not count again", func(t *testing.T) { // M12b
		f := newInboundPipeFix(t, "pipe-rm-c", limit2)
		f.send(f.pub(2, 1))
		f.expect(mqttPacketPubRec, 1) // held: outstanding {1}
		hold := f.holdStoreResult(nil, 2)
		outcome := f.refusedOrAdmitted(1)
		f.send(f.pub(1, 2), f.dup(f.pub(2, 1)), []byte{mqttPacketPing, 0}) // outstanding {1, 2}, then the DUP
		if outcome() {
			t.Fatal("a DUP of an identifier already outstanding was refused as a new send")
		}
		hold.release()
		f.expect(mqttPacketPubAck, 2)
		f.expect(mqttPacketPubRec, 1)
		f.expectPingResp()
	})
	t.Run("an identifier stays outstanding while any of its publications is", func(t *testing.T) { // M12d
		f := newInboundPipeFix(t, "pipe-rm-d", limit2)
		// PI 5 twice, on two topics so each store has its own subject: the first publication's
		// store is answered once all three are queued, the second's and PI 6's are held
		// throughout.
		first := newInboundGate()
		t.Cleanup(first.release)
		rest := newInboundGate()
		t.Cleanup(rest.release)
		setInboundHook(t, &mqttTestStoreMsgResultHookV, func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
			switch subject {
			case f.prefix + "5a":
				first.park(nil)
			case f.prefix + "5b", f.subject(6):
				rest.park(nil)
			}
			return resp, err
		})
		queued := make(chan struct{}, 8)
		setInboundHook(t, &mqttTestInboundSwitchV, func(c *client, _ mqttInboundKind, _ uint16, _ bool) {
			if c.mqtt != nil && c.mqtt.cid == f.cid {
				queued <- struct{}{}
			}
		})
		f.send(inboundAckBenchPublish(1, "pipe/5a", 5, []byte("a")), f.pub(1, 6), inboundAckBenchPublish(1, "pipe/5b", 5, []byte("b")))
		for i := 0; i < 3; i++ {
			<-queued
		}
		first.wait(t, "the first store of PI 5")
		first.release()
		f.expect(mqttPacketPubAck, 5) // outstanding: 5 (second) and 6
		outcome := f.refusedOrAdmitted(7)
		f.send(f.pub(1, 7))
		if !outcome() {
			t.Fatal("PI 7 was admitted while 5 and 6 were outstanding at Receive Maximum 2")
		}
		rest.release()
		f.expect(mqttPacketPubAck, 6)
		f.expect(mqttPacketPubAck, 5)
		if got := f.expectDisconnect(); got != mqttReasonReceiveMaxExceeded {
			t.Fatalf("DISCONNECT reason %#x, want receive maximum exceeded", got)
		}
	})
}

// TestMQTTInboundPipeExpiryAgesInTheQueue: Message Expiry counts the time a publication waits
// before its issuance (QoS 0/1, M13) and before its new hold (QoS 2, M13b). The clock advances
// while the target is queued, before its step runs.
func TestMQTTInboundPipeExpiryAgesInTheQueue(t *testing.T) {
	var offset atomic.Int64
	now := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	setInboundHook(t, &mqttTestExpiryNowV, now)

	storedTTL := func(f *inboundPipeFix) func(pi uint16) (string, bool) {
		var mu sync.Mutex
		seen := map[string]string{}
		setInboundHook(t, &mqttTestStoreMsgHookV, func(subject string, hdr int, msg []byte) error {
			if strings.HasPrefix(subject, f.prefix) {
				mu.Lock()
				seen[subject] = string(getHeader(JSMessageTTL, msg[:hdr]))
				mu.Unlock()
			}
			return nil
		})
		return func(pi uint16) (string, bool) {
			mu.Lock()
			defer mu.Unlock()
			v, ok := seen[f.subject(pi)]
			return v, ok
		}
	}
	// received waits until f's readLoop has dispatched a job of kind: its receipt time is
	// taken by then, so a clock advanced afterwards is time it spent queued.
	received := func(f *inboundPipeFix, kind mqttInboundKind) func() {
		ch := make(chan struct{}, 8)
		setInboundHook(t, &mqttTestInboundSwitchV, func(c *client, k mqttInboundKind, _ uint16, _ bool) {
			if c.mqtt != nil && c.mqtt.cid == f.cid && k == kind {
				ch <- struct{}{}
			}
		})
		return func() {
			select {
			case <-ch:
			case <-time.After(15 * time.Second):
				t.Fatal("the target was never dispatched")
			}
		}
	}
	queueBehindRelease := func(f *inboundPipeFix, target []byte, advance time.Duration) {
		f.send(f.pub(2, 9))
		f.expect(mqttPacketPubRec, 9)
		fences := f.pauseAt(mqttInboundPubRel, 9, "fences")
		got := received(f, mqttInboundQoS1)
		f.send(f.rel(9), target)
		fences.wait(t, "the release ahead of the target")
		got()
		offset.Add(int64(advance))
		fences.release()
		f.expect(mqttPacketPubComp, 9)
	}
	t.Run("QoS 1 expired in the queue", func(t *testing.T) {
		offset.Store(0)
		f := newInboundPipeFix(t, "pipe-exp-a", nil)
		ttl := storedTTL(f)
		queueBehindRelease(f, f.pubExpiring(1, 1, 2), 3*time.Second)
		f.expect(mqttPacketPubAck, 1)
		if _, stored := ttl(1); stored {
			t.Fatal("a publication that expired in the queue was stored")
		}
	})
	t.Run("QoS 1 aged in the queue", func(t *testing.T) {
		offset.Store(0)
		f := newInboundPipeFix(t, "pipe-exp-b", nil)
		ttl := storedTTL(f)
		queueBehindRelease(f, f.pubExpiring(1, 1, 3), time.Second+100*time.Millisecond)
		f.expect(mqttPacketPubAck, 1)
		if v, stored := ttl(1); !stored || v != "2s" {
			t.Fatalf("stored with TTL %q (stored %v); a 3 s interval that waited 1 s carries 2 s", v, stored)
		}
	})
	holdInterval := func(f *inboundPipeFix, pi uint16) (uint32, bool) {
		rec, _, err := f.asm.loadQoS2Record(clientByID(t, f.s, f.cid).mqttQoS2InternalSubject(pi))
		if err != nil || rec == nil || rec.state != mqttQoS2Held {
			t.Fatalf("no held record for %d: %v", pi, err)
		}
		h := mqttParsePublishNATSHeader(rec.stored.Header)
		return mqttMessageExpiry(h.props)
	}
	newHoldBehindChain := func(f *inboundPipeFix, advance time.Duration, secs uint32) {
		f.send(f.pub(2, 5))
		f.expect(mqttPacketPubRec, 5)
		fences := f.pauseAt(mqttInboundPubRel, 5, "fences")
		got := received(f, mqttInboundQoS2Pub)
		f.send(f.rel(5), f.pubExpiring(2, 5, secs))
		fences.wait(t, "the release ahead on the identifier")
		got()
		offset.Add(int64(advance))
		fences.release()
		f.expect(mqttPacketPubComp, 5)
		f.expect(mqttPacketPubRec, 5)
	}
	t.Run("QoS 2 new hold aged by its wait", func(t *testing.T) {
		offset.Store(0)
		f := newInboundPipeFix(t, "pipe-exp-c", nil)
		newHoldBehindChain(f, time.Second+100*time.Millisecond, 3)
		if v, ok := holdInterval(f, 5); !ok || v != 2 {
			t.Fatalf("the new hold carries interval %d (%v); a 3 s interval that waited 1 s carries 2", v, ok)
		}
	})
	t.Run("QoS 2 new hold expired in its wait", func(t *testing.T) {
		offset.Store(0)
		f := newInboundPipeFix(t, "pipe-exp-d", nil)
		newHoldBehindChain(f, 3*time.Second, 2)
		if v, ok := holdInterval(f, 5); !ok || v != 0 {
			t.Fatalf("the new hold carries interval %d (%v); fully elapsed carries 0", v, ok)
		}
		// Only the release of THIS hold counts: the exchange before it was delivered onward.
		submitted := f.recordSubmissions()
		f.send(f.rel(5))
		f.expect(mqttPacketPubComp, 5)
		if got := submitted(); len(got) != 0 {
			t.Fatalf("a hold that expired in its wait was delivered onward (%v)", got)
		}
	})
}

// TestMQTTInboundPipeSameIdentifierChain: a DUP PUBLISH whose reconcile is in progress holds
// back the PUBREL on its identifier (M14 lets the release start).
func TestMQTTInboundPipeSameIdentifierChain(t *testing.T) {
	f := newInboundPipeFix(t, "pipe-chain", nil)
	f.send(f.pub(2, 1))
	f.expect(mqttPacketPubRec, 1)
	submitted := f.recordSubmissions()

	load := newInboundGate()
	t.Cleanup(load.release)
	var armed atomic.Bool
	asm := f.asm
	asm.mu.Lock()
	asm.testQoS2Load = func(subject string) (*mqttQoS2Record, uint64, error) {
		if pi, ok := inboundAckHoldPI(subject); ok && pi == 1 && armed.CompareAndSwap(true, false) {
			load.park(nil)
		}
		return asm.jsa.loadQoS2Record(subject)
	}
	asm.mu.Unlock()
	t.Cleanup(func() {
		asm.mu.Lock()
		asm.testQoS2Load = nil
		asm.mu.Unlock()
	})
	starts := make(chan [2]uint64, 16)
	setInboundHook(t, &mqttTestInboundJobStartV, func(j *mqttInboundJob, blockedBy uint64) {
		if f.mine(j) && j.kind == mqttInboundPubRel {
			starts <- [2]uint64{j.seq, blockedBy}
		}
	})
	armed.Store(true)
	f.send(f.dup(f.pub(2, 1)), f.rel(1))
	load.wait(t, "the DUP's reconcile")
	select {
	case s := <-starts:
		if s[1] == 0 {
			t.Fatal("the PUBREL started while the DUP on its identifier was still reconciling")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the PUBREL's start was never decided")
	}
	load.release()
	f.expect(mqttPacketPubRec, 1)
	f.expect(mqttPacketPubComp, 1)
	if got := submitted(); len(got) != 1 || got[0] != f.subject(1) {
		t.Fatalf("onward stores %v, want exactly one for the exchange", got)
	}
	rec, _, err := f.asm.loadQoS2Record(clientByID(t, f.s, f.cid).mqttQoS2InternalSubject(1))
	if err != nil || (rec != nil && rec.state == mqttQoS2Held) {
		t.Fatalf("a hold is left on the identifier after PUBCOMP (%v)", err)
	}
}

// TestMQTTInboundPipeLeases: queued jobs take their leases in FIFO order (M15a), and a started
// job owns one (M15b).
func TestMQTTInboundPipeLeases(t *testing.T) {
	prev := mqttIncarnationDrainTimeout
	mqttIncarnationDrainTimeout = 3 * time.Second
	t.Cleanup(func() { mqttIncarnationDrainTimeout = prev })

	type ev struct {
		event  string
		owners int
	}
	t.Run("in FIFO order", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-lease-a", nil)
		f.send(f.pub(2, 7))
		f.expect(mqttPacketPubRec, 7)
		leaseA := f.pauseAt(mqttInboundQoS1, 1, "lease")
		decisions := make(chan [2]any, 64)
		setInboundHook(t, &mqttTestInboundLeaseDecisionV, func(j *mqttInboundJob, d string) {
			if f.mine(j) {
				decisions <- [2]any{j.pi, d}
			}
		})
		// A: QoS 1 PI 1, held before its lease. B: the PUBREL of PI 7, eligible.
		f.send(f.pub(1, 1), f.rel(7))
		leaseA.wait(t, "A's lease turn")
		for {
			d := <-decisions
			if d[0] == uint16(7) {
				if d[1] != "cursor" {
					t.Fatalf("the release behind A recorded %q; it must wait for the lease cursor", d[1])
				}
				break
			}
		}
		// A takeover; its retirement is held after the close until A has parked.
		events := make(chan ev, 64)
		closedGate := newInboundGate()
		t.Cleanup(closedGate.release)
		var held atomic.Bool
		setInboundHook(t, &mqttTestTrackerEventV, func(_ *mqttIncarnationTracker, e string, owners int) {
			events <- ev{e, owners}
			if e == "wait" && held.CompareAndSwap(false, true) {
				closedGate.park(nil)
			}
		})
		conn2 := inboundTakeoverAsync(t, f)
		closedGate.wait(t, "the takeover's retirement")
		leaseA.release()
		for {
			d := <-decisions
			if d[0] == uint16(1) && d[1] == "parked" {
				break
			}
			if d[0] == uint16(7) && d[1] == "acquired" {
				t.Fatal("the release took a lease while A, ahead of it, had none")
			}
		}
		closedGate.release()
		select {
		case <-conn2:
		case <-time.After(30 * time.Second):
			t.Fatal("the takeover never completed")
		}
		for len(events) > 0 {
			if e := <-events; e.event == "ceiling" {
				t.Fatal("the takeover's retirement hit its drain ceiling")
			}
		}
		for len(decisions) > 0 {
			if d := <-decisions; d[0] == uint16(7) && d[1] == "acquired" {
				t.Fatal("the release took a lease while A, ahead of it, had none")
			}
		}
	})
	t.Run("a started job owns one", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-lease-b", nil)
		f.send(f.pub(2, 7))
		f.expect(mqttPacketPubRec, 7)
		fences := f.pauseAt(mqttInboundPubRel, 7, "fences")
		readyAt := make(chan time.Time, 4)
		setInboundHook(t, &mqttTestInboundResponseReadyV, func(j *mqttInboundJob) {
			if f.mine(j) && j.pi == 7 {
				readyAt <- time.Now()
			}
		})
		f.send(f.rel(7), f.pub(1, 1))
		fences.wait(t, "the release's fences")
		events := make(chan ev, 64)
		setInboundHook(t, &mqttTestTrackerEventV, func(_ *mqttIncarnationTracker, e string, owners int) {
			events <- ev{e, owners}
		})
		conn2 := inboundTakeoverAsync(t, f)
		// The close evaluates the drain before the retirement waits on it: that evaluation must
		// count the started release, whose packet lease is long released.
		owners := -1
		for sawWait := false; !sawWait; {
			select {
			case e := <-events:
				switch e.event {
				case "eval":
					owners = e.owners
				case "wait":
					sawWait = true
				}
			case <-time.After(15 * time.Second):
				t.Fatal("the takeover never began its retirement")
			}
		}
		if owners < 1 {
			t.Fatalf("the retirement's drain evaluation counted %d owners while a started release was in its fences", owners)
		}
		select {
		case <-conn2:
			t.Fatal("the takeover completed while a started release held no lease")
		default:
		}
		fences.release()
		var ready time.Time
		select {
		case ready = <-readyAt:
		case <-time.After(15 * time.Second):
			t.Fatal("the release never completed")
		}
		select {
		case done := <-conn2:
			if done.Before(ready) {
				t.Fatal("the takeover completed before the release's lease was released")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the takeover never completed")
		}
	})
}

// TestMQTTInboundPipeFailureKeepsThePrefix: a failure at depth answers what came before it,
// then tells the client why and closes; nothing after it is answered, and the Will is
// published (M16: the failure position does not close the transport — a liveness kill, the
// one this file admits). A failure found when a client DISCONNECT drains the pipe makes the
// close ungraceful, so the Will is published (M17).
func TestMQTTInboundPipeFailureKeepsThePrefix(t *testing.T) {
	willSeen := func(f *inboundPipeFix, subject string) func() bool {
		var seen atomic.Bool
		setInboundHook(t, &mqttTestFanOutFailV, func(s string) bool {
			if s == subject {
				seen.Store(true)
			}
			return false
		})
		return seen.Load
	}
	will := func(topic string) *mqttWill {
		return &mqttWill{topic: []byte(topic), message: []byte("gone"), qos: 0}
	}
	t.Run("at depth", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-fail-depth-anchor", nil)
		f.nc.Close()
		f.cid = "pipe-fail-depth"
		f.nc, f.br = f.dial(f.cid, will("pipe/will/depth"))
		seen := willSeen(f, "pipe.will.depth")
		hold := f.holdStoreResult(nil, 1)
		failing := func(subject string, _ int, _ []byte) error {
			if subject == f.subject(2) {
				return errors.New("pipe test: store refused")
			}
			return nil
		}
		setInboundHook(t, &mqttTestStoreMsgHookV, failing)
		readyErr := make(chan error, 4)
		setInboundHook(t, &mqttTestInboundResponseReadyV, func(j *mqttInboundJob) {
			if f.mine(j) && j.pi == 2 {
				readyErr <- j.err
			}
		})
		f.send(f.pub(1, 1), f.pub(1, 2), f.pub(1, 3), f.pub(1, 4), f.pub(1, 5))
		hold.wait(t, "store 1")
		if err := <-readyErr; err == nil {
			t.Fatal("the failing store was answered as a success")
		}
		hold.release()
		f.expect(mqttPacketPubAck, 1)
		if got := f.expectDisconnect(); got == mqttReasonSuccess {
			t.Fatal("the failure's DISCONNECT carries no failure reason")
		}
		f.expectEOF(10 * time.Second)
		inboundAwaitClientGone(t, f.s, f.cid)
		if !seen() {
			t.Fatal("the Will of a connection closed by a failure was not published")
		}
	})
	t.Run("found at DISCONNECT", func(t *testing.T) {
		f := newInboundPipeFix(t, "pipe-fail-disc-anchor", nil)
		f.nc.Close()
		f.cid = "pipe-fail-disc"
		f.nc, f.br = f.dial(f.cid, will("pipe/will/disc"))
		seen := willSeen(f, "pipe.will.disc")
		hold := f.holdStoreResult(errors.New("pipe test: store failed"), 1)
		dispatch := make(chan struct{}, 4)
		setInboundHook(t, &mqttTestInboundDispatchV, func(c *client, pt byte, drain, _ bool) {
			if c.mqtt != nil && c.mqtt.cid == f.cid && pt == mqttPacketDisconnect && drain {
				dispatch <- struct{}{}
			}
		})
		entry := make(chan struct{}, 1)
		setInboundHook(t, &mqttTestClosedClientEntryV, func(c *client) {
			if c.mqtt != nil && c.mqtt.cid == f.cid {
				entry <- struct{}{}
			}
		})
		f.send(f.pub(1, 1), f.pub(1, 2), []byte{mqttPacketDisconnect, 0})
		hold.wait(t, "store 1")
		<-dispatch
		hold.release()
		// The Will is decided and published inside the teardown, so once the teardown has run
		// and the client is gone the answer is final: no waiting on an absence.
		select {
		case <-entry:
		case <-time.After(15 * time.Second):
			t.Fatal("the teardown never ran")
		}
		inboundAwaitClientGone(t, f.s, f.cid)
		if !seen() {
			t.Fatal("a DISCONNECT whose drain found a failure discarded the Will")
		}
	})
}

// refusedOrAdmitted returns a wait that reports which came first for f's client: the PUBLISH on
// pi being admitted (dispatched as a job: a Receive Maximum refusal never gets that far) or the
// readLoop's error exit for a Receive Maximum violation, which it reports before it completes
// the earlier, held packets. Both are positive events, so neither the correct broker nor its
// mutant leaves the test waiting on an absence.
func (f *inboundPipeFix) refusedOrAdmitted(pi uint16) func() (refused bool) {
	return inboundRefusedOrAdmitted(f.t, clientByID(f.t, f.s, f.cid), pi)
}

func inboundRefusedOrAdmitted(t *testing.T, sc *client, pi uint16) func() (refused bool) {
	admitted := make(chan struct{}, 4)
	setInboundHook(t, &mqttTestInboundSwitchV, func(c *client, _ mqttInboundKind, got uint16, _ bool) {
		if c == sc && got == pi {
			select {
			case admitted <- struct{}{}:
			default:
			}
		}
	})
	refused := make(chan struct{}, 4)
	setInboundHook(t, &mqttTestReadLoopErrorV, func(c *client, err error) {
		if c == sc && errors.Is(err, errMQTTReceiveMaxExceeded) {
			select {
			case refused <- struct{}{}:
			default:
			}
		}
	})
	return func() bool {
		select {
		case <-admitted:
			return false
		case <-refused:
			return true
		case <-time.After(15 * time.Second):
			t.Fatalf("PI %d was neither admitted nor refused", pi)
			return false
		}
	}
}

// inboundAwaitAsync awaits a submitted store on its own goroutine. Kept apart from the tests
// that arm hooks: a function that takes a global's address launches no goroutine
// (TestNoPackageGlobalIsAssignedFromAGoroutine).
func inboundAwaitAsync(p *mqttPendingStore) <-chan error {
	got := make(chan error, 1)
	go func() { _, err := p.await(); got <- err }()
	return got
}

// inboundTakeoverAsync connects a second client with f's client ID on its own goroutine and
// reports when its CONNACK arrived; the connection is closed when the test ends.
func inboundTakeoverAsync(t *testing.T, f *inboundPipeFix) <-chan time.Time {
	done := make(chan time.Time, 1)
	conns := make(chan net.Conn, 1)
	t.Cleanup(func() {
		select {
		case c := <-conns:
			c.Close()
		default:
		}
	})
	go func() {
		c, err := net.Dial("tcp", net.JoinHostPort(f.o.MQTT.Host, strconv.Itoa(f.o.MQTT.Port)))
		if err != nil {
			return
		}
		conns <- c
		if _, err := c.Write(mqttV5CreateConnect(&mqttV5ConnInfo{clientID: f.cid, cleanStart: true})); err != nil {
			return
		}
		if pt, _, err := inboundAckBenchReadPacket(bufio.NewReader(c)); err == nil && pt&mqttPacketMask == mqttPacketConnectAck {
			done <- time.Now()
		}
	}()
	return done
}

// inboundAwaitClientGone waits until no client with id is registered: its closeConnection,
// which follows mqttHandleClosedClient on the readLoop, has run.
func inboundAwaitClientGone(t *testing.T, s *Server, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		s.mu.Lock()
		found := false
		for _, c := range s.clients {
			c.mu.Lock()
			if c.mqtt != nil && c.mqtt.cid == id {
				found = true
			}
			c.mu.Unlock()
		}
		s.mu.Unlock()
		if !found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("client %q never left the server", id)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
