//go:build mqtt_fulltests

package mqtt

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// These tests pin two observable properties of the inbound acknowledgement path that a
// throughput figure cannot show, and that any pipelining of the path has to keep:
//
//   - An acknowledgement is never enqueued before the durable effect it reports returned: a
//     PUBACK before its $MQTT_msgs store, a PUBREC before its hold's create CAS, a successful
//     PUBCOMP before its terminal CAS. Checked at the moment the response is enqueued against
//     effects recorded as they return, so an early acknowledgement that happens is caught
//     however fast it was.
//   - Stores are issued, and responses written, in the order of the packets that caused them.
//
// They observe; they do not force a wrong path to misbehave. The ordering tests hold an
// earlier packet's work for up to a second to put pressure on the order, but a path that
// handles one packet at a time never lets a later packet overtake, and the tests claim
// nothing about whether an overtaking was attempted. They arm process-global hooks, so none
// of them runs in parallel.

// inboundAckFacts records what the broker reported, as it returned.
type inboundAckFacts struct {
	mu        sync.Mutex
	issued    []string        // $MQTT_msgs subjects in the order their stores were issued
	stored    map[string]bool // $MQTT_msgs subject -> its store returned success
	held      map[uint16]bool // packet identifier -> its hold create CAS returned success
	finalized map[uint16]bool // packet identifier -> its terminal CAS returned success
	releasing bool            // a PUBREL has been sent: hold-subject stores are no longer creates
	errs      []string
}

func inboundAckHoldPI(subject string) (uint16, bool) {
	i := strings.LastIndexByte(subject, '.')
	if i < 0 {
		return 0, false
	}
	n, err := strconv.ParseUint(subject[i+1:], 10, 16)
	return uint16(n), err == nil
}

// inboundAckProbe is one fresh standalone broker, its account session manager, the facts its
// hooks record, and one raw v5 client.
type inboundAckProbe struct {
	t           *testing.T
	asm         *mqttAccountSessionManager
	f           *inboundAckFacts
	storePrefix string
	nc          net.Conn
	br          *bufio.Reader
}

// newInboundAckProbe starts a broker with interest on the probe's subjects, arms the recording
// hooks, and connects cid. hold, when set, runs in the post-store hook before a $MQTT_msgs
// store's success is recorded and passed on; the ordering tests use it to keep one store's
// result back. Every hook is removed before the test returns.
func newInboundAckProbe(t *testing.T, cid string, hold func(pi uint16)) *inboundAckProbe {
	t.Helper()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	p := &inboundAckProbe{
		t: t, asm: asmOf(t, s),
		f:           &inboundAckFacts{stored: map[string]bool{}, held: map[uint16]bool{}, finalized: map[uint16]bool{}},
		storePrefix: mqttStreamSubjectPrefix + "probe.",
	}
	// Interest, so $MQTT_msgs keeps what the probe stores rather than discarding it.
	nc, js := jsClientConnect(t, s)
	t.Cleanup(nc.Close)
	if _, err := js.AddConsumer(mqttStreamName, &nats.ConsumerConfig{
		Durable: "inbound-ack-probe", FilterSubject: p.storePrefix + ">", AckPolicy: nats.AckExplicitPolicy,
	}); err != nil {
		t.Fatalf("interest consumer: %v", err)
	}
	if hold == nil {
		hold = func(uint16) {}
	}
	f, asm := p.f, p.asm

	preStore := func(subject string, _ int, _ []byte) error {
		if strings.HasPrefix(subject, p.storePrefix) {
			f.mu.Lock()
			f.issued = append(f.issued, subject)
			f.mu.Unlock()
		}
		return nil
	}
	mqttTestStoreMsgHookV.Store(&preStore)
	// The hold create CAS stores through jsa.storeMsg too (qos2_move.go), so it is recorded
	// here: before any PUBREL, the only successful store on a hold subject is a create.
	postStore := func(subject string, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if err != nil {
			return resp, err
		}
		switch {
		case strings.HasPrefix(subject, p.storePrefix):
			pi, _ := inboundAckHoldPI(subject)
			hold(pi)
			f.mu.Lock()
			f.stored[subject] = true
			f.mu.Unlock()
		case strings.HasPrefix(subject, mqttQoS2IncomingMsgsStreamSubjectPrefix) &&
			!strings.HasSuffix(subject, mqttQoS2ClaimSubjectSuffix):
			if pi, ok := inboundAckHoldPI(subject); ok {
				f.mu.Lock()
				if !f.releasing {
					f.held[pi] = true
				}
				f.mu.Unlock()
			}
		}
		return resp, err
	}
	mqttTestStoreMsgResultHookV.Store(&postStore)

	asm.mu.Lock()
	asm.testQoS2FinalizeStore = func(subject, gen string, expectSeq uint64) (uint64, error) {
		seq, err := asm.jsa.storeQoS2DoneCAS(subject, gen, expectSeq)
		if pi, ok := inboundAckHoldPI(subject); ok && err == nil {
			f.mu.Lock()
			f.finalized[pi] = true
			f.mu.Unlock()
		}
		return seq, err
	}
	asm.mu.Unlock()

	frame := func(c *client, _ string, b []byte) {
		if c.mqtt == nil || c.mqtt.cid != cid || len(b) < 4 {
			return
		}
		pt := b[0] & mqttPacketMask
		pi := uint16(b[2])<<8 | uint16(b[3])
		success := b[1] == 2 // the two-byte form has no reason code: success
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case pt == mqttPacketPubAck && success && !f.stored[p.subject(pi)]:
			f.errs = append(f.errs, fmt.Sprintf("PUBACK %d enqueued before its $MQTT_msgs store returned", pi))
		case pt == mqttPacketPubRec && success && !f.held[pi]:
			f.errs = append(f.errs, fmt.Sprintf("PUBREC %d enqueued before its hold CAS returned", pi))
		case pt == mqttPacketPubComp && success && !f.finalized[pi]:
			// Scoped to the probe's own exchanges, all fresh and ordinary: a success PUBCOMP
			// with no terminal CAS is legitimate for a tombstone or an expired hold, which the
			// probe never creates.
			f.errs = append(f.errs, fmt.Sprintf("PUBCOMP %d enqueued before its terminal CAS returned", pi))
		}
	}
	mqttTestMixedSiteFrameV.Store(&frame)

	t.Cleanup(func() {
		mqttTestMixedSiteFrameV.Store(nil)
		mqttTestStoreMsgResultHookV.Store(nil)
		mqttTestStoreMsgHookV.Store(nil)
		asm.mu.Lock()
		asm.testQoS2FinalizeStore = nil
		asm.testQoS2Load = nil
		asm.mu.Unlock()
	})

	c, mr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: cid, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	t.Cleanup(func() { c.Close() })
	if _, reason, _ := testMQTTReadConnAckV5(t, mr); reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNACK reason %#x", reason)
	}
	p.nc, p.br = c, bufio.NewReader(c)
	return p
}

// topic is the probe topic of the exchange using pi: one topic per exchange, so a stored
// subject names exactly one exchange.
func (p *inboundAckProbe) topic(pi uint16) string { return "probe/" + strconv.Itoa(int(pi)) }

func (p *inboundAckProbe) subject(pi uint16) string { return p.storePrefix + strconv.Itoa(int(pi)) }

func (p *inboundAckProbe) publish(qos byte, pi uint16) []byte {
	return inboundAckBenchPublish(qos, p.topic(pi), pi, []byte("probe"))
}

// release builds PUBRELs, and marks the probe as releasing before any is written, so a later
// store on a hold subject (the terminal CAS) is not mistaken for a create.
func (p *inboundAckProbe) release(pis ...uint16) []byte {
	p.f.mu.Lock()
	p.f.releasing = true
	p.f.mu.Unlock()
	var b []byte
	for _, pi := range pis {
		b = append(b, mqttPacketPubRel|0x2, 0x2, byte(pi>>8), byte(pi))
	}
	return b
}

func (p *inboundAckProbe) send(pkts ...[]byte) {
	p.t.Helper()
	var all []byte
	for _, b := range pkts {
		all = append(all, b...)
	}
	if _, err := p.nc.Write(all); err != nil {
		p.t.Fatalf("write: %v", err)
	}
}

// expect reads the next response and requires its type, identifier and a success reason.
func (p *inboundAckProbe) expect(pt byte, pi uint16) {
	p.t.Helper()
	_ = p.nc.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, body, err := inboundAckBenchReadPacket(p.br)
	if err != nil {
		p.t.Fatalf("reading the response owed for %#x %d: %v", pt, pi, err)
	}
	if len(body) < 2 {
		p.t.Fatalf("response %#x without a packet identifier", got)
	}
	gotPI := uint16(body[0])<<8 | uint16(body[1])
	if got&mqttPacketMask != pt || gotPI != pi {
		p.t.Fatalf("received %#x %d when %#x %d was owed next", got&mqttPacketMask, gotPI, pt, pi)
	}
	if len(body) > 2 && body[2] != mqttReasonSuccess {
		p.t.Fatalf("%#x %d carries reason %#x", pt, pi, body[2])
	}
}

// checkFacts reports every early acknowledgement the frame hook saw.
func (p *inboundAckProbe) checkFacts() {
	p.t.Helper()
	p.f.mu.Lock()
	defer p.f.mu.Unlock()
	for _, e := range p.f.errs {
		p.t.Error(e)
	}
}

func (p *inboundAckProbe) requireIssued(want ...uint16) {
	p.t.Helper()
	var subjects []string
	for _, pi := range want {
		subjects = append(subjects, p.subject(pi))
	}
	p.f.mu.Lock()
	defer p.f.mu.Unlock()
	if got := strings.Join(p.f.issued, " "); got != strings.Join(subjects, " ") {
		p.t.Fatalf("$MQTT_msgs stores issued in order %v, want %v", p.f.issued, subjects)
	}
}

// inboundAckPressure holds one piece of work until a later packet's progress is seen or a
// second has passed, and logs which. Neither outcome passes or fails anything.
type inboundAckPressure struct {
	signal chan struct{}
	once   sync.Once
}

func newInboundAckPressure() *inboundAckPressure {
	return &inboundAckPressure{signal: make(chan struct{})}
}

func (g *inboundAckPressure) open() { g.once.Do(func() { close(g.signal) }) }

func (g *inboundAckPressure) hold(t *testing.T) {
	select {
	case <-g.signal:
		t.Log("a later packet progressed while the earlier one was held")
	case <-time.After(time.Second):
		t.Log("no later packet progressed while the earlier one was held")
	}
}

// holdLoad holds the first hold load of pi made after arm closes, then performs the real load.
func (p *inboundAckProbe) holdLoad(arm <-chan struct{}, pi uint16, g *inboundAckPressure) {
	var once sync.Once
	asm := p.asm
	asm.mu.Lock()
	asm.testQoS2Load = func(subject string) (*mqttQoS2Record, uint64, error) {
		select {
		case <-arm:
			if got, ok := inboundAckHoldPI(subject); ok && got == pi {
				once.Do(func() { g.hold(p.t) })
			}
		default:
		}
		return asm.jsa.loadQoS2Record(subject)
	}
	asm.mu.Unlock()
}

func TestMQTTInboundAckFollowsDurability(t *testing.T) {
	const burst = 64
	p := newInboundAckProbe(t, "inbound-ack-durability", nil)

	// Each burst is written whole before any response is read, so a path that overlaps
	// packets has every opportunity to answer one early.
	var q1 [][]byte
	for pi := uint16(1); pi <= burst; pi++ {
		q1 = append(q1, p.publish(1, pi))
	}
	p.send(q1...)
	for pi := uint16(1); pi <= burst; pi++ {
		p.expect(mqttPacketPubAck, pi)
	}
	var q2 [][]byte
	var pis []uint16
	for pi := uint16(101); pi < 101+burst; pi++ {
		q2 = append(q2, p.publish(2, pi))
		pis = append(pis, pi)
	}
	p.send(q2...)
	for _, pi := range pis {
		p.expect(mqttPacketPubRec, pi)
	}
	p.send(p.release(pis...))
	for _, pi := range pis {
		p.expect(mqttPacketPubComp, pi)
	}
	p.checkFacts()

	// The probe must have watched what it claims to.
	p.f.mu.Lock()
	defer p.f.mu.Unlock()
	if len(p.f.stored) != 2*burst || len(p.f.held) != burst || len(p.f.finalized) != burst {
		t.Fatalf("recorded %d stores, %d holds, %d terminal commits; want %d, %d, %d",
			len(p.f.stored), len(p.f.held), len(p.f.finalized), 2*burst, burst, burst)
	}
}

func TestMQTTInboundAckOrderUnderPressure(t *testing.T) {
	t.Run("release after release", func(t *testing.T) {
		p := newInboundAckProbe(t, "inbound-ack-order-rr", nil)
		arm := make(chan struct{})
		g := newInboundAckPressure()
		p.holdLoad(arm, 1, g)
		sampled := func(mqttQoS2RetireVerdict) {
			select {
			case <-arm:
				g.open()
			default:
			}
		}
		mqttTestQoS2RetireSampledHookV.Store(&sampled)
		t.Cleanup(func() { mqttTestQoS2RetireSampledHookV.Store(nil) })

		p.send(p.publish(2, 1), p.publish(2, 2))
		p.expect(mqttPacketPubRec, 1)
		p.expect(mqttPacketPubRec, 2)
		close(arm)
		p.send(p.release(1, 2))
		p.expect(mqttPacketPubComp, 1)
		p.expect(mqttPacketPubComp, 2)
		p.checkFacts()
		p.requireIssued(1, 2)
	})

	t.Run("publishes after a release", func(t *testing.T) {
		p := newInboundAckProbe(t, "inbound-ack-order-rp", nil)
		arm := make(chan struct{})
		g := newInboundAckPressure()
		p.holdLoad(arm, 1, g)
		var parsed int
		var mu sync.Mutex
		parse := func() {
			select {
			case <-arm:
				mu.Lock()
				if parsed++; parsed == 2 {
					g.open()
				}
				mu.Unlock()
			default:
			}
		}
		mqttTestPublishGatePauseV.Store(&parse)
		t.Cleanup(func() { mqttTestPublishGatePauseV.Store(nil) })

		p.send(p.publish(2, 1))
		p.expect(mqttPacketPubRec, 1)
		close(arm)
		p.send(p.release(1), p.publish(1, 2), p.publish(1, 3))
		p.expect(mqttPacketPubComp, 1)
		p.expect(mqttPacketPubAck, 2)
		p.expect(mqttPacketPubAck, 3)
		p.checkFacts()
		p.requireIssued(1, 2, 3)
	})

	t.Run("completion after completion", func(t *testing.T) {
		g := newInboundAckPressure()
		var p *inboundAckProbe
		p = newInboundAckProbe(t, "inbound-ack-order-cc", func(pi uint16) {
			switch pi {
			case 1:
				g.hold(p.t)
			case 2:
				g.open()
			}
		})
		p.send(p.publish(1, 1), p.publish(1, 2))
		p.expect(mqttPacketPubAck, 1)
		p.expect(mqttPacketPubAck, 2)
		p.checkFacts()
		p.requireIssued(1, 2)
	})
}
