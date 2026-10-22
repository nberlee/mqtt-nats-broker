//go:build mqttport_fulltests

package mqtt

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// The inbound acknowledgement benchmark measures how many QoS 1 and QoS 2 exchanges an MQTT
// connection completes per second, and how long each waits for its acknowledgement, against
// a standalone broker and against one on a three-node JetStream cluster. It is the
// measurement the roadmap's evaluation of a pipelined acknowledgement path rests on, and it
// is a test rather than a Go benchmark because the cluster harness is *testing.T-only and
// because a number is reported only for a run whose effects checked out.
//
// Three properties have to hold for a row to mean anything, and each has its own check:
//
//   - The acknowledgements arrive in the order the client's packets demand: the client keeps
//     a ledger of the responses its own writes are owed, in the order it wrote them.
//   - The broker stored exactly what it acknowledged, each once, in publish order: the
//     stored messages are read back through an interest consumer after the run. That is an
//     EVENTUAL check; it cannot see an acknowledgement sent before its store landed.
//   - No acknowledgement precedes its durable effect: TestMQTTInboundAckFollowsDurability.
//
// The consumer is also what gives $MQTT_msgs interest: the stream is interest-retained, so
// without one every store would be acknowledged and discarded, and a broker that discarded
// faster would look faster.
//
// Opt-in: MQTTNB_INBOUND_ACK_BENCH=1 (make bench-inbound). MQTTNB_INBOUND_ACK_BENCH_OUT
// names a CSV file the rows are appended to, and MQTTNB_INBOUND_ACK_BENCH_LABEL labels them
// with the tree being measured.

const (
	inboundAckBenchEnv      = "MQTTNB_INBOUND_ACK_BENCH"
	inboundAckBenchOutEnv   = "MQTTNB_INBOUND_ACK_BENCH_OUT"
	inboundAckBenchLabelEnv = "MQTTNB_INBOUND_ACK_BENCH_LABEL"

	inboundAckBenchPayload  = 128
	inboundAckBenchConsumer = "inbound-ack-bench"
	inboundAckBenchTimeout  = 10 * time.Minute
)

type inboundAckBenchCase struct {
	qos     byte
	conns   int
	window  int
	perConn int
}

func (bc inboundAckBenchCase) name() string {
	return fmt.Sprintf("qos%d_conns%d_window%d", bc.qos, bc.conns, bc.window)
}

// The matrix is fixed so that runs of different trees compare row by row. The 64-connection
// window-1 rows are the typical device fleet: many clients each keeping one exchange
// outstanding, which the inbound pipeline's direct path exists for.
var inboundAckBenchMatrix = []inboundAckBenchCase{
	{qos: 1, conns: 1, window: 1, perConn: 20000},
	{qos: 1, conns: 1, window: 256, perConn: 20000},
	{qos: 1, conns: 8, window: 256, perConn: 5000},
	{qos: 1, conns: 64, window: 1, perConn: 500},
	{qos: 2, conns: 1, window: 1, perConn: 5000},
	{qos: 2, conns: 1, window: 256, perConn: 5000},
	{qos: 2, conns: 8, window: 256, perConn: 1000},
	{qos: 2, conns: 64, window: 1, perConn: 150},
}

// inboundAckBenchRig is one broker under measurement: where MQTT clients dial, a JetStream
// context on the same account for the interest consumer, the URL of the node the broker's
// own connection uses (the census taps there), and the nats-servers whose message counters
// give a descriptive traffic figure.
type inboundAckBenchRig struct {
	broker  *Server
	host    string
	port    int
	js      nats.JetStreamContext
	tapURL  string
	servers []*natsserver.Server
}

func (r inboundAckBenchRig) inMsgs(t *testing.T) int64 {
	t.Helper()
	var n int64
	for _, ns := range r.servers {
		v, err := ns.Varz(nil)
		if err != nil {
			t.Fatalf("varz: %v", err)
		}
		n += v.InMsgs
	}
	return n
}

func TestMQTTInboundAckThroughput(t *testing.T) {
	if os.Getenv(inboundAckBenchEnv) != "1" {
		t.Skipf("set %s=1 to run the inbound acknowledgement benchmark", inboundAckBenchEnv)
	}
	rigs := []struct {
		name  string
		start func(t *testing.T) inboundAckBenchRig
	}{
		{"standalone", inboundAckBenchStandalone},
		{"r3", inboundAckBenchR3},
	}
	for _, rig := range rigs {
		t.Run(rig.name, func(t *testing.T) {
			for _, bc := range inboundAckBenchMatrix {
				t.Run(bc.name(), func(t *testing.T) {
					// A fresh broker per case, so no case inherits another's streams,
					// consumers or session records.
					r := rig.start(t)
					res := runInboundAckBenchCase(t, r, bc)
					inboundAckBenchReport(t, rig.name, bc, res)
				})
			}
		})
	}
}

func inboundAckBenchStandalone(t *testing.T) inboundAckBenchRig {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	t.Cleanup(func() { testMQTTShutdownServer(s) })
	nc, js := jsClientConnect(t, s)
	t.Cleanup(nc.Close)
	return inboundAckBenchRig{broker: s, host: o.MQTT.Host, port: o.MQTT.Port, js: js, tapURL: s.ClientURL(),
		servers: []*natsserver.Server{testEmbedded.paired(s)}}
}

func inboundAckBenchR3(t *testing.T) inboundAckBenchRig {
	cl := testNewJetStreamCluster(t, 3)
	b := cl.startBroker(t, 0)
	nc, err := nats.Connect(strings.Join(cl.orderedURLs(1), ","))
	if err != nil {
		t.Fatalf("bench NATS connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("bench JetStream context: %v", err)
	}
	// The census taps the node the broker's service connection is on, where its publishes
	// enter the cluster.
	r := inboundAckBenchRig{broker: b.s, host: b.opts.MQTT.Host, port: b.opts.MQTT.Port, js: js,
		tapURL: b.s.nrc.ConnectedUrl()}
	for _, n := range cl.nodes {
		r.servers = append(r.servers, n.ns)
	}
	return r
}

type inboundAckBenchResult struct {
	completed    int
	elapsed      time.Duration
	firstAck     []time.Duration // PUBLISH -> PUBACK (QoS 1) or PUBREC (QoS 2)
	completion   []time.Duration // PUBLISH -> PUBCOMP (QoS 2 only)
	msgsPerExchg float64
	rssKB        int64
	goroutines   int
	census       map[string]int64
}

// inboundAckCensus counts, by class, every JetStream request and broker-owned publish the
// broker makes during a case, through the shared request census (jetstream_request_census_test.go):
// a plain subscriber on the account, so it sees requests at the broker's own boundary — no-wait
// deletes and atomic-batch members included — without instrumenting the broker, and identically
// whichever tree is measured. Comparing the classes per exchange across two trees is what shows a
// change overlapped requests rather than dropping any. This bench classes with its own classifier
// and counts batch members per batch.
type inboundAckCensus struct {
	*jsRequestCensus
}

func startInboundAckCensus(t *testing.T, url string) *inboundAckCensus {
	t.Helper()
	return &inboundAckCensus{startJSRequestCensus(t, url, []string{"$JS.API.>", mqttPrefix + ">"},
		inboundAckCensusClass, inboundAckCensusBatchMembers)}
}

// inboundAckCensusBatchMembers buckets members per batch, by how many a batch had: a batch that
// lost a member moves to a different bucket even when the number of commits is unchanged.
func inboundAckCensusBatchMembers(cs *jsRequestCensus, m *nats.Msg) {
	if id := m.Header.Get(JSBatchId); id != _EMPTY_ && m.Header.Get(JSBatchCommit) == _EMPTY_ {
		cs.counts["batch-members:"+id]++
	}
}

// inboundAckCensusClass names the class of a message the tap saw, and reports false for one
// that is not the broker's: the test's own requests reply on _INBOX.
func inboundAckCensusClass(m *nats.Msg) (string, bool) {
	subj, reply := m.Subject, m.Reply
	for _, p := range []string{"$MQTT.JSA.", "$MQTT.inbox."} {
		if strings.HasPrefix(subj, p) {
			// A reply to the broker: $MQTT.JSA.<node>.<kind>.<nuid> is classed by kind.
			if tk := strings.Split(subj, "."); p == "$MQTT.JSA." && len(tk) >= 5 {
				return "reply:" + tk[3], true
			}
			return "reply:inbox", true
		}
	}
	broker := strings.HasPrefix(reply, "$MQTT.JSA.") || strings.HasPrefix(reply, "$MQTT.inbox.") ||
		(reply == _EMPTY_ && strings.HasPrefix(subj, "$JS.API.")) ||
		(reply == _EMPTY_ && m.Header.Get(JSBatchId) != _EMPTY_)
	if !broker {
		return _EMPTY_, false
	}
	var class string
	if api, ok := strings.CutPrefix(subj, "$JS.API."); ok {
		// The verb and the stream it names; consumer and subject tokens after the stream
		// vary per session and are not part of the class.
		var keep []string
		for _, tk := range strings.Split(api, ".") {
			keep = append(keep, tk)
			if strings.HasPrefix(tk, "$") {
				break
			}
		}
		class = "api:" + strings.Join(keep, ".")
	} else {
		rest := strings.TrimPrefix(subj, mqttPrefix)
		family, _, _ := strings.Cut(rest, ".")
		class = "pub:" + family
		if strings.HasPrefix(subj, mqttQoS2IncomingMsgsStreamSubjectPrefix) {
			switch {
			case strings.HasSuffix(subj, mqttQoS2ClaimSubjectSuffix):
				class = "pub:qos2in-claim"
			case m.Header.Get(mqttQoS2DoneHeader) != _EMPTY_:
				class = "pub:qos2in-tombstone"
			default:
				class = "pub:qos2in-hold"
			}
		}
	}
	if m.Header.Get(JSBatchId) != _EMPTY_ {
		if m.Header.Get(JSBatchCommit) != _EMPTY_ {
			class += "+batch-commit"
		} else {
			class += "+batch-member"
		}
	}
	return class, true
}

// drain waits until every request the broker made for the case has been counted, and returns
// the counts. For QoS 2 the asynchronous reaps must have taken effect (the hold stream is
// empty, which a fresh broker per case makes exact), which proves the broker published them;
// every other request of the path was awaited before its terminal response. Then the broker's
// service connection flushes: the server handles that connection's messages in order and
// delivers each to local subscribers as it handles it, so when the PONG arrives every copy is
// queued to the tap (an effect seen through another connection does not prove that: the
// JetStream worker can act first). Then the tap's own fence comes back behind them, every
// subscription has dropped nothing, and the connection reported no error.
func (cs *inboundAckCensus) drain(t *testing.T, r inboundAckBenchRig, qos byte) map[string]int64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	if qos == 2 {
		for {
			si, err := r.js.StreamInfo(mqttQoS2IncomingMsgsStreamName)
			if err != nil {
				t.Fatalf("census drain: hold stream info: %v", err)
			}
			if si.State.Msgs == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("census drain: %d records still on %s after 30s", si.State.Msgs, mqttQoS2IncomingMsgsStreamName)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	snap := cs.snapshot(t, r.broker.nrc.Flush)
	out := make(map[string]int64, len(snap.counts))
	batches := map[int64]int64{}
	for k, v := range snap.counts {
		if strings.HasPrefix(k, "batch-members:") {
			batches[v]++
			continue
		}
		out[k] = v
	}
	for members, n := range batches {
		out["batches-with-members:"+strconv.FormatInt(members, 10)] = n
	}
	cs.nc.Close()
	return out
}

func runInboundAckBenchCase(t *testing.T, r inboundAckBenchRig, bc inboundAckBenchCase) inboundAckBenchResult {
	t.Helper()
	tag := strings.NewReplacer("/", "_", "#", "_", "+", "_").Replace(t.Name())
	cfg := &nats.ConsumerConfig{
		Durable:       inboundAckBenchConsumer,
		FilterSubject: mqttStreamSubjectPrefix + "bench." + tag + ".>",
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       10 * time.Minute,
		MaxAckPending: -1,
	}
	if _, err := r.js.AddConsumer(mqttStreamName, cfg); err != nil {
		t.Fatalf("interest consumer: %v", err)
	}

	conns := make([]*inboundAckBenchConn, bc.conns)
	for i := range conns {
		conns[i] = dialInboundAckBenchConn(t, r, fmt.Sprintf("%s-%d", tag, i), fmt.Sprintf("bench/%s/%d", tag, i), bc)
	}
	census := startInboundAckCensus(t, r.tapURL)
	before := r.inMsgs(t)
	start := time.Now()
	errCh := make(chan error, bc.conns)
	for _, c := range conns {
		go func() { errCh <- c.run() }()
	}
	deadline := time.After(inboundAckBenchTimeout)
	for range conns {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("bench connection: %v", err)
			}
		case <-deadline:
			t.Fatalf("bench case did not complete within %v", inboundAckBenchTimeout)
		}
	}
	var res inboundAckBenchResult
	var last time.Time
	for _, c := range conns {
		res.completed += c.completed
		if c.lastDone.After(last) {
			last = c.lastDone
		}
		res.firstAck = append(res.firstAck, c.firstAck...)
		res.completion = append(res.completion, c.completion...)
	}
	res.elapsed = last.Sub(start)
	res.msgsPerExchg = float64(r.inMsgs(t)-before) / float64(res.completed)
	res.rssKB = inboundAckBenchRSS()
	res.goroutines = runtime.NumGoroutine()
	// The connections stay open through the census and the read-back: closing them starts
	// session teardown, whose requests are not the exchanges' and must not race the drain.
	res.census = census.drain(t, r, bc.qos)

	stored := inboundAckBenchReadBack(t, r.js, cfg.FilterSubject)
	for _, c := range conns {
		if err := c.quiet(); err != nil {
			t.Fatalf("bench connection %s: %v", c.conn, err)
		}
		c.close()
	}
	sent := make(map[string][]byte, bc.conns*bc.perConn)
	for _, c := range conns {
		for seq := 0; seq < bc.perConn; seq++ {
			sent[c.conn+":"+strconv.Itoa(seq)] = inboundAckBenchPayloadFor(c.conn, seq)
		}
	}
	if err := inboundAckBenchOracle(bc.conns*bc.perConn, sent, stored); err != nil {
		t.Fatalf("the stored messages do not match the acknowledged exchanges: %v", err)
	}
	return res
}

// inboundAckBenchExpect is one response the client's own writes are owed.
type inboundAckBenchExpect struct {
	pt  byte
	pi  uint16
	seq int
}

// inboundAckBenchConn is one MQTT v5 publisher holding up to window exchanges outstanding.
type inboundAckBenchConn struct {
	bc    inboundAckBenchCase
	nc    net.Conn
	br    *bufio.Reader
	conn  string // the client ID, and the tag every payload carries
	topic string

	// wmu serializes whole packet writes AND the ledger appends that go with them, so the
	// ledger is in actual write order whichever goroutine wrote.
	wmu    sync.Mutex
	ledger []inboundAckBenchExpect

	sem chan struct{} // one slot per outstanding exchange, released at its terminal response
	// sent[seq] is when PUBLISH seq was written. Its packet identifier is seq%65535+1, which
	// cannot collide while at most window (< 65535) exchanges are outstanding.
	sent       []time.Time
	completed  int
	lastDone   time.Time
	firstAck   []time.Duration
	completion []time.Duration
}

func dialInboundAckBenchConn(t *testing.T, r inboundAckBenchRig, conn, topic string, bc inboundAckBenchCase) *inboundAckBenchConn {
	t.Helper()
	nc, mr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: conn, cleanStart: true}, r.host, r.port)
	if _, reason, _ := testMQTTReadConnAckV5(t, mr); reason != mqttConnAckRCConnectionAccepted {
		nc.Close()
		t.Fatalf("CONNACK reason %#x", reason)
	}
	return &inboundAckBenchConn{
		bc:    bc,
		nc:    nc,
		br:    bufio.NewReaderSize(nc, 64*1024),
		conn:  conn,
		topic: topic,
		sem:   make(chan struct{}, bc.window),
		sent:  make([]time.Time, bc.perConn),
	}
}

func (c *inboundAckBenchConn) close() { c.nc.Close() }

func inboundAckBenchPI(seq int) uint16 { return uint16(seq%65535 + 1) }

// inboundAckBenchPayloadFor is the exact payload of message seq on connection conn.
func inboundAckBenchPayloadFor(conn string, seq int) []byte {
	p := bytes.Repeat([]byte{'.'}, inboundAckBenchPayload)
	copy(p, conn+":"+strconv.Itoa(seq)+":")
	return p
}

// write sends one packet and records the response it is owed, atomically with respect to
// every other write on this connection.
func (c *inboundAckBenchConn) write(pkt []byte, owed inboundAckBenchExpect) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.ledger = append(c.ledger, owed)
	_, err := c.nc.Write(pkt)
	return err
}

func (c *inboundAckBenchConn) nextOwed() (inboundAckBenchExpect, bool) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if len(c.ledger) == 0 {
		return inboundAckBenchExpect{}, false
	}
	e := c.ledger[0]
	c.ledger = c.ledger[1:]
	return e, true
}

func (c *inboundAckBenchConn) run() error {
	rerr := make(chan error, 1)
	go func() { rerr <- c.readResponses() }()
	first := byte(mqttPacketPubAck)
	if c.bc.qos == 2 {
		first = mqttPacketPubRec
	}
	for seq := 0; seq < c.bc.perConn; seq++ {
		select {
		case c.sem <- struct{}{}:
		case err := <-rerr:
			return fmt.Errorf("reader ended after %d of %d PUBLISHes: %w", seq, c.bc.perConn, err)
		}
		pi := inboundAckBenchPI(seq)
		pkt := inboundAckBenchPublish(c.bc.qos, c.topic, pi, inboundAckBenchPayloadFor(c.conn, seq))
		c.sent[seq] = time.Now()
		if err := c.write(pkt, inboundAckBenchExpect{pt: first, pi: pi, seq: seq}); err != nil {
			return err
		}
	}
	if err := <-rerr; err != nil {
		return err
	}
	return c.fence()
}

// fence ends the ledger's span: a PINGREQ after every owed response, whose PINGRESP must be
// the next packet. A response the broker sent beyond what was owed (a second PUBACK or
// PUBCOMP for the last exchange, say) arrives before it and fails the case.
func (c *inboundAckBenchConn) fence() error {
	c.wmu.Lock()
	_, err := c.nc.Write([]byte{mqttPacketPing, 0})
	c.wmu.Unlock()
	if err != nil {
		return err
	}
	_ = c.nc.SetReadDeadline(time.Now().Add(30 * time.Second))
	defer func() { _ = c.nc.SetReadDeadline(time.Time{}) }()
	pt, body, err := inboundAckBenchReadPacket(c.br)
	if err != nil {
		return fmt.Errorf("reading the PINGRESP after the last owed response: %w", err)
	}
	if pt != mqttPacketPingResp || len(body) != 0 {
		return fmt.Errorf("packet %#x arrived after the last owed response, before the PINGRESP", pt)
	}
	return nil
}

// quiet reports a packet that arrived after the fence: nothing more is owed, so anything the
// broker wrote since (a late duplicate acknowledgement) is a defect.
func (c *inboundAckBenchConn) quiet() error {
	if c.br.Buffered() > 0 {
		return fmt.Errorf("%d unexpected bytes arrived after the last owed response", c.br.Buffered())
	}
	_ = c.nc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	defer func() { _ = c.nc.SetReadDeadline(time.Time{}) }()
	if _, err := c.br.ReadByte(); err == nil {
		return errors.New("a packet arrived after the last owed response")
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		return fmt.Errorf("the connection failed after the last owed response: %w", err)
	}
	return nil
}

// readResponses matches every response against the head of the ledger: the type, the
// identifier and the exchange it is owed for, with a success reason. One out of order,
// unexpected or duplicated ends the case.
func (c *inboundAckBenchConn) readResponses() error {
	for c.completed < c.bc.perConn {
		pt, body, err := inboundAckBenchReadPacket(c.br)
		if err != nil {
			return err
		}
		now := time.Now()
		if len(body) < 2 {
			return fmt.Errorf("packet %#x without a packet identifier", pt)
		}
		pi := uint16(body[0])<<8 | uint16(body[1])
		if len(body) > 2 && body[2] != mqttReasonSuccess {
			return fmt.Errorf("packet %#x for identifier %d carries reason %#x", pt, pi, body[2])
		}
		owed, ok := c.nextOwed()
		if !ok {
			return fmt.Errorf("unexpected packet %#x for identifier %d: nothing is owed", pt, pi)
		}
		if pt&mqttPacketMask != owed.pt || pi != owed.pi {
			return fmt.Errorf("received packet %#x for identifier %d when %#x for identifier %d (exchange %d) was owed next",
				pt&mqttPacketMask, pi, owed.pt, owed.pi, owed.seq)
		}
		switch owed.pt {
		case mqttPacketPubRec:
			c.firstAck = append(c.firstAck, now.Sub(c.sent[owed.seq]))
			rel := []byte{mqttPacketPubRel | 0x2, 0x2, byte(pi >> 8), byte(pi)}
			if err := c.write(rel, inboundAckBenchExpect{pt: mqttPacketPubComp, pi: pi, seq: owed.seq}); err != nil {
				return err
			}
			continue
		case mqttPacketPubAck:
			c.firstAck = append(c.firstAck, now.Sub(c.sent[owed.seq]))
		case mqttPacketPubComp:
			c.completion = append(c.completion, now.Sub(c.sent[owed.seq]))
		}
		c.completed++
		c.lastDone = now
		<-c.sem
	}
	return nil
}

func inboundAckBenchPublish(qos byte, topic string, pi uint16, payload []byte) []byte {
	w := newMQTTWriter(0)
	w.WriteByte(mqttPacketPub | qos<<1)
	w.WriteVarInt(2 + len(topic) + 2 + 1 + len(payload))
	w.WriteString(topic)
	w.WriteUint16(pi)
	w.WriteByte(0) // no properties
	w.Write(payload)
	return w.Bytes()
}

func inboundAckBenchReadPacket(br *bufio.Reader) (byte, []byte, error) {
	pt, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, mul := 0, 1
	for i := 0; ; i++ {
		b, err := br.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		n += int(b&0x7f) * mul
		if b&0x80 == 0 {
			break
		}
		if i == 3 {
			return 0, nil, errors.New("malformed remaining length")
		}
		mul *= 128
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(br, body); err != nil {
		return 0, nil, err
	}
	return pt, body, nil
}

// inboundAckBenchStored is one record the read-back found: its stream sequence and payload.
type inboundAckBenchStored struct {
	streamSeq uint64
	data      []byte
}

// inboundAckBenchReadBack drains the interest consumer, acknowledging each delivery and
// waiting for the acknowledgement to be processed, until a fetch comes back empty AND the
// consumer reports nothing pending and nothing awaiting acknowledgement, within a deadline.
// A stream sequence seen twice is the transport redelivering one record, not a second
// record, and is dropped here; two records with one payload are left for the oracle to find.
func inboundAckBenchReadBack(t *testing.T, js nats.JetStreamContext, filter string) []inboundAckBenchStored {
	t.Helper()
	sub, err := js.PullSubscribe(filter, inboundAckBenchConsumer, nats.Bind(mqttStreamName, inboundAckBenchConsumer))
	if err != nil {
		t.Fatalf("bind interest consumer: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	var got []inboundAckBenchStored
	seen := make(map[uint64]bool)
	// Well inside the consumer's AckWait, so no delivery is redelivered within it.
	deadline := time.Now().Add(60 * time.Second)
	for {
		if time.Now().After(deadline) {
			ci, _ := sub.ConsumerInfo()
			if ci != nil {
				t.Fatalf("read-back incomplete after 60s: %d records read, %d pending, %d awaiting acknowledgement",
					len(got), ci.NumPending, ci.NumAckPending)
			}
			t.Fatalf("read-back incomplete after 60s: %d records read", len(got))
		}
		msgs, err := sub.Fetch(1000, nats.MaxWait(2*time.Second))
		if err != nil && !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("fetch from interest consumer after %d records: %v", len(got), err)
		}
		for _, m := range msgs {
			md, err := m.Metadata()
			if err != nil {
				t.Fatalf("delivery metadata: %v", err)
			}
			if err := m.AckSync(); err != nil {
				t.Fatalf("ack: %v", err)
			}
			if seen[md.Sequence.Stream] {
				continue
			}
			seen[md.Sequence.Stream] = true
			got = append(got, inboundAckBenchStored{streamSeq: md.Sequence.Stream, data: append([]byte(nil), m.Data...)})
		}
		if len(msgs) > 0 {
			continue
		}
		ci, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("consumer info: %v", err)
		}
		if ci.NumPending == 0 && ci.NumAckPending == 0 {
			sort.Slice(got, func(i, j int) bool { return got[i].streamSeq < got[j].streamSeq })
			return got
		}
	}
}

// inboundAckBenchIdentity is the "<conn>:<seq>" a stored payload carries.
func inboundAckBenchIdentity(data []byte) (conn string, seq int, ok bool) {
	parts := bytes.SplitN(data, []byte{':'}, 3)
	if len(parts) != 3 {
		return _EMPTY_, 0, false
	}
	n, err := strconv.Atoi(string(parts[1]))
	return string(parts[0]), n, err == nil
}

// The oracle is four predicates, each its own function so that each can be broken alone and
// shown to matter (TestMQTTInboundAckBenchOracleDiscriminates).

// inboundAckBenchCount: exactly the acknowledged number of records was stored.
func inboundAckBenchCount(acked int, stored []inboundAckBenchStored) error {
	if len(stored) != acked {
		return fmt.Errorf("stored %d records, acknowledged %d", len(stored), acked)
	}
	return nil
}

// inboundAckBenchUnique: no payload identity was stored as two records.
func inboundAckBenchUnique(stored []inboundAckBenchStored) error {
	at := make(map[string]uint64, len(stored))
	for _, s := range stored {
		conn, seq, ok := inboundAckBenchIdentity(s.data)
		if !ok {
			return fmt.Errorf("unidentifiable stored payload at stream sequence %d", s.streamSeq)
		}
		id := conn + ":" + strconv.Itoa(seq)
		if prev, dup := at[id]; dup {
			return fmt.Errorf("%s stored twice, at stream sequences %d and %d", id, prev, s.streamSeq)
		}
		at[id] = s.streamSeq
	}
	return nil
}

// inboundAckBenchOrder: each connection's records are in publish order in the stream.
func inboundAckBenchOrder(stored []inboundAckBenchStored) error {
	last := make(map[string]int)
	for _, s := range stored {
		conn, seq, ok := inboundAckBenchIdentity(s.data)
		if !ok {
			return fmt.Errorf("unidentifiable stored payload at stream sequence %d", s.streamSeq)
		}
		if prev, seen := last[conn]; seen && seq <= prev {
			return fmt.Errorf("%s:%d stored after %s:%d", conn, seq, conn, prev)
		}
		last[conn] = seq
	}
	return nil
}

// inboundAckBenchComplete: every acknowledged message is stored with exactly the bytes sent.
func inboundAckBenchComplete(sent map[string][]byte, stored []inboundAckBenchStored) error {
	byID := make(map[string][]byte, len(stored))
	for _, s := range stored {
		if conn, seq, ok := inboundAckBenchIdentity(s.data); ok {
			byID[conn+":"+strconv.Itoa(seq)] = s.data
		}
	}
	for id, want := range sent {
		got, ok := byID[id]
		if !ok {
			return fmt.Errorf("%s acknowledged but not stored", id)
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("%s stored with a different payload", id)
		}
	}
	return nil
}

func inboundAckBenchOracle(acked int, sent map[string][]byte, stored []inboundAckBenchStored) error {
	return errors.Join(
		inboundAckBenchCount(acked, stored),
		inboundAckBenchUnique(stored),
		inboundAckBenchOrder(stored),
		inboundAckBenchComplete(sent, stored),
	)
}

func inboundAckBenchRSS() int64 {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "VmRSS:" {
			n, _ := strconv.ParseInt(f[1], 10, 64)
			return n
		}
	}
	return -1
}

func inboundAckBenchPercentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(p*float64(len(s)-1))]
}

func inboundAckBenchReport(t *testing.T, rig string, bc inboundAckBenchCase, res inboundAckBenchResult) {
	t.Helper()
	rate := float64(res.completed) / res.elapsed.Seconds()
	row := []string{
		os.Getenv(inboundAckBenchLabelEnv), rig, strconv.Itoa(int(bc.qos)), strconv.Itoa(bc.conns),
		strconv.Itoa(bc.window), strconv.Itoa(res.completed), strconv.FormatFloat(rate, 'f', 0, 64),
		inboundAckBenchPercentile(res.firstAck, 0.5).String(), inboundAckBenchPercentile(res.firstAck, 0.99).String(),
		inboundAckBenchPercentile(res.completion, 0.5).String(), inboundAckBenchPercentile(res.completion, 0.99).String(),
		strconv.FormatFloat(res.msgsPerExchg, 'f', 2, 64), strconv.FormatInt(res.rssKB, 10), strconv.Itoa(res.goroutines),
	}
	t.Logf("label=%s rig=%s qos=%s conns=%s window=%s exchanges=%s rate=%s/s ack_p50=%s ack_p99=%s comp_p50=%s comp_p99=%s js_msgs_per_exchange=%s rss_kb=%s goroutines=%s",
		row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13])
	if out := os.Getenv(inboundAckBenchOutEnv); out != _EMPTY_ {
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("bench output: %v", err)
		}
		defer f.Close()
		fmt.Fprintln(f, strings.Join(row, ","))
	}
	classes := make([]string, 0, len(res.census))
	for k := range res.census {
		classes = append(classes, k)
	}
	sort.Strings(classes)
	var census []string
	for _, k := range classes {
		perExchange := float64(res.census[k]) / float64(res.completed)
		t.Logf("census %s: %d (%.3f per exchange)", k, res.census[k], perExchange)
		census = append(census, strings.Join([]string{row[0], rig, bc.name(), k,
			strconv.FormatInt(res.census[k], 10), strconv.FormatFloat(perExchange, 'f', 4, 64)}, ","))
	}
	if out := os.Getenv(inboundAckBenchOutEnv); out != _EMPTY_ {
		f, err := os.OpenFile(out+".census", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("bench census output: %v", err)
		}
		defer f.Close()
		fmt.Fprintln(f, strings.Join(census, "\n"))
	}
}

// Each oracle predicate must reject the failure it exists for, called on its own, so that
// breaking any one of them is seen here rather than hidden behind another predicate that
// happens to reject the same fixture.
func TestMQTTInboundAckBenchOracleDiscriminates(t *testing.T) {
	rec := func(seq uint64, conn string, n int) inboundAckBenchStored {
		return inboundAckBenchStored{streamSeq: seq, data: inboundAckBenchPayloadFor(conn, n)}
	}
	sent := map[string][]byte{}
	for _, conn := range []string{"a", "b"} {
		for n := 0; n < 2; n++ {
			sent[conn+":"+strconv.Itoa(n)] = inboundAckBenchPayloadFor(conn, n)
		}
	}
	complete := []inboundAckBenchStored{rec(1, "a", 0), rec(2, "b", 0), rec(3, "a", 1), rec(4, "b", 1)}
	if err := inboundAckBenchOracle(4, sent, complete); err != nil {
		t.Fatalf("a complete, ordered read-back was rejected: %v", err)
	}

	if err := inboundAckBenchCount(4, complete[:3]); err == nil {
		t.Error("count: a missing record was accepted")
	}
	if err := inboundAckBenchCount(4, append(complete[:4:4], rec(5, "a", 2))); err == nil {
		t.Error("count: an extra record was accepted")
	}
	if err := inboundAckBenchUnique(append(complete[:4:4], rec(5, "a", 1))); err == nil {
		t.Error("unique: one message stored as two records was accepted")
	}
	if err := inboundAckBenchOrder([]inboundAckBenchStored{rec(1, "a", 1), rec(2, "b", 0), rec(3, "a", 0), rec(4, "b", 1)}); err == nil {
		t.Error("order: a connection's messages stored out of publish order were accepted")
	}
	if err := inboundAckBenchComplete(sent, complete[:3]); err == nil {
		t.Error("complete: an acknowledged message that was never stored was accepted")
	}
	altered := rec(4, "b", 1)
	altered.data[len(altered.data)-1] = '#'
	if err := inboundAckBenchComplete(sent, append(complete[:3:3], altered)); err == nil {
		t.Error("complete: a stored payload differing from the one sent was accepted")
	}
}

// The ledger must reject a broker that acknowledges more than it was asked to, including a
// duplicate of the very last response, which arrives after the reader has every exchange it
// was counting.
func TestMQTTInboundAckBenchLedgerRejectsExtraResponses(t *testing.T) {
	// The fake broker answers each QoS 1 PUBLISH with a PUBACK, sending the one for dupPI
	// twice, and each PINGREQ with a PINGRESP, followed by a stray PUBACK when lateDup is set.
	// With swap it answers identifiers 3 and 4 in the wrong order: the same responses, the
	// same count, out of order.
	run := func(dupPI uint16, lateDup, swap bool) (runErr, quietErr error) {
		// Loopback TCP rather than net.Pipe: a reader that stops on a defect must not
		// leave the other side blocked in an unbuffered write.
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		defer ln.Close()
		client, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer client.Close()
		broker, err := ln.Accept()
		if err != nil {
			t.Fatalf("accept: %v", err)
		}
		defer broker.Close()
		go func() {
			var held []byte
			br := bufio.NewReader(broker)
			for {
				pt, body, err := inboundAckBenchReadPacket(br)
				if err != nil {
					return
				}
				switch pt & mqttPacketMask {
				case mqttPacketPub:
					tlen := int(body[0])<<8 | int(body[1])
					pi := body[2+tlen : 4+tlen]
					ack := []byte{mqttPacketPubAck, 2, pi[0], pi[1]}
					if swap && pi[1] == 3 && pi[0] == 0 {
						held = ack
						continue
					}
					if swap && pi[1] == 4 && pi[0] == 0 {
						ack = append(ack, held...)
					}
					if _, err := broker.Write(ack); err != nil {
						return
					}
					if uint16(pi[0])<<8|uint16(pi[1]) == dupPI {
						if _, err := broker.Write(ack); err != nil {
							return
						}
					}
				case mqttPacketPing:
					if _, err := broker.Write([]byte{mqttPacketPingResp, 0}); err != nil {
						return
					}
					if lateDup {
						if _, err := broker.Write([]byte{mqttPacketPubAck, 2, 0, 10}); err != nil {
							return
						}
					}
				}
			}
		}()
		bc := inboundAckBenchCase{qos: 1, conns: 1, window: 4, perConn: 10}
		c := &inboundAckBenchConn{bc: bc, nc: client, br: bufio.NewReader(client), conn: "fixture", topic: "fixture",
			sem: make(chan struct{}, bc.window), sent: make([]time.Time, bc.perConn)}
		runErr = c.run()
		if runErr == nil {
			quietErr = c.quiet()
		}
		return runErr, quietErr
	}
	if runErr, quietErr := run(0, false, false); runErr != nil || quietErr != nil {
		t.Fatalf("a broker owing nothing more was rejected: %v, %v", runErr, quietErr)
	}
	if runErr, _ := run(5, false, false); runErr == nil {
		t.Error("a duplicated acknowledgement mid-run was accepted")
	}
	// Each of the next three is caught by one check only, so no check hides another.
	if runErr, _ := run(0, false, true); runErr == nil {
		t.Error("ledger: acknowledgements out of order were accepted")
	}
	if runErr, _ := run(10, false, false); runErr == nil {
		t.Error("fence: a duplicate of the last acknowledgement, before the PINGRESP, was accepted")
	}
	if runErr, quietErr := run(0, true, false); runErr != nil || quietErr == nil {
		t.Errorf("quiet: an acknowledgement after the PINGRESP was not caught (run: %v, quiet: %v)", runErr, quietErr)
	}
}
