package mqtt

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

// A JSA request is a publish with a reply subject, and nats.go always asks the server for
// no-responders notifications. A publish nothing receives is therefore answered with a
// header-only "NATS/1.0 503" on the broker's own reply subscription — the transport
// answering, not JetStream. These tests drive that answer through a real server and pin what
// the JSA makes of it: a named no-responders error, never a decode failure and never a
// confirmation.

// noRespondersASM is a session manager on a real server, optionally behind a JetStream
// domain served by serveJSDomain's forwarder, which the caller may then withdraw.
func noRespondersASM(t *testing.T, nc *nats.Conn, domain string) *mqttAccountSessionManager {
	t.Helper()
	s := &Server{
		nrc:      nc,
		opts:     &Options{MQTT: MQTTOpts{JsDomain: domain}},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}
	t.Cleanup(func() { close(s.quitCh) })
	return asm
}

// assertNoResponders requires err to be the JSA's no-responders error for subject: status
// 503, no JetStream error code, and a description that names both.
func assertNoResponders(t *testing.T, err error, subject string) {
	t.Helper()
	if err == nil {
		t.Fatalf("a request to %q that nothing receives returned no error", subject)
	}
	ae := jsAPIError(err)
	if ae == nil {
		t.Fatalf("error %v (%T) is not a JetStream API error", err, err)
	}
	if ae.Code != 503 || ae.ErrCode != 0 {
		t.Fatalf("no-responders answer surfaced as code %d / err_code %d (%q), want 503 / 0",
			ae.Code, ae.ErrCode, ae.Description)
	}
	if !strings.Contains(ae.Description, "no responders") || !strings.Contains(ae.Description, subject) {
		t.Fatalf("description %q does not name no responders and the subject %q", ae.Description, subject)
	}
}

// TestJSAStoreWithNoStreamNamesNoResponders: a store to a subject no stream binds — what a
// clustered stream's subjects look like during its leader election — is answered by the
// transport. The JSA must report it as no responders, not as JSON it failed to decode.
func TestJSAStoreWithNoStreamNamesNoResponders(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	asm := noRespondersASM(t, nc, _EMPTY_)

	const subject = "mqttnb.witness.unbound"
	_, err := asm.jsa.storeMsg(subject, 0, []byte("payload"))
	assertNoResponders(t, err, subject)
}

// TestJSAAPIRequestWithNoResponderNamesNoResponders: a JS API request under a domain nothing
// serves any longer — a leafnode-reached domain whose link is down — takes the same answer
// through an API kind, so the status is classified before the per-kind decode, not inside
// one kind's arm.
func TestJSAAPIRequestWithNoResponderNamesNoResponders(t *testing.T) {
	t.Parallel()
	const domain = "GONE"
	_, nc := runNATS(t)
	// The forwarder serves the domain while the session manager provisions its streams, and is
	// withdrawn before the request under test.
	fwd, err := nc.Subscribe("$JS."+domain+".API.>", func(m *nats.Msg) {
		out := nats.NewMsg(JSApiPrefix + "." + strings.TrimPrefix(m.Subject, "$JS."+domain+".API."))
		out.Header, out.Data = m.Header, m.Data
		resp, rerr := nc.RequestMsg(out, 5*time.Second)
		if rerr != nil {
			return
		}
		resp.Subject = m.Reply
		_ = nc.PublishMsg(resp)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	asm := noRespondersASM(t, nc, domain)
	if err := fwd.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	_, lerr := asm.jsa.lookupStream(mqttStreamName)
	assertNoResponders(t, lerr, "$JS."+domain+".API.STREAM.INFO."+mqttStreamName)
}

// TestJSAAckConfirmationOfAbsentConsumerIsNotConfirmed: a consumer's ack subscription exists
// only while it is leader, and not at all once it is deleted. A confirmed acknowledgement
// sent where no consumer leader listens is answered by the transport, and that answer must
// fail the wait — callers delete and release on a confirmation, so a 503 read as one is a
// false proof that the source is gone.
func TestJSAAckConfirmationOfAbsentConsumerIsNotConfirmed(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	asm := noRespondersASM(t, nc, _EMPTY_)

	const stream, durable = "ACKWITNESS", "ackwitness"
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{"ackwitness.>"}, Storage: jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	cons, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable: durable, AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Minute,
	})
	if err != nil {
		t.Fatalf("CreateConsumer: %v", err)
	}
	for _, subj := range []string{"ackwitness.live", "ackwitness.orphaned"} {
		if _, err := js.Publish(ctx, subj, []byte("x")); err != nil {
			t.Fatalf("Publish %s: %v", subj, err)
		}
	}
	batch, err := cons.Fetch(2, jetstream.FetchMaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	replies := map[string]string{}
	for m := range batch.Messages() {
		replies[m.Subject()] = m.Reply()
	}
	if len(replies) != 2 {
		t.Fatalf("fetched %d deliveries, want 2", len(replies))
	}

	// The control: a consumer leader listens, and its genuine confirmation — an empty reply
	// with no status — still passes.
	live := asm.jsa.prepareAckConfirm(replies["ackwitness.live"])
	live.enqueue()
	if err := live.wait(); err != nil {
		t.Fatalf("confirmed acknowledgement with the consumer present: %v", err)
	}

	if err := js.DeleteConsumer(ctx, stream, durable); err != nil {
		t.Fatalf("DeleteConsumer: %v", err)
	}
	orphaned := replies["ackwitness.orphaned"]
	p := asm.jsa.prepareAckConfirm(orphaned)
	p.enqueue()
	assertNoResponders(t, p.wait(), orphaned)
}

// TestJSANoRespondersStaysAmbiguous: the no-responders error carries no JetStream error
// code, so no classifier that matches by identity can read it as a definite answer — a
// refusal before mutation, a CAS rejection, a positive "no message", contention. It lands
// where a timeout lands. Checked against every identifier the server defines, not a list,
// and before the identity itself, so a wrong identity still meets the classifiers.
func TestJSANoRespondersStaysAmbiguous(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	asm := noRespondersASM(t, nc, _EMPTY_)

	const subject = "mqttnb.witness.unbound"
	_, err := asm.jsa.storeMsg(subject, 0, []byte("payload"))
	if err == nil {
		t.Fatalf("a store to %q that nothing receives returned no error", subject)
	}
	for name, definite := range map[string]bool{
		"mqttDeleteRefusedBeforeMutation": mqttDeleteRefusedBeforeMutation(err),
		"mqttIsRecordCASRejection":        mqttIsRecordCASRejection(err),
		"mqttStoreAnswered":               mqttStoreAnswered(err),
		"mqttExchangeReadAbsent":          mqttExchangeReadAbsent(err),
		"mqttIsMetadataContention":        mqttIsMetadataContention(err),
	} {
		if definite {
			t.Errorf("%s reads the no-responders error as a definite answer", name)
		}
	}
	for id := range ApiErrors {
		if IsNatsErr(err, id) {
			t.Errorf("the no-responders error matches server identifier %d (%+v)", id, ApiErrors[id])
		}
	}
	assertNoResponders(t, err, subject)
}

// TestJSAReplyStatusIsReadFromTheHeaderBlockOnly: what makes a reply a status is a Status
// header in the message's own header block — any status, not only 503 — and nothing else. A
// header without one is an answer, a body that merely reads like a header block is an answer,
// and a status leaves nothing behind for the next message on the same subscription. Driven
// at the dispatcher with frames built exactly as processSub builds them.
func TestJSAReplyStatusIsReadFromTheHeaderBlockOnly(t *testing.T) {
	t.Parallel()
	_, nc := runNATS(t)
	asm := noRespondersASM(t, nc, _EMPTY_)
	// One delivery client for every frame, as processSub keeps one per subscription.
	dc := &client{}

	deliver := func(kind string, m *nats.Msg, framed bool) any {
		t.Helper()
		reply := asm.jsa.rplyr + kind + "." + nuid.Next()
		ch := make(chan *mqttJSAResponse, 1)
		asm.jsa.replies.Store(reply, ch)
		var msg []byte
		if framed {
			msg, dc.pa.hdr = frameNATSMsg(m)
		} else {
			msg, dc.pa.hdr = m.Data, 0
		}
		asm.processJSAPIReplies(nil, dc, nil, reply, _EMPTY_, msg)
		select {
		case r := <-ch:
			return r.value
		default:
			t.Fatalf("no response delivered for %s", reply)
			return nil
		}
	}
	status := func(code, desc string) *nats.Msg {
		m := nats.NewMsg("ignored")
		m.Header.Set("Status", code)
		if desc != _EMPTY_ {
			m.Header.Set("Description", desc)
		}
		m.Header.Set("Nats-Subject", "$JS.ACK.S.C.1.1.1.1.0")
		return m
	}
	ackErr := func(v any) error {
		if v == nil {
			return nil
		}
		err, ok := v.(error)
		if !ok {
			t.Fatalf("acknowledgement confirmation delivered %T, want nil or an error", v)
		}
		return err
	}

	t.Run("any status is not a confirmation", func(t *testing.T) {
		err := ackErr(deliver(mqttJSAAckConfirm, status("408", "Request Timeout"), true))
		ae := jsAPIError(err)
		if ae == nil || ae.Code != 408 || ae.ErrCode != 0 {
			t.Fatalf("a 408 status on a confirmation surfaced as %v, want code 408 / err_code 0", err)
		}
	})
	t.Run("a header without a status is a confirmation", func(t *testing.T) {
		m := nats.NewMsg("ignored")
		m.Header.Set("Nats-Subject", "$JS.ACK.S.C.1.1.1.1.0")
		if err := ackErr(deliver(mqttJSAAckConfirm, m, true)); err != nil {
			t.Fatalf("a header with no Status failed the confirmation: %v", err)
		}
	})
	t.Run("a body is never a header block", func(t *testing.T) {
		m := nats.NewMsg("ignored")
		m.Data = []byte("NATS/1.0 503\r\nStatus: 503\r\n\r\n")
		if err := ackErr(deliver(mqttJSAAckConfirm, m, false)); err != nil {
			t.Fatalf("a body that reads like a status failed the confirmation: %v", err)
		}
		m.Header.Set("Nats-Subject", "$JS.ACK.S.C.1.1.1.1.0")
		if err := ackErr(deliver(mqttJSAAckConfirm, m, true)); err != nil {
			t.Fatalf("a status-free header block before such a body failed the confirmation: %v", err)
		}
	})
	t.Run("a status leaves nothing for the next reply", func(t *testing.T) {
		if err := ackErr(deliver(mqttJSAAckConfirm, status("503", _EMPTY_), true)); err == nil {
			t.Fatal("a 503 status confirmed the acknowledgement")
		}
		// The confirmation carries a status-free header block, so it is parsed: a status left
		// over from the previous reply would be found here.
		m := nats.NewMsg("ignored")
		m.Header.Set("Nats-Subject", "$JS.ACK.S.C.1.1.1.1.0")
		if err := ackErr(deliver(mqttJSAAckConfirm, m, true)); err != nil {
			t.Fatalf("the genuine confirmation after a status failed: %v", err)
		}
	})
	t.Run("a JSON kind's status is not a decode failure", func(t *testing.T) {
		v := deliver(mqttJSAStreamLookup, status("408", "Request Timeout"), true)
		resp, ok := v.(*JSApiStreamInfoResponse)
		if !ok {
			t.Fatalf("stream lookup delivered %T", v)
		}
		if resp.Error == nil || resp.Error.Code != 408 || resp.Error.ErrCode != 0 {
			t.Fatalf("a 408 status on a stream lookup surfaced as %+v, want code 408 / err_code 0", resp.Error)
		}
	})
}
