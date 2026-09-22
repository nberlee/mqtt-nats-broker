package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pmqtt "github.com/eclipse/paho.mqtt.golang"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startDenyReservedBroker runs a broker whose MQTT clients authenticate as a
// NATS user that is denied publish AND subscribe on the whole reserved
// namespace -- the ACL docs/ARCHITECTURE.md recommends for production. The
// service user keeps full rights. Returns the MQTT broker URL and the NATS URL
// so a test can also connect to NATS directly with the client credentials.
func startDenyReservedBroker(t *testing.T) (broker, natsURL string) {
	t.Helper()
	deny := []string{mqttPrefix + ">"}
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		Users: []*natsserver.User{
			{Username: "svc", Password: "svcpw"},
			{Username: "alice", Password: "pw", Permissions: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: deny},
				Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}, Deny: deny},
			}},
		},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	natsURL = srv.ClientURL()

	svc, err := nats.Connect(natsURL, nats.UserInfo("svc", "svcpw"))
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(svc.Close)

	s := NewServer(svc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, "deny-node")
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		return nats.Connect(natsURL, nats.UserInfo(user, pass))
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	return fmt.Sprintf("tcp://%s", s.Addr().String()), natsURL
}

// TestReservedDenyACLIsDeployable is the operational half of the namespace
// policy. Denying subscribe on "$MQTT.>" for the client identity must not break
// the broker, which is only true because the broker creates its own delivery
// subscriptions ($MQTT.sub.*, $MQTT.deliver.pubrel.*) on the service connection
// rather than on the per-client one. If they were still on the client
// connection, NATS would deny them -- deny is evaluated before allow, so they
// could not be carved out -- and QoS 2 delivery would silently stop.
func TestReservedDenyACLIsDeployable(t *testing.T) {
	t.Parallel()
	broker, _ := startDenyReservedBroker(t)

	got := make(chan pmqtt.Message, 4)
	sub := authClient(t, broker, "deny-sub")
	defer sub.Disconnect(100)
	// QoS 2 exercises both the deliver subject and the PUBREL delivery
	// subscription -- the two internal subjects on the critical path.
	if tok := sub.Subscribe("foo/bar", 2, func(_ pmqtt.Client, m pmqtt.Message) {
		got <- m
	}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pub := authClient(t, broker, "deny-pub")
	defer pub.Disconnect(100)
	if tok := pub.Publish("foo/bar", 2, false, []byte("hello")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}

	select {
	case m := <-got:
		if string(m.Payload()) != "hello" {
			t.Errorf("payload = %q, want hello", m.Payload())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("QoS 2 message never delivered under a $MQTT.> subscribe deny; an internal subscription is still on the per-client connection")
	}

	// Retained replay uses the same machinery on resubscribe.
	if tok := pub.Publish("foo/retained", 1, true, []byte("kept")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("retained publish: %v", tok.Error())
	}
	replay := make(chan pmqtt.Message, 4)
	if tok := sub.Subscribe("foo/retained", 1, func(_ pmqtt.Client, m pmqtt.Message) {
		replay <- m
	}); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("retained subscribe: %v", tok.Error())
	}
	select {
	case m := <-replay:
		if string(m.Payload()) != "kept" {
			t.Errorf("retained payload = %q, want kept", m.Payload())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("retained message never replayed under a $MQTT.> subscribe deny")
	}
}

// v5AuthClient connects an authenticated MQTT 5.0 client. Shared subscriptions
// are v5-only, and paho.mqtt.golang is 3.1.1-only, so the shared case below
// needs paho.golang with credentials.
func v5AuthClient(t *testing.T, brokerTCP, id string, onPub func(paho.PublishReceived) (bool, error)) *paho.Client {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(brokerTCP, "tcp://"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cfg := paho.ClientConfig{Conn: conn}
	if onPub != nil {
		cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){onPub}
	}
	c := paho.NewClient(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca, err := c.Connect(ctx, &paho.Connect{
		ClientID: id, KeepAlive: 30, CleanStart: true,
		Username: "alice", Password: []byte("pw"), UsernameFlag: true, PasswordFlag: true,
	})
	if err != nil {
		t.Fatalf("v5 connect %s: %v", id, err)
	}
	if ca.ReasonCode != 0 {
		t.Fatalf("v5 CONNACK reason = %d, want 0", ca.ReasonCode)
	}
	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return c
}

// TestReservedDenyACLSharedSubscription covers the third moved subscription
// class. A shared subscription delivers over $MQTT.qsub.<key>, which is
// JetStream-backed and distinct from the plain deliver subject and the PUBREL
// delivery subject exercised above. It is published here at QoS 1 on purpose:
// the Paho suite's shared-subscription test publishes at QoS 0, which never
// touches the $MQTT.qsub.* delivery path at all.
func TestReservedDenyACLSharedSubscription(t *testing.T) {
	t.Parallel()
	broker, _ := startDenyReservedBroker(t)

	got := make(chan string, 8)
	recv := func(pr paho.PublishReceived) (bool, error) {
		got <- string(pr.Packet.Payload)
		return true, nil
	}
	sub := v5AuthClient(t, broker, "deny-shared-sub", recv)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := sub.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "$share/g1/deny/shared", QoS: 1}},
	}); err != nil {
		t.Fatalf("shared subscribe: %v", err)
	}

	pub := v5AuthClient(t, broker, "deny-shared-pub", nil)
	if _, err := pub.Publish(ctx, &paho.Publish{Topic: "deny/shared", QoS: 1, Payload: []byte("shared-hello")}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case p := <-got:
		if p != "shared-hello" {
			t.Errorf("payload = %q, want shared-hello", p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shared subscription delivered nothing under a $MQTT.> subscribe deny; the $MQTT.qsub.* delivery subscription is still on the per-client connection")
	}
}

// TestReservedDenyACLBindsDirectNATSClients is the other half, and the half the
// broker's own checks cannot cover. Under auth passthrough an MQTT client holds
// real NATS credentials, so it can skip MQTT entirely and subscribe directly.
// Only the account ACL stops it -- and that ACL is only deployable because of
// the separation the test above proves.
func TestReservedDenyACLBindsDirectNATSClients(t *testing.T) {
	t.Parallel()
	_, natsURL := startDenyReservedBroker(t)

	nc, err := nats.Connect(natsURL, nats.UserInfo("alice", "pw"))
	if err != nil {
		t.Fatalf("direct connect: %v", err)
	}
	defer nc.Close()

	// $MQTT.sub.> is the JetStream push-consumer deliver subject: reading it
	// would yield every QoS 1/2 delivery in the account. $MQTT.accept.> is the
	// acceptance-fact space, which is permanent and never reaped, so reading it
	// yields every client's whole connection history rather than a live window.
	for _, subject := range []string{
		mqttSubPrefix + ">",
		mqttStreamSubjectPrefix + ">",
		mqttSessStreamSubjectPrefix + ">",
		mqttAcceptSubjectPrefix + ">",
		mqttHandoffSubjectPrefix + ">",
		mqttTeardownReceiptSubjectPrefix + ">",
	} {
		t.Run(subject, func(t *testing.T) {
			// nats.go's async Subscribe does not report the denial inline, and
			// Flush does not return it either: the server's -ERR is recorded in
			// LastError before the PONG that Flush waits for. That is the same
			// observation the broker's own subscribe path relies on.
			sub, err := nc.Subscribe(subject, func(*nats.Msg) {})
			if err != nil {
				return // denied synchronously; also fine
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if lastErr := nc.LastError(); !errors.Is(lastErr, nats.ErrPermissionViolation) {
				t.Errorf("subscribe to %q was not denied: LastError = %v", subject, lastErr)
			}
		})
	}
}
