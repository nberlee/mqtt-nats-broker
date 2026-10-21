// These fixtures are the evidence for a PROPERTY, not a definition of it: the
// property is in docs/ARCHITECTURE.md and plan §11, and a route absent from
// this file is not thereby permitted. Every arm carries an ALLOWED control on
// the same mechanism, because a non-delivery assertion passes trivially against
// a broker that delivers nothing.

package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pmqtt "github.com/eclipse/paho.mqtt.golang"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// authzConfOneAccount is the ordinary shape: one JetStream account, a
// full-rights service user, and "alice" with ordinary-topic denies. No system
// account is declared, deliberately - nats-server supplies a default $SYS, and
// this fixture is what proves the identity request needs no config change.
const authzConfOneAccount = `
port: -1
host: "127.0.0.1"
jetstream: enabled
accounts {
  APP: {
    jetstream: enabled
    users: [
      { user: svc, password: svcpw, permissions: { publish: ">", subscribe: ">" } }
      { user: alice, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$MQTT.>", "denied.>", "$sparkplug.certificates.spBv1//0.grp.NBIRTH.>"] }
          subscribe: { allow: [">"], deny: ["foo.secret"] }
      } }
    ]
  }
}
`

// authzConfTwoAccounts gives "mallory" credentials in an account the bridge's
// streams do not live in - F-080's whole subject.
const authzConfTwoAccounts = `
port: -1
host: "127.0.0.1"
jetstream: enabled
accounts {
  APP: {
    jetstream: enabled
    users: [
      { user: svc, password: svcpw }
      { user: alice, password: pw }
    ]
  }
  OTHER: {
    users: [ { user: mallory, password: pw } ]
  }
}
`

// authzConfNoSysAccess denies alice the identity request itself, which is the
// one deployment shape that cannot bootstrap. Deny beats allow in NATS, so this
// cannot be carved back.
const authzConfNoSysAccess = `
port: -1
host: "127.0.0.1"
jetstream: enabled
accounts {
  APP: {
    jetstream: enabled
    users: [
      { user: svc, password: svcpw, permissions: { publish: ">", subscribe: ">" } }
      { user: alice, password: pw, permissions: {
          publish:   { allow: [">"], deny: ["$SYS.>", "$MQTT.>"] }
          subscribe: { allow: [">"] }
      } }
    ]
  }
}
`

func authzClient(t *testing.T, broker, id, user string, proto uint) (pmqtt.Client, error) {
	t.Helper()
	// Auto-reconnect OFF, deliberately. Several fixtures here revoke a client's
	// identity and then assert an absence; with reconnect on, paho silently opens a
	// NEW connection -- which gets a FRESH, valid snapshot -- and retries the
	// publish, so the absence never materialises and the test fails for a reason
	// that is the client library's behaviour rather than the broker's.
	o := pmqtt.NewClientOptions().AddBroker(broker).SetClientID(id).
		SetUsername(user).SetPassword("pw").SetCleanSession(true).
		SetProtocolVersion(proto).SetAutoReconnect(false).
		SetConnectTimeout(8 * time.Second)
	c := pmqtt.NewClient(o)
	tok := c.Connect()
	if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		return nil, fmt.Errorf("connect %s: %w", id, tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c, nil
}

// storedOnStream reports whether a message exists on subject in streamName.
//
// The assertions below need it because "the subscriber saw nothing" is only half
// of what F-079 is about: the other half is data at rest. A broker whose delivery
// broke while its store still ran would satisfy a delivery-only assertion.
func storedOnStream(t *testing.T, s *Server, streamName, subject string) bool {
	t.Helper()
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}
	sm, lerr := asm.jsa.loadLastMsgFor(streamName, subject)
	if lerr != nil {
		// Only a CONFIRMED not-found counts as absence. Treating every error as
		// "no message" makes each negative assertion below pass at the boolean zero
		// value whenever the lookup times out or loses its responder -- an absence
		// the test never actually observed, which a positive control on a different
		// subject cannot rule out.
		if isJSNotFound(lerr) {
			return false
		}
		t.Fatalf("looking up %q on %s: %v (this is an operational failure, not an absence)", subject, streamName, lerr)
	}
	return sm != nil
}

// isJSNotFound reports whether err is JetStream's "no message found" answer, as
// opposed to a failure to get an answer at all.
func isJSNotFound(err error) bool {
	var apiErr *ApiError
	if errors.As(err, &apiErr) {
		return apiErr.ErrCode == uint16(JSNoMessageFoundErr)
	}
	return strings.Contains(strings.ToLower(err.Error()), "no message found")
}

// authzBroker is startAuthzBroker plus the *Server, for tests that read streams.
func authzBroker(t *testing.T, conf string) (broker string, s *Server) {
	t.Helper()
	path := t.TempDir() + "/authz.conf"
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	opts.NoSigs = true
	opts.StoreDir = t.TempDir()
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	url := srv.ClientURL()
	svc, err := nats.Connect(url, nats.UserInfo("svc", "svcpw"))
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(svc.Close)
	s = NewServer(svc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, "authz-node")
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		return nats.Connect(url, nats.UserInfo(user, pass))
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	return fmt.Sprintf("tcp://%s", s.Addr().String()), s
}

// TestDeniedPublishQoS1NotStoredOrDelivered is F-079's QoS 1 row.
//
// The measured defect was not merely that a denied message was delivered: it was
// that the publisher was told PUBACK, i.e. success. So this asserts three
// separate things, because each is satisfiable while the others fail.
func TestDeniedPublishQoS1NotStoredOrDelivered(t *testing.T) {
	t.Parallel()
	broker, s := authzBroker(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 8)
	sub, err := authzClient(t, broker, "q1-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	if tok := sub.Subscribe("#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pub, err := authzClient(t, broker, "q1-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}

	// CONTROL FIRST. Every assertion below is a negative, and a negative passes
	// against a broker that does nothing at all -- so the mechanism is shown to
	// work before its absence is read as enforcement.
	if tok := pub.Publish("allowed/q1", 1, false, []byte("yes")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control publish: %v", tok.Error())
	}
	select {
	case m := <-got:
		if string(m.Payload()) != "yes" {
			t.Fatalf("control: got %q", m.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("control: an ALLOWED QoS 1 publish must be delivered, or this test proves nothing")
	}
	// The control is the DELIVERY above, not a stream read. $MQTT_msgs is
	// interest-retained, so a QoS 1 message the subscriber has acknowledged is
	// legitimately gone from it -- asserting its presence would race the reaper and
	// fail for a reason that has nothing to do with authorization. Delivery at QoS 1
	// cannot happen without the store, so it is the sound positive.

	// THE DEFECT.
	if tok := pub.Publish("denied/q1", 1, false, []byte("no")); !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("denied publish: the client's send did not settle")
	}
	select {
	case m := <-got:
		t.Fatalf("a denied QoS 1 publish must not be delivered, got %q on %q", m.Payload(), m.Topic())
	case <-time.After(1500 * time.Millisecond):
	}
	// The data-at-rest half, corroborating the non-delivery above: a broker whose
	// delivery broke while its store still ran would satisfy non-delivery alone.
	if storedOnStream(t, s, mqttStreamName, mqttStreamSubjectPrefix+"denied.q1") {
		t.Fatal("a denied QoS 1 publish must not be stored in $MQTT_msgs")
	}
}

// TestDeniedRetainedPublishNotPersisted is F-079's retained row, and it is the
// one QoS 0 case that still matters: the live fan-out rides the client's own
// connection and NATS already drops it, so the retained STORE on the service
// connection is the entire observable.
func TestDeniedRetainedPublishNotPersisted(t *testing.T) {
	t.Parallel()
	broker, s := authzBroker(t, authzConfOneAccount)

	pub, err := authzClient(t, broker, "ret-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	if tok := pub.Publish("allowed/r", 0, true, []byte("keep")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control retained publish: %v", tok.Error())
	}
	if tok := pub.Publish("denied/r", 0, true, []byte("drop")); !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("denied retained publish did not settle")
	}
	// The retained store is asynchronous with respect to the publish, so the
	// control is what establishes the write has had time to happen; asserting the
	// denied absence without it would race the store and pass for the wrong reason.
	deadline := time.Now().Add(5 * time.Second)
	for !storedOnStream(t, s, mqttRetainedMsgsStreamName, mqttRetainedMsgsStreamSubject+"allowed.r") {
		if time.Now().After(deadline) {
			t.Fatal("control: an ALLOWED retained publish must be persisted")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if storedOnStream(t, s, mqttRetainedMsgsStreamName, mqttRetainedMsgsStreamSubject+"denied.r") {
		t.Fatal("a denied retained publish must not be persisted in $MQTT_rmsgs")
	}

	// And a later subscriber must not receive it, which is what retention is FOR
	// and is a different failure from the store having happened.
	got := make(chan pmqtt.Message, 4)
	sub, err := authzClient(t, broker, "ret-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	if tok := sub.Subscribe("denied/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	select {
	case m := <-got:
		t.Fatalf("a denied retained message must not be replayed, got %q", m.Payload())
	case <-time.After(1500 * time.Millisecond):
	}
}

// TestCrossAccountCredentialsRefused is F-080.
//
// Measured before the fix: "mallory", whose credentials live in account OTHER,
// authenticated against a broker whose streams live in APP, and its QoS 1 publish
// landed in APP's $MQTT_msgs.
func TestCrossAccountCredentialsRefused(t *testing.T) {
	t.Parallel()
	broker, _ := authzBroker(t, authzConfTwoAccounts)

	// CONTROL: the same-account identity is accepted, so a refusal below is about
	// the account and not about the broker being broken.
	if _, err := authzClient(t, broker, "same-acct", "alice", 4); err != nil {
		t.Fatalf("an identity in the service account must be accepted: %v", err)
	}
	if _, err := authzClient(t, broker, "cross-acct", "mallory", 4); err == nil {
		t.Fatal("credentials bound to a different NATS account must not authenticate: " +
			"every durable operation would land in the service account's streams")
	}
}

// TestUserInfoUnavailableRefusesConnect is the deployment shape that cannot
// bootstrap: the identity request itself is denied, so the broker cannot learn
// who the client is. It refuses, rather than proceeding on an unknown identity.
func TestUserInfoUnavailableRefusesConnect(t *testing.T) {
	t.Parallel()
	broker, _ := authzBroker(t, authzConfNoSysAccess)
	if _, err := authzClient(t, broker, "no-sys", "alice", 4); err == nil {
		t.Fatal("a client whose identity cannot be established must be refused, not admitted unpoliced")
	}
}

// TestWildcardOverlappingSubscribeDenyNotDelivered is F-081.
//
// Measured before the fix: with foo.secret denied for subscribe and the client
// subscribed to the overlapping wildcard foo/#, QoS 1, QoS 2 and retained replay
// were all delivered. QoS 0 was not, because it rides the client's own connection
// -- and that asymmetry is the control this test keeps, so it cannot pass on the
// wrong mechanism.
func TestWildcardOverlappingSubscribeDenyNotDelivered(t *testing.T) {
	t.Parallel()
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
			t.Parallel()
			broker, srv := authzBroker(t, authzConfOneAccount)
			got := make(chan pmqtt.Message, 8)
			sub, err := authzClient(t, broker, fmt.Sprintf("ovl-sub-%d", qos), "alice", 4)
			if err != nil {
				t.Fatalf("subscriber: %v", err)
			}
			if tok := sub.Subscribe("foo/#", qos, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
				t.Fatalf("subscribe: %v", tok.Error())
			}
			pub, err := authzClient(t, broker, fmt.Sprintf("ovl-pub-%d", qos), "alice", 4)
			if err != nil {
				t.Fatalf("publisher: %v", err)
			}

			// CONTROL: an allowed sibling under the SAME wildcard is delivered, so a
			// non-delivery below is the deny and not a broken subscription.
			if tok := pub.Publish("foo/public", qos, false, []byte("ok")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
				t.Fatalf("control publish: %v", tok.Error())
			}
			select {
			case m := <-got:
				if m.Topic() != "foo/public" {
					t.Fatalf("control: got %q", m.Topic())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("control: an allowed sibling must be delivered through the same wildcard")
			}

			if tok := pub.Publish("foo/secret", qos, false, []byte("leak")); !tok.WaitTimeout(5 * time.Second) {
				t.Fatal("publish to the denied subject did not settle")
			}
			select {
			case m := <-got:
				t.Fatalf("a subject the account denies for SUBSCRIBE must not be delivered at QoS %d, got %q on %q",
					qos, m.Payload(), m.Topic())
			case <-time.After(2 * time.Second):
			}

			// Non-delivery is only half of it. A deny on a PLAIN subscription is a
			// permanent fact about the only consumer that will ever be offered this
			// message, so the guard must dispose of it TERMINALLY. Handing it back --
			// which is the right disposition for a shared group, where another member
			// may be allowed it -- would redeliver against MaxDeliver of -1 forever,
			// spinning the guard for the life of the session. Nothing is delivered
			// either way, so only the consumer's own accounting tells the two apart.
			assertNothingHeldForRedelivery(t, srv)
		})
	}
}

// assertNothingHeldForRedelivery fails unless every consumer on the message
// stream has settled: nothing awaiting an acknowledgement and nothing left to
// deliver. It is the observable that distinguishes "acknowledged" from
// "redelivering forever", which no delivery assertion can see.
func assertNothingHeldForRedelivery(t *testing.T, s *Server) {
	t.Helper()
	js, err := s.nrc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	var last string
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); time.Sleep(150 * time.Millisecond) {
		last = ""
		for ci := range js.ConsumersInfo(mqttStreamName) {
			if ci.NumAckPending > 0 || ci.NumPending > 0 {
				last = fmt.Sprintf("%s: ack_pending=%d pending=%d delivered=%d",
					ci.Name, ci.NumAckPending, ci.NumPending, ci.Delivered.Consumer)
			}
		}
		if last == "" {
			return
		}
	}
	t.Fatalf("a denied message is still held for redelivery on a plain subscription (%s): the "+
		"guard handed it back instead of disposing of it, and with MaxDeliver -1 it will do so forever", last)
}

// TestSubscribeDenyDoesNotFilterQoS0OnTheBrokerSide is the CONTROL for the test
// above, and it is deliberately an assertion about NATS rather than about this
// broker.
//
// A plain QoS 0 subscription rides the client's own connection, so NATS filters
// the denied subject and the broker's own guard never runs. Keeping this arm
// separate is what lets a mutation that removes the broker's guard be told apart
// from one that breaks delivery outright: this test must keep passing under it.
func TestSubscribeDenyDoesNotFilterQoS0OnTheBrokerSide(t *testing.T) {
	t.Parallel()
	broker, _ := authzBroker(t, authzConfOneAccount)
	got := make(chan pmqtt.Message, 4)
	sub, err := authzClient(t, broker, "q0-ctl-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	if tok := sub.Subscribe("foo/#", 0, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	pub, err := authzClient(t, broker, "q0-ctl-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	if tok := pub.Publish("foo/public", 0, false, []byte("ok")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control publish: %v", tok.Error())
	}
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("control: an allowed QoS 0 message must arrive")
	}
	if tok := pub.Publish("foo/secret", 0, false, []byte("leak")); !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("publish did not settle")
	}
	select {
	case m := <-got:
		t.Fatalf("NATS itself must filter QoS 0 on a denied subject, got %q", m.Payload())
	case <-time.After(1500 * time.Millisecond):
	}
}

// withUserInfoHook installs the identity seam for one test and removes it after.
//
// Not parallel-safe, and deliberately so: the hook is process-wide, so a test
// using it must not run beside another that connects.
func withUserInfoHook(t *testing.T, fn func() ([]byte, error)) {
	t.Helper()
	mqttTestUserInfoHookV.Store(&fn)
	t.Cleanup(func() { mqttTestUserInfoHookV.Store(nil) })
}

// userInfoBody builds a $SYS.REQ.USER.INFO response envelope, so the seam feeds
// the fetch the same shape a server would and every validation still applies.
func userInfoBody(t *testing.T, ui *natsserver.UserInfo) []byte {
	t.Helper()
	b, err := json.Marshal(struct {
		Data *natsserver.UserInfo `json:"data"`
	}{Data: ui})
	if err != nil {
		t.Fatalf("marshal user info: %v", err)
	}
	return b
}

// TestIdentityShapesThatMustRefuseConnect isolates the CONNECT-time identity
// rules from the transport, which an end-to-end fixture cannot do.
//
// The cross-account row is the reason this test exists. A client whose
// credentials live in another account cannot be identified at all -- the broker
// collects identity replies in its own account, so that client's reply never
// arrives and the CONNECT is refused by timeout. An end-to-end cross-account
// fixture therefore passes with the account comparison DELETED, which a mutation
// run showed. Here the comparison is the only thing that can refuse.
func TestIdentityShapesThatMustRefuseConnect(t *testing.T) {
	broker, _ := authzBroker(t, authzConfOneAccount)
	full := &natsserver.SubjectPermission{Allow: []string{">"}}
	// The shape a usable MQTT identity must have: it cannot publish into the
	// reserved namespace, which is what makes its own identity reply unforgeable.
	safePub := &natsserver.SubjectPermission{Allow: []string{">"}, Deny: []string{mqttPrefix + ">"}}
	safe := &natsserver.Permissions{Publish: safePub, Subscribe: full}

	for _, tc := range []struct {
		name string
		body func(t *testing.T) []byte
		err  error
	}{
		{
			name: "an identity in another account",
			body: func(t *testing.T) []byte {
				return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Account: "SOMEWHERE_ELSE", Permissions: safe})
			},
		},
		{
			name: "no user named",
			body: func(t *testing.T) []byte {
				return userInfoBody(t, &natsserver.UserInfo{Account: "APP", Permissions: safe})
			},
		},
		{
			name: "no account named",
			body: func(t *testing.T) []byte {
				return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Permissions: safe})
			},
		},
		{
			name: "a permissions object missing a direction",
			body: func(t *testing.T) []byte {
				return userInfoBody(t, &natsserver.UserInfo{
					UserID: "alice", Account: "APP",
					Permissions: &natsserver.Permissions{Publish: full},
				})
			},
		},
		{
			name: "a deny entry that is not a valid filter",
			body: func(t *testing.T) []byte {
				return userInfoBody(t, &natsserver.UserInfo{
					UserID: "alice", Account: "APP",
					Permissions: &natsserver.Permissions{
						Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: []string{mqttPrefix + ">", "foo..bar"}},
						Subscribe: full,
					},
				})
			},
		},
		{
			name: "an undecodable body",
			body: func(*testing.T) []byte { return []byte("not json") },
		},
		{
			name: "an identity whose lifetime has already elapsed",
			body: func(t *testing.T) []byte {
				// NEGATIVE remaining lifetime: the server already considers this
				// identity expired. Zero is NOT this case and must stay acceptable,
				// which the control below covers - nats-server truncates the
				// remainder to seconds, so zero also means "under a second left".
				return userInfoBody(t, &natsserver.UserInfo{
					UserID: "alice", Account: "APP", Permissions: safe, Expires: -time.Second,
				})
			},
		},
		{
			name: "the request itself failed",
			err:  errors.New("synthetic identity failure"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withUserInfoHook(t, func() ([]byte, error) {
				if tc.err != nil {
					return nil, tc.err
				}
				return tc.body(t), nil
			})
			id := "shape-" + strings.ReplaceAll(tc.name, " ", "-")
			if _, err := authzClient(t, broker, id, "alice", 4); err == nil {
				t.Fatal("an identity the broker cannot vouch for must be refused, not admitted unpoliced")
			}
		})
	}

	// Two controls, and neither is optional. Every row above asserts a refusal, so
	// a broker that refused EVERY CONNECT would satisfy all of them.
	// The control, and it is not optional: every row above asserts a refusal, so a
	// broker that refused EVERY CONNECT would satisfy all of them.
	t.Run("a well formed same-account identity is accepted", func(t *testing.T) {
		withUserInfoHook(t, func() ([]byte, error) {
			return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Account: "APP", Permissions: safe}), nil
		})
		c, err := authzClient(t, broker, "shape-control", "alice", 4)
		if err != nil {
			t.Fatalf("a valid identity must be accepted: %v", err)
		}
		if tok := c.Publish("anything/at/all", 1, false, []byte("x")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("an allowed subject must publish: %v", tok.Error())
		}
	})
	t.Run("an identity with no permissions at all is accepted and unrestricted", func(t *testing.T) {
		// The shape a server with no ACLs emits, and it must be read as
		// unrestricted: the opposite reading would refuse every publish on the
		// default deployment.
		//
		// It is ALSO the shape a forged reply would claim, and the broker cannot
		// tell the two apart -- see mqttUserInfoInboxPrefix. That is a deployment
		// prerequisite (deny MQTT identities publish on the reserved namespace),
		// not something checkable here: any test of the claimed permissions is a
		// test the forger controls. An earlier revision tried and was circular.
		withUserInfoHook(t, func() ([]byte, error) {
			return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Account: "APP"}), nil
		})
		c, err := authzClient(t, broker, "shape-noperms", "alice", 4)
		if err != nil {
			t.Fatalf("an identity with no configured permissions must be accepted: %v", err)
		}
		if tok := c.Publish("anything/at/all", 1, false, []byte("x")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("no configured permissions means unrestricted: %v", tok.Error())
		}
	})
}

// TestRetainedReplayHonoursASubscribeDeny is F-081's retained arm, and it is a
// different route from the QoS 1/2 one: retained replay builds its packet
// directly and never reaches the delivery funnel, so it carries its own guard.
//
// It is also a different route from TestDeniedRetainedPublishNotPersisted, which
// is about a denied PUBLISH. Here the publish is allowed and the copy is
// legitimately stored; what must not happen is its replay to a subscriber whose
// account denies that subject.
func TestRetainedReplayHonoursASubscribeDeny(t *testing.T) {
	t.Parallel()
	broker, s := authzBroker(t, authzConfOneAccount)

	pub, err := authzClient(t, broker, "retdeny-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	// foo/secret is allowed to PUBLISH and denied to SUBSCRIBE, so both copies are
	// really stored and the test turns entirely on replay.
	for _, tc := range []struct{ topic, subject, payload string }{
		{"foo/public", "foo.public", "ok"},
		{"foo/secret", "foo.secret", "leak"},
	} {
		if tok := pub.Publish(tc.topic, 1, true, []byte(tc.payload)); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
			t.Fatalf("retained publish %q: %v", tc.topic, tok.Error())
		}
		deadline := time.Now().Add(5 * time.Second)
		for !storedOnStream(t, s, mqttRetainedMsgsStreamName, mqttRetainedMsgsStreamSubject+tc.subject) {
			if time.Now().After(deadline) {
				t.Fatalf("fixture: the retained copy for %q must exist, or the replay assertion is vacuous", tc.topic)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	got := make(chan pmqtt.Message, 8)
	sub, err := authzClient(t, broker, "retdeny-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	if tok := sub.Subscribe("foo/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	// The control and the defect arrive on the same wildcard, so the allowed one
	// must be seen before the denied one's absence means anything.
	deadline := time.After(5 * time.Second)
	sawPublic := false
	for !sawPublic {
		select {
		case m := <-got:
			switch m.Topic() {
			case "foo/public":
				sawPublic = true
			case "foo/secret":
				t.Fatal("a retained message on a subject the account denies for SUBSCRIBE must not be replayed")
			default:
				t.Fatalf("unexpected retained topic %q", m.Topic())
			}
		case <-deadline:
			t.Fatal("control: the allowed retained message must be replayed through the wildcard")
		}
	}
	// And nothing else follows it.
	select {
	case m := <-got:
		t.Fatalf("a retained message on a denied subject must not be replayed, got %q", m.Topic())
	case <-time.After(1500 * time.Millisecond):
	}
}

// TestDeniedPublishQoS2NotHeldOrDelivered is F-079's QoS 2 row.
//
// QoS 2 has its own gate, inside the hold store rather than in mqttProcessPub,
// because an occupied packet identifier must be answered from the existing
// exchange whatever subject the incoming packet names. So it needs its own
// fixture: a mutation run showed the QoS 0/1 fixtures leave the QoS 2 gate
// entirely uncovered.
//
// The observable for 3.1.1 is the connection closing, which is what [3.3.5.2]
// offers in place of a reason code and what this broker takes -- answering PUBREC
// would tell the publisher its denied message was accepted, which is the measured
// symptom F-079 reported.
func TestDeniedPublishQoS2NotHeldOrDelivered(t *testing.T) {
	t.Parallel()
	broker, _ := authzBroker(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 8)
	sub, err := authzClient(t, broker, "q2-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	if tok := sub.Subscribe("#", 2, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pub, err := authzClient(t, broker, "q2-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	// CONTROL: the QoS 2 exchange completes and delivers when it is allowed.
	if tok := pub.Publish("allowed/q2", 2, false, []byte("yes")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control publish: %v", tok.Error())
	}
	select {
	case m := <-got:
		if string(m.Payload()) != "yes" {
			t.Fatalf("control: got %q", m.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("control: an ALLOWED QoS 2 publish must be delivered, or this test proves nothing")
	}

	// THE DEFECT. Measured before the fix: PUBREC/PUBCOMP reported success and the
	// subscriber received it.
	pub.Publish("denied/q2", 2, false, []byte("no")).WaitTimeout(5 * time.Second)
	select {
	case m := <-got:
		t.Fatalf("a denied QoS 2 publish must not be delivered, got %q on %q", m.Payload(), m.Topic())
	case <-time.After(2 * time.Second):
	}
	// A 3.1.1 publisher is refused by the connection closing, since the protocol
	// gives no per-publish refusal code.
	if pub.IsConnectionOpen() {
		t.Fatal("a denied QoS 2 publish must not leave a 3.1.1 connection open reporting success")
	}
}

// authzBrokerTrackingDials is authzBroker with a handle on the per-client NATS
// connections it dialled, so a test can break one without touching the service
// connection.
func authzBrokerTrackingDials(t *testing.T, conf string) (broker string, dialed func(string) *nats.Conn, svcConn *nats.Conn, mqttSrv *Server) {
	t.Helper()
	path := t.TempDir() + "/authz.conf"
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	opts.NoSigs = true
	opts.StoreDir = t.TempDir()
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	url := srv.ClientURL()
	svc, err := nats.Connect(url, nats.UserInfo("svc", "svcpw"))
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(svc.Close)

	var mu sync.Mutex
	conns := map[string]*nats.Conn{}
	s := NewServer(svc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: -1}}, "authz-dial-node")
	s.SetClientDialer(func(clientID, user, pass string) (*nats.Conn, error) {
		nc, derr := nats.Connect(url, nats.UserInfo(user, pass), nats.MaxReconnects(0))
		if derr == nil {
			mu.Lock()
			conns[clientID] = nc
			mu.Unlock()
		}
		return nc, derr
	})
	if err := s.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(s.Shutdown)
	return fmt.Sprintf("tcp://%s", s.Addr().String()), func(id string) *nats.Conn {
		mu.Lock()
		defer mu.Unlock()
		return conns[id]
	}, svc, s
}

// TestPerClientNATSLossClosesTheConnectionAndStillFiresAnAuthorizedWill covers
// the two halves of the snapshot's lifecycle that pull in opposite directions.
//
// Losing the per-client NATS connection must revoke the snapshot, because that
// connection re-authenticates on every reconnect and may come back with different
// permissions -- so the MQTT connection goes with it.
//
// But the Will must still fire. Every site that discharges a Will runs on the
// teardown path, after the snapshot has been invalidated, so a broker that asked
// "may this client publish?" there would suppress an AUTHORIZED Will on every
// NATS blip. That is why the decision is captured at CONNECT and merely read
// later, and this test is what makes that falsifiable: with the capture removed
// and the epoch consulted instead, the Will vanishes and only this fails.
func TestPerClientNATSLossClosesTheConnectionAndStillFiresAnAuthorizedWill(t *testing.T) {
	t.Parallel()
	broker, dialed, _, _ := authzBrokerTrackingDials(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 4)
	watcher, err := authzClient(t, broker, "will-watch", "alice", 4)
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	if tok := watcher.Subscribe("will/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	// A Will on an ALLOWED topic: the point is that authorization does not stand in
	// the way of it, not that a denied one is blocked (which other fixtures cover).
	// QoS 0, deliberately. A QoS 1/2 Will survives this trigger even if the live
	// fan-out fails, because its store carries it -- so a QoS 1 Will proves nothing
	// about the path that actually breaks. The immediate fan-out rides the very
	// connection whose loss caused the teardown, so at QoS 0 the store is not there
	// to save it and the Will must be handed to the client-free route instead.
	o := pmqtt.NewClientOptions().AddBroker(broker).SetClientID("will-owner").
		SetUsername("alice").SetPassword("pw").SetCleanSession(true).SetProtocolVersion(4).
		SetWill("will/allowed", "goodbye", 0, false).
		SetAutoReconnect(false).SetConnectTimeout(8 * time.Second)
	owner := pmqtt.NewClient(o)
	if tok := owner.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		t.Fatalf("will owner connect: %v", tok.Error())
	}
	t.Cleanup(func() { owner.Disconnect(100) })

	nc := dialed("will-owner")
	if nc == nil {
		t.Fatal("fixture: the per-client NATS connection was not captured, so this test would prove nothing")
	}
	// Break the per-client connection only. The service connection is untouched,
	// which is what makes this a snapshot-lifecycle test and not a broker outage.
	nc.Close()

	deadline := time.Now().Add(10 * time.Second)
	for owner.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("losing the per-client NATS connection must close the MQTT connection: " +
				"its identity snapshot can no longer be vouched for")
		}
		time.Sleep(50 * time.Millisecond)
	}

	select {
	case m := <-got:
		if string(m.Payload()) != "goodbye" {
			t.Fatalf("the Will payload must survive intact, got %q", m.Payload())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("an AUTHORIZED Will must still fire when the snapshot is invalidated: " +
			"the decision was captured at CONNECT precisely because every discharge site " +
			"runs after the connection is gone")
	}
}

// TestNoServiceOwnedEffectAfterSnapshotInvalidation proves that revoking the
// snapshot -- not closing the socket -- is what stops an unauthorized effect.
//
// The distinction is not academic. readLoop tears the connection down only after
// the current parse returns, so at the moment a NATS disconnect fires there can
// already be a PUBLISH in hand. A broker that merely closed the socket would let
// that PUBLISH run to completion on the service connection, writing durable state
// on behalf of an identity that no longer exists. Closing is the consequence;
// invalidation is the primitive.
//
// A mutation that removes the invalidation and keeps the close passes every other
// fixture in this file, which is why this one exists.
func TestNoServiceOwnedEffectAfterSnapshotInvalidation(t *testing.T) {
	broker, dialed, _, _ := authzBrokerTrackingDials(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 4)
	watcher, err := authzClient(t, broker, "inflight-watch", "alice", 4)
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	if tok := watcher.Subscribe("inflight/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	pub, err := authzClient(t, broker, "inflight-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}

	// CONTROL FIRST, with no hook installed: the topic is allowed and the path
	// works, so the absence below is the revocation and not a broken fixture.
	if tok := pub.Publish("inflight/control", 1, false, []byte("before")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control publish: %v", tok.Error())
	}
	select {
	case m := <-got:
		if string(m.Payload()) != "before" {
			t.Fatalf("control: got %q", m.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("control: an allowed publish must be delivered before the snapshot is revoked")
	}

	nc := dialed("inflight-pub")
	if nc == nil {
		t.Fatal("fixture: the per-client NATS connection was not captured")
	}

	// Park the next PUBLISH after parsing and before authorization, revoke the
	// snapshot while it is parked, then let it go.
	var once sync.Once
	released := make(chan struct{})
	pause := func() {
		once.Do(func() {
			nc.Close()
			// The handler runs on a nats.go goroutine, so give it the moment it
			// needs to take c.mu and clear the flag before the gate reads it.
			time.Sleep(300 * time.Millisecond)
			close(released)
		})
	}
	mqttTestPublishGatePauseV.Store(&pause)
	t.Cleanup(func() { mqttTestPublishGatePauseV.Store(nil) })

	pub.Publish("inflight/after", 1, false, []byte("after")).WaitTimeout(5 * time.Second)
	select {
	case <-released:
	case <-time.After(10 * time.Second):
		t.Fatal("fixture: the parked publish never reached the gate")
	}

	select {
	case m := <-got:
		t.Fatalf("a publish whose identity was revoked while it was in flight must produce "+
			"no service-owned effect, got %q on %q", m.Payload(), m.Topic())
	case <-time.After(2 * time.Second):
	}
}

// clientByID returns the broker's client object for an MQTT client id, so a test
// can drive an internal function with state a wire-level fixture cannot arrange.
func clientByID(t *testing.T, s *Server, id string) *client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		for _, c := range s.clients {
			c.mu.Lock()
			match := c.mqtt != nil && c.mqtt.cid == id
			c.mu.Unlock()
			if match {
				s.mu.Unlock()
				return c
			}
		}
		s.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("no client with id %q", id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRetainedHookUsesTheDeliveredPublicationNotTheParserState pins the fix for a
// bug that predates this item and was found through it.
//
// mqttHandlePubRetain used to read c.mqtt.pp -- the SHARED parser object, which
// every packet the client sends overwrites -- instead of the publication actually
// being delivered. mqttProcessPubRel reconstructs the released QoS 2 publication
// into a local pp and hands it down, so a PUBLISH parsed between the original
// PUBLISH and its PUBREL would be retained in its place.
//
// Two consequences, and the second is why this item owns the fix: the retained
// copy is simply the wrong message, and if the intervening packet was a DENIED
// Sparkplug birth, its derived certificate effect is manufactured on the back of
// an unrelated authorized exchange -- an authorization bypass no gate can catch,
// because the effect is produced from state the gate never saw.
//
// Driven directly rather than over the wire: paho sends PUBREL immediately after
// PUBREC, so the interleaving cannot be arranged from outside, and a test that
// cannot arrange it would prove nothing.
func TestRetainedHookUsesTheDeliveredPublicationNotTheParserState(t *testing.T) {
	t.Parallel()
	broker, s := authzBroker(t, authzConfOneAccount)
	if _, err := authzClient(t, broker, "retpp", "alice", 4); err != nil {
		t.Fatalf("connect: %v", err)
	}
	c := clientByID(t, s, "retpp")

	delivered := &mqttPublish{
		topic:   []byte("kept/topic"),
		subject: []byte("kept.topic"),
		msg:     []byte("delivered"),
		sz:      len("delivered"),
		flags:   mqttPubFlagRetain,
	}
	stale := &mqttPublish{
		topic:   []byte("stale/topic"),
		subject: []byte("stale.topic"),
		msg:     []byte("stale"),
		sz:      len("stale"),
		flags:   mqttPubFlagRetain,
	}
	// The parser object holds a DIFFERENT publication, exactly as it would after a
	// packet arrived between a QoS 2 PUBLISH and its PUBREL.
	c.mu.Lock()
	c.mqtt.pp = stale
	c.mu.Unlock()

	// Through the real delivery path, not by calling the hook directly: the bug was
	// the CALL SITE reading hidden state rather than passing what it was given, so
	// a test that invoked the hook itself would leave that untested.
	if err := s.mqttInitiateMsgDeliveryJSA(c, &c.mqtt.asm.jsa, delivered, _EMPTY_, _EMPTY_, _EMPTY_); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !storedOnStream(t, s, mqttRetainedMsgsStreamName, mqttRetainedMsgsStreamSubject+"kept.topic") {
		if time.Now().After(deadline) {
			t.Fatal("the publication passed to the retained hook must be the one retained")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if storedOnStream(t, s, mqttRetainedMsgsStreamName, mqttRetainedMsgsStreamSubject+"stale.topic") {
		t.Fatal("the retained hook must not read the shared parser object: it retained the " +
			"packet the client happened to send last instead of the one being delivered")
	}
}

// derivedCertSubject is the NATS subject a Sparkplug birth's derived certificate
// copy is stored under, computed with the broker's own converter rather than
// spelled out: a literal "." in an MQTT topic encodes as "//", so a hand-written
// expectation is wrong in a way that looks right.
func derivedCertSubject(t *testing.T, birthTopic string) string {
	t.Helper()
	derived := append(append([]byte{}, sparkbCertificatesTopicPrefix...), birthTopic...)
	subj, err := mqttTopicToNATSPubSubject(derived)
	if err != nil {
		t.Fatalf("derive certificate subject for %q: %v", birthTopic, err)
	}
	return string(subj)
}

// TestDeniedSparkplugCertificateSubjectNotPublished is the derived-subject route.
//
// A Sparkplug birth is ONE publication with TWO effective subjects: the topic the
// client named, and a `$sparkplug/certificates/...` subject the broker derives,
// publishes and retains on the SERVICE JSA. Authorizing only what the client
// named leaves the derived one unpoliced, which is the same defect one level
// down. The config here allows the birth topic and denies exactly its derived
// subject, so nothing but the derived check can produce the expected outcome.
func TestDeniedSparkplugCertificateSubjectNotPublished(t *testing.T) {
	t.Parallel()
	broker, s := authzBroker(t, authzConfOneAccount)
	pub, err := authzClient(t, broker, "spb-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}

	// A witness on the ORIGINAL birth topic, because the derived copy is only one
	// of the publication's two effects. The rule is all-or-nothing -- every
	// effective subject is authorized before the FIRST effect -- so a broker that
	// fanned the birth out and suppressed only the derived copy has still published
	// something it was not entitled to publish, and the derived-store check alone
	// cannot see that.
	got := make(chan pmqtt.Message, 8)
	watch, err := authzClient(t, broker, "spb-watch", "alice", 4)
	if err != nil {
		t.Fatalf("witness: %v", err)
	}
	if tok := watch.Subscribe("spBv1.0/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("witness subscribe: %v", tok.Error())
	}

	// CONTROL: a birth whose derived subject is ALLOWED produces the derived
	// retained copy. Without it, a broker with no Sparkplug support at all passes.
	if tok := pub.Publish("spBv1.0/other/NBIRTH/node", 0, false, []byte("ok")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control birth: %v", tok.Error())
	}
	deadline := time.Now().Add(5 * time.Second)
	for !storedOnStream(t, s, mqttRetainedMsgsStreamName,
		mqttRetainedMsgsStreamSubject+derivedCertSubject(t, "spBv1.0/other/NBIRTH/node")) {
		if time.Now().After(deadline) {
			t.Fatal("control: an allowed birth must produce its derived certificate copy")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// And the control birth REACHES the witness. This is what stops the absence
	// asserted below being satisfied by a witness that was never going to receive
	// anything -- a wrong filter, a subscription that never completed, a broker
	// with Sparkplug fan-out switched off entirely.
	select {
	case m := <-got:
		if m.Topic() != "spBv1.0/other/NBIRTH/node" {
			t.Fatalf("control: the witness received %q, not the control birth", m.Topic())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("control: an allowed birth must reach a subscriber; without this the " +
			"non-delivery asserted below proves nothing")
	}

	// THE DEFECT: the birth topic is allowed, its derived subject is denied.
	if tok := pub.Publish("spBv1.0/grp/NBIRTH/node", 0, false, []byte("leak")); !tok.WaitTimeout(5 * time.Second) {
		t.Fatal("denied birth did not settle")
	}
	time.Sleep(500 * time.Millisecond)
	if storedOnStream(t, s, mqttRetainedMsgsStreamName,
		mqttRetainedMsgsStreamSubject+derivedCertSubject(t, "spBv1.0/grp/NBIRTH/node")) {
		t.Fatal("a birth whose DERIVED certificate subject is denied must produce no derived copy: " +
			"authorizing the subject the client named is not authorizing the one the broker derives")
	}
	select {
	case m := <-got:
		t.Fatalf("the ORIGINAL birth was delivered (%q) although the publication was refused: "+
			"a Sparkplug birth is one publication with two effective subjects, and suppressing "+
			"only the derived one still publishes what the identity was not entitled to publish",
			m.Topic())
	default:
	}

	// This is MQTT 3.1.1, which has no QoS 0 acknowledgement and therefore no way to
	// tell the client its publication was refused except by closing the connection.
	// Leaving it open reports apparent success for a publication that never happened.
	for deadline := time.Now().Add(5 * time.Second); pub.IsConnected(); {
		if time.Now().After(deadline) {
			t.Fatal("a 3.1.1 client whose publication was refused must have its connection " +
				"closed: with no PUBACK on QoS 0, an open connection tells it the publish " +
				"succeeded")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestOccupiedQoS2IdentifierIgnoresTheIncomingSubject pins the ordering the
// protocol forces on the QoS 2 gate.
//
// MQTT transfers ownership of the message to the receiver at PUBREC, so a later
// PUBLISH on the same packet identifier before PUBREL must be answered from the
// exchange already held, whatever subject it names. An unconditional subject
// check ahead of that would let a deliberately denied subject reject an exchange
// the broker already owns -- and a denied publish must not be able to reap or
// mutate another exchange's record either.
func TestOccupiedQoS2IdentifierIgnoresTheIncomingSubject(t *testing.T) {
	t.Parallel()
	broker, s := authzBroker(t, authzConfOneAccount)
	if _, err := authzClient(t, broker, "occ", "alice", 4); err != nil {
		t.Fatalf("connect: %v", err)
	}
	c := clientByID(t, s, "occ")

	held := &mqttPublish{
		topic: []byte("allowed/held"), subject: []byte("allowed.held"),
		msg: []byte("held"), sz: 4, pi: 7, flags: 2 << 1,
	}
	// A success PUBREC is the hold's acceptance; a refusal PUBREC (0x87) or an error is not.
	if resp, err := s.mqttStoreQoS2MsgOnce(c, held, _EMPTY_, nil, 0); err != nil || !resp.send || resp.reason != mqttReasonSuccess {
		t.Fatalf("the allowed hold must be created: answer %+v, %v", resp, err)
	}
	heldSubject := c.mqttQoS2InternalSubject(7)
	before := loadQoS2HoldForAuthz(t, c, heldSubject)

	// The same identifier, a DENIED subject. The exchange is already the broker's,
	// so this must be accepted as a duplicate rather than refused.
	intruder := &mqttPublish{
		topic: []byte("denied/other"), subject: []byte("denied.other"),
		msg: []byte("other"), sz: 5, pi: 7, flags: 2 << 1,
	}
	if resp, err := s.mqttStoreQoS2MsgOnce(c, intruder, _EMPTY_, nil, 0); err != nil || !resp.send || resp.reason != mqttReasonSuccess {
		t.Fatalf("a PUBLISH on an OCCUPIED identifier must be answered from the hold, "+
			"not refused on its own subject: answer %+v, %v", resp, err)
	}

	// "Answered from the hold" is a claim about WHICH record is there afterwards,
	// and a second store returning nil cannot tell that apart from a replacement:
	// a denied duplicate that overwrote the record would leave the identifier just
	// as occupied. So the durable record itself is read back. The generation is the
	// identity -- it is minted once per accepted PUBLISH -- and the payload and the
	// encoded subject are what a replacement would have carried away with it.
	after := loadQoS2HoldForAuthz(t, c, heldSubject)
	if after.gen != before.gen {
		t.Fatalf("the denied duplicate REPLACED the accepted exchange (generation %q -> %q): "+
			"ownership passed at PUBREC and the record is no longer the client's to overwrite",
			before.gen, after.gen)
	}
	if after.state != mqttQoS2Held {
		t.Fatalf("the accepted exchange must still be HELD after a denied duplicate, got state %v",
			after.state)
	}
	if got := string(after.stored.Data); got != "held" {
		t.Fatalf("the hold now carries the DENIED publication's payload (%q): the gate stored "+
			"content the identity was never authorized for", got)
	}
	if got := string(getHeader(mqttNatsHeaderSubject, after.stored.Header)); got != "allowed.held" {
		t.Fatalf("the hold now names the DENIED subject (%q): a PUBREL would deliver it", got)
	}

	// The control that keeps this from passing vacuously: a denied subject on a
	// FREE identifier is still refused.
	fresh := &mqttPublish{
		topic: []byte("denied/fresh"), subject: []byte("denied.fresh"),
		msg: []byte("fresh"), sz: 5, pi: 8, flags: 2 << 1,
	}
	// Refused is an error (3.1.1, this client) or a refusal PUBREC (v5): anything but the
	// success PUBREC that accepts the hold.
	if resp, err := s.mqttStoreQoS2MsgOnce(c, fresh, _EMPTY_, nil, 0); err == nil && resp.send && resp.reason == mqttReasonSuccess {
		t.Fatal("a denied subject on an UNOCCUPIED identifier is a new publication and must be refused")
	}
	// Refusing the client is only half of it: the refusal must also have written
	// nothing. An error return says what the CLIENT was told, not what the stream
	// holds, and a gate that stored first and refused afterwards would leave an
	// unauthorized payload for a later PUBREL to deliver.
	rec, _, err := c.mqtt.asm.loadQoS2Record(c.mqttQoS2InternalSubject(8))
	if err != nil {
		t.Fatalf("reading back the refused identifier: %v", err)
	}
	if rec != nil {
		t.Fatalf("a REFUSED publication left a record behind (state %v): the refusal must be "+
			"total, not a message the client was not told about", rec.state)
	}
}

// loadQoS2HoldForAuthz returns the hold that must exist on subject, failing the
// test if it does not. An absent record is never an acceptable answer here: every
// caller has just asserted that an accepted exchange survives.
func loadQoS2HoldForAuthz(t *testing.T, c *client, subject string) *mqttQoS2Record {
	t.Helper()
	rec, _, err := c.mqtt.asm.loadQoS2Record(subject)
	if err != nil {
		t.Fatalf("loading the hold on %q: %v", subject, err)
	}
	if rec == nil {
		t.Fatalf("the hold on %q is GONE: a denied publish must not be able to reap "+
			"another exchange's record", subject)
	}
	return rec
}

// TestServiceEpochInvalidOnDisconnectRefusesPublication covers the half of the
// epoch that belongs to the SERVICE connection rather than to any client.
//
// It has to be invalidated on the DISCONNECT, not on the reconnect, and that is
// not a stylistic preference: nats.go resends subscriptions and flushes its
// reconnect buffer BEFORE it queues the reconnect callback, so a callback that
// discovers the account changed discovers it after the buffered effects have
// already been written into the new account. Invalidating on the disconnect is
// what stops an effect being buffered at all.
//
// The discriminating observable is the REFUSAL, not the absence: with the epoch
// invalid the publisher is refused deterministically (a 3.1.1 client by having
// its connection closed), whereas a broker that let the publication through
// merely has its JetStream store time out, which also produces no message.
func TestServiceEpochInvalidOnDisconnectRefusesPublication(t *testing.T) {
	broker, _, svc, srv := authzBrokerTrackingDials(t, authzConfOneAccount)

	pub, err := authzClient(t, broker, "svcepoch-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	// CONTROL: the path works while the service connection is up.
	if tok := pub.Publish("epoch/before", 1, false, []byte("ok")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control publish: %v", tok.Error())
	}
	if !pub.IsConnectionOpen() {
		t.Fatal("control: the publisher must still be connected")
	}

	svc.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := srv.serviceAccount(); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("closing the service connection must invalidate the service epoch")
		}
		time.Sleep(50 * time.Millisecond)
	}

	pub.Publish("epoch/after", 1, false, []byte("no")).WaitTimeout(5 * time.Second)
	closed := false
	// Generous, because the refusal is prompt but the TEARDOWN is not: with the
	// service connection gone, the disconnect path's JetStream requests each wait
	// out their own timeout before the socket finally closes. The assertion is
	// about the refusal; the wait only has to outlast that teardown.
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if !pub.IsConnectionOpen() {
			closed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !closed {
		t.Fatal("with the service epoch invalid a publication must be REFUSED, not attempted: " +
			"a 3.1.1 publisher is refused by having its connection closed")
	}
}

// v5AuthzClient is an MQTT 5 client against an authz broker, for the assertions
// only v5 can carry: a reason code on the acknowledgement.
func v5AuthzClient(t *testing.T, brokerTCP, id string) *paho.Client {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(brokerTCP, "tcp://"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := paho.NewClient(paho.ClientConfig{Conn: conn})
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

// TestDeniedPublishTellsAnMQTT5PublisherWhy is the half of the refusal that
// MQTT 3.1.1 cannot express.
//
// F-079's measured symptom was not only that the message got through: it was that
// the publisher was told PUBACK, i.e. success. MQTT 5 has a reason code for
// exactly this, so a v5 client must receive 0x87 and stay connected. Without this
// arm every authorization test here speaks 3.1.1, and restoring a success-shaped
// v5 acknowledgement would leave the suite green - which is the defect restated.
func TestDeniedPublishTellsAnMQTT5PublisherWhy(t *testing.T) {
	t.Parallel()
	broker, _ := authzBroker(t, authzConfOneAccount)
	c := v5AuthzClient(t, broker, "v5-deny-pub")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// CONTROL: an allowed publish is acknowledged with success, so a nonzero
	// reason below is the deny and not a broker that refuses everything.
	ack, err := c.Publish(ctx, &paho.Publish{Topic: "allowed/v5", QoS: 1, Payload: []byte("ok")})
	if err != nil {
		t.Fatalf("control publish: %v", err)
	}
	if ack.ReasonCode != 0 {
		t.Fatalf("control: an allowed QoS 1 publish must be acknowledged 0, got 0x%02x", ack.ReasonCode)
	}

	for _, tc := range []struct {
		name  string
		qos   byte
		topic string
	}{
		{"qos1", 1, "denied/v5q1"},
		{"qos2", 2, "denied/v5q2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// paho.golang surfaces a non-success QoS 1 acknowledgement as an
			// error AND returns the response, so the reason code is read from the
			// response either way; only a missing response is a real failure.
			ack, err := c.Publish(ctx, &paho.Publish{Topic: tc.topic, QoS: tc.qos, Payload: []byte("no")})
			if ack == nil {
				t.Fatalf("denied publish returned no acknowledgement to read a reason from: %v", err)
			}
			if ack.ReasonCode != mqttReasonNotAuthorized {
				t.Fatalf("a denied QoS %d publish must be acknowledged 0x%02x Not authorized, got 0x%02x: "+
					"telling the publisher its denied message was accepted is the defect this closes",
					tc.qos, mqttReasonNotAuthorized, ack.ReasonCode)
			}
		})
	}
}

// TestSameAccountServiceReconnectDoesNotStrandClients is the guard against the
// cure being worse than the disease.
//
// An earlier revision pinned each snapshot to a per-reconnect GENERATION. That
// closed the cross-account hazard and opened a much larger one: an ordinary
// same-account reconnect advanced the generation, so every live client's
// publications were refused and its deliveries withheld FOREVER, with no event
// that would ever restore them. Binding to the ACCOUNT instead makes a
// same-account reconnect a non-event, which is what this test pins.
func TestSameAccountServiceReconnectDoesNotStrandClients(t *testing.T) {
	broker, _, svc, srv := authzBrokerTrackingDials(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 4)
	sub, err := authzClient(t, broker, "reconn-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	if tok := sub.Subscribe("reconn/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	pub, err := authzClient(t, broker, "reconn-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}

	// Simulate the reconnect handler's same-account outcome directly. Bouncing a
	// real service connection would also tear down the JetStream assets and the
	// delivery subscriptions, which is a different failure and would mask this one.
	srv.svcID.mu.Lock()
	acct := srv.svcID.account
	srv.svcID.valid = false
	srv.svcID.mu.Unlock()
	srv.svcID.mu.Lock()
	srv.svcID.account, srv.svcID.valid = acct, true
	srv.svcID.mu.Unlock()
	_ = svc

	// The clients were established before that cycle and must be entirely
	// unaffected: same account, same destination, nothing they were authorized
	// against has changed.
	if tok := pub.Publish("reconn/after", 1, false, []byte("still works")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("a same-account service reconnect must not refuse an existing client's publish: %v", tok.Error())
	}
	select {
	case m := <-got:
		if string(m.Payload()) != "still works" {
			t.Fatalf("got %q", m.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a same-account service reconnect must not withhold an existing client's delivery")
	}
	if !pub.IsConnectionOpen() {
		t.Fatal("a same-account service reconnect must not close existing clients")
	}
}

// TestWillOutboxRecordIsRefusedForAnotherAccount pins the destination binding on
// the persisted delayed Will.
//
// A delayed Will is published long after its connection is gone, by an internal
// client with no identity of its own, so the RECORD is the decision. A decision
// taken for one account cannot license a publication into another - and the
// comparison is exact rather than "differs if set", because every writer of an
// outbox record stamps the account it was authorized for, so an EMPTY one under
// auth passthrough is a record no writer here produced. Treating empty as "no
// opinion" let exactly the records this check exists to stop straight through,
// which is how the third writer (the uncertainty-parked path) was found.
func TestWillOutboxRecordIsRefusedForAnotherAccount(t *testing.T) {
	t.Parallel()
	broker, s := authzBroker(t, authzConfOneAccount)
	if _, err := authzClient(t, broker, "willacct", "alice", 4); err != nil {
		t.Fatalf("connect: %v", err)
	}
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}
	svcAccount, ok := s.serviceAccountForWill()
	if !ok || svcAccount == _EMPTY_ {
		t.Fatalf("fixture: the service account must be established, got %q ok=%v", svcAccount, ok)
	}

	base := func(acct string) *mqttWillOutbox {
		return &mqttWillOutbox{
			WillID: "w-" + acct, Account: acct,
			Topic: "will/acct", Msg: []byte("bye"), QoS: 0,
		}
	}
	// CONTROL: the account it was authorized for is published. Without this the
	// refusals below would be satisfied by a broker that publishes nothing.
	if err := asm.deliverWillRecord(s, base(svcAccount)); err != nil {
		t.Fatalf("a Will recorded for the CURRENT service account must be published: %v", err)
	}
	for _, acct := range []string{"SOMEWHERE_ELSE", _EMPTY_} {
		if err := asm.deliverWillRecord(s, base(acct)); err == nil {
			t.Fatalf("a Will recorded for account %q must be refused while the service "+
				"connection is bound to %q, and left retryable rather than published", acct, svcAccount)
		}
	}
}

// TestNoDurableWriteAfterInvalidationBetweenGateAndStore covers the window the
// authorization gate does NOT.
//
// The gate runs early, before the live fan-out and the subs barrier; the durable
// write happens several steps later. TestNoServiceOwnedEffectAfterSnapshotInvalidation
// parks BEFORE the gate, so the gate refuses and the final fence is never
// exercised - a mutation dropping the fence's validity half survives it. This one
// parks AFTER the gate and immediately before the store, which is the only place
// that mutation is visible.
func TestNoDurableWriteAfterInvalidationBetweenGateAndStore(t *testing.T) {
	broker, dialed, _, srv := authzBrokerTrackingDials(t, authzConfOneAccount)

	// A QoS 1 subscriber, because delivery through $MQTT_msgs is the observable:
	// the stream is interest-retained, so reading it directly races the reaper and
	// with no subscriber it keeps nothing at all. The live fan-out happens BEFORE
	// the seam below, so a QoS 1 delivery can only come from the store the fence
	// guards.
	got := make(chan pmqtt.Message, 4)
	sub, err := authzClient(t, broker, "latefence-sub", "alice", 4)
	if err != nil {
		t.Fatalf("subscriber: %v", err)
	}
	if tok := sub.Subscribe("latefence/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	pub, err := authzClient(t, broker, "latefence-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	// CONTROL, with no seam installed.
	if tok := pub.Publish("latefence/before", 1, false, []byte("ok")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control publish: %v", tok.Error())
	}
	select {
	case m := <-got:
		if string(m.Payload()) != "ok" {
			t.Fatalf("control: got %q", m.Payload())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("control: an allowed QoS 1 publish must be stored and delivered")
	}
	_ = srv

	nc := dialed("latefence-pub")
	if nc == nil {
		t.Fatal("fixture: the per-client NATS connection was not captured")
	}
	var once sync.Once
	pause := func() {
		once.Do(func() {
			nc.Close()
			time.Sleep(300 * time.Millisecond)
		})
	}
	mqttTestDurableWritePauseV.Store(&pause)
	t.Cleanup(func() { mqttTestDurableWritePauseV.Store(nil) })

	pub.Publish("latefence/after", 1, false, []byte("no")).WaitTimeout(5 * time.Second)
	select {
	case m := <-got:
		t.Fatalf("a publication whose identity was revoked between the gate and the store "+
			"must not be written: the fence exists for exactly this window, got %q", m.Payload())
	case <-time.After(2 * time.Second):
	}
}

// TestNoDerivedSparkplugEffectAfterInvalidation covers the derived publication's
// own fence.
//
// The derived `$sparkplug/certificates/...` send happens before the retained
// store, so the store's fence cannot retract it -- it needs one of its own. And
// the authorization gate refuses first, so removing that fence is invisible
// unless a test revokes the identity BETWEEN the gate and the send. That is what
// the seam here does; without it the fence has no discriminating mutation.
func TestNoDerivedSparkplugEffectAfterInvalidation(t *testing.T) {
	broker, dialed, svc, srv := authzBrokerTrackingDials(t, authzConfOneAccount)

	// The DERIVED LIVE publication is what this fence guards, and it is a plain
	// NATS send on the service connection. Observing the retained copy instead
	// would prove nothing: that store has a fence of its own, so removing this one
	// leaves the retained assertion unchanged - which a mutation run showed.
	live := make(chan *nats.Msg, 8)
	if _, serr := svc.Subscribe(derivedCertSubject(t, "spBv1.0/other/NBIRTH/>"), func(m *nats.Msg) { live <- m }); serr != nil {
		t.Fatalf("observe derived sends: %v", serr)
	}
	if ferr := svc.Flush(); ferr != nil {
		t.Fatalf("flush: %v", ferr)
	}

	pub, err := authzClient(t, broker, "spbfence-pub", "alice", 4)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	// CONTROL, no seam: an allowed birth produces its derived retained copy.
	if tok := pub.Publish("spBv1.0/other/NBIRTH/n1", 0, false, []byte("ok")); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("control birth: %v", tok.Error())
	}
	select {
	case <-live:
	case <-time.After(5 * time.Second):
		t.Fatal("control: an allowed birth must produce its derived certificate publication")
	}
	_ = srv

	nc := dialed("spbfence-pub")
	if nc == nil {
		t.Fatal("fixture: the per-client NATS connection was not captured")
	}
	var once sync.Once
	pause := func() {
		once.Do(func() {
			nc.Close()
			time.Sleep(300 * time.Millisecond)
		})
	}
	mqttTestDerivedSendPauseV.Store(&pause)
	t.Cleanup(func() { mqttTestDerivedSendPauseV.Store(nil) })

	pub.Publish("spBv1.0/other/NBIRTH/n2", 0, false, []byte("no")).WaitTimeout(5 * time.Second)
	select {
	case m := <-live:
		t.Fatalf("a birth whose identity was revoked between the gate and the derived send "+
			"must produce no derived publication: the retained store's fence cannot retract "+
			"this one, got %q", m.Subject)
	case <-time.After(2 * time.Second):
	}
}

// TestDeniedWillIsNotPublishedUnderAuthPassthrough is the DENY half of the Will
// decision, and until it existed the half was unfalsifiable.
//
// The decision is captured at CONNECT and only read on the teardown path, because
// every discharge site runs after the snapshot is invalidated -- that much is
// covered by the allowed-Will fixture above, which fails if the capture is replaced
// by a live check. Nothing failed if the capture was replaced by the constant TRUE:
// a census plus a mutation run confirmed that `c.willAuthorized = true` survived the
// entire suite, tagged and untagged. The one denied-Will fixture in the tree is an
// upstream-ported cluster test on a broker with no client dialer, so mqttAuthzApplies
// is false there and it never reaches this decision at all.
//
// Both arms run on ONE broker and are triggered the same way, so the control is not
// merely a different test that happens to pass: it is the same mechanism, the same
// trigger, the same connection-loss path, differing only in the Will's subject.
func TestDeniedWillIsNotPublishedUnderAuthPassthrough(t *testing.T) {
	broker, dialed, _, _ := authzBrokerTrackingDials(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 8)
	watcher, err := authzClient(t, broker, "willdeny-watch", "alice", 4)
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	// Both subjects on one subscription: a watcher that could only ever have seen
	// one of them would make the absence below meaningless.
	if tok := watcher.Subscribe("wd/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	// QoS 0 Wills, for the reason the allowed-Will fixture states: a QoS 1/2 Will is
	// carried by its store even when the live fan-out fails, so it would prove
	// nothing about the path that actually publishes.
	connectWillOwner := func(id, willTopic string) pmqtt.Client {
		t.Helper()
		o := pmqtt.NewClientOptions().AddBroker(broker).SetClientID(id).
			SetUsername("alice").SetPassword("pw").SetCleanSession(true).SetProtocolVersion(4).
			SetWill(willTopic, "bye-"+id, 0, false).
			SetAutoReconnect(false).SetConnectTimeout(8 * time.Second)
		c := pmqtt.NewClient(o)
		if tok := c.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
			t.Fatalf("%s connect: %v", id, tok.Error())
		}
		t.Cleanup(func() { c.Disconnect(100) })
		return c
	}

	// "denied.>" is alice's publish deny; "wd/denied" maps to "wd.denied", which is
	// NOT denied -- so the denied arm has to name a subject the ACL actually covers
	// while the watcher can still subscribe to it. It does: the deny is on publish,
	// the watcher's filter is a subscribe.
	denied := connectWillOwner("willdeny-owner", "denied/will")
	allowed := connectWillOwner("willallow-owner", "wd/allowed")

	// A second watcher for the denied subject, because the first one's filter cannot
	// cover it. Subscribing is permitted; publishing is what alice is denied.
	if tok := watcher.Subscribe("denied/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe denied/#: %v", tok.Error())
	}

	// Break each per-client NATS connection: the same trigger the allowed-Will
	// fixture uses, and the one that discharges a Will through the client-free route
	// after the snapshot has already been invalidated.
	for _, id := range []string{"willdeny-owner", "willallow-owner"} {
		nc := dialed(id)
		if nc == nil {
			t.Fatalf("fixture: no per-client NATS connection captured for %s", id)
		}
		nc.Close()
	}
	deadline := time.Now().Add(10 * time.Second)
	for denied.IsConnectionOpen() || allowed.IsConnectionOpen() {
		if time.Now().After(deadline) {
			t.Fatal("losing the per-client NATS connection must close the MQTT connection")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The CONTROL first. Without it, a broker that suppressed every Will -- which is
	// exactly what asking for authorization on the teardown path produces -- would
	// satisfy the assertion that follows.
	var sawAllowed bool
	for waitUntil := time.Now().Add(10 * time.Second); !sawAllowed && time.Now().Before(waitUntil); {
		select {
		case m := <-got:
			if m.Topic() == "denied/will" {
				t.Fatal("a Will on a subject the account DENIES was published: the decision " +
					"captured at CONNECT is not being honoured at the discharge site, and the " +
					"publication rode the full-rights internal client")
			}
			if m.Topic() == "wd/allowed" {
				sawAllowed = true
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !sawAllowed {
		t.Fatal("control: an AUTHORIZED Will must still fire on this trigger; without it the " +
			"denied arm's absence has another cause")
	}

	// The allowed Will has landed, so the denied one had at least as long to. Settle
	// briefly in case the two discharge out of order.
	for waitUntil := time.Now().Add(2 * time.Second); time.Now().Before(waitUntil); {
		select {
		case m := <-got:
			if m.Topic() == "denied/will" {
				t.Fatal("a Will on a subject the account DENIES was published")
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestAuthorizedWillSurvivesAServiceBlip is round 21's finding, driven end to end.
//
// Under auth passthrough every synthetic-record Will publishes through the internal
// account client, and deliverWillRecord's account fence refuses while the service
// connection's identity is unavailable. Those sites write nothing durable -- the only
// durable park is $MQTT_willq, which is exactly what is unreachable then -- so before
// this change the caller logged the refusal and the Will was simply gone. An ordinary
// NATS blip destroyed an AUTHORIZED Will, which is the opposite of what the rest of
// this code says: a deny is permanent and refuses, an outage is an UNKNOWN and must
// defer.
//
// Only the identity is made unavailable here, not the connection. That is deliberate:
// dropping the service connection would also stop the watcher receiving anything, so
// the fixture could not tell "held correctly" from "never published", which is the
// same trap the delayed-Will residual describes.
func TestAuthorizedWillSurvivesAServiceBlip(t *testing.T) {
	broker, dialed, _, srv := authzBrokerTrackingDials(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 4)
	watcher, err := authzClient(t, broker, "blip-watch", "alice", 4)
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	if tok := watcher.Subscribe("blip/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	// QoS 0: no store to carry it, so the live publish is the only path and the loss
	// is total. It is also the case Codex called the clearest regression, because
	// before auth passthrough it fanned out through the still-live per-client
	// connection and now has no fallback at all.
	o := pmqtt.NewClientOptions().AddBroker(broker).SetClientID("blip-owner").
		SetUsername("alice").SetPassword("pw").SetCleanSession(true).SetProtocolVersion(4).
		SetWill("blip/will", "survived", 0, false).
		SetAutoReconnect(false).SetConnectTimeout(8 * time.Second)
	owner := pmqtt.NewClient(o)
	if tok := owner.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		t.Fatalf("will owner connect: %v", tok.Error())
	}
	t.Cleanup(func() { owner.Disconnect(100) })

	// The blip. This is what the service connection's own disconnect handler does,
	// and it is the state every gate and guard reads.
	srv.svcID.mu.Lock()
	account := srv.svcID.account
	srv.svcID.valid = false
	srv.svcID.mu.Unlock()

	nc := dialed("blip-owner")
	if nc == nil {
		t.Fatal("fixture: the per-client NATS connection was not captured, so this test would prove nothing")
	}
	nc.Close()
	for deadline := time.Now().Add(10 * time.Second); owner.IsConnectionOpen(); {
		if time.Now().After(deadline) {
			t.Fatal("losing the per-client NATS connection must close the MQTT connection")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Withheld while the destination is unknown. Publishing it here would be the
	// other failure -- into whatever account the connection is on, which nobody has
	// re-identified.
	select {
	case m := <-got:
		t.Fatalf("the Will was published while its authorized destination was unknown (%q)", m.Payload())
	case <-time.After(1500 * time.Millisecond):
	}

	// The destination returns, unchanged, exactly as the reconnect handler finds it.
	srv.svcID.mu.Lock()
	srv.svcID.account, srv.svcID.valid = account, true
	srv.svcID.mu.Unlock()
	srv.dischargeOwedImmediateWills()

	select {
	case m := <-got:
		if string(m.Payload()) != "survived" {
			t.Fatalf("the Will payload must survive intact, got %q", m.Payload())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("an AUTHORIZED Will refused only because its destination account was momentarily " +
			"unavailable must be published when that account returns: an outage is an UNKNOWN, " +
			"and answering it the way a DENY is answered destroys a Will the client was entitled to")
	}
}

// TestOwedWillKickCoversTheReconnectWindow pins the recheck-and-kick in
// parkOwedImmediateWill, which no end-to-end route can reach.
//
// The window is INSIDE one park call: the reconnect that would discharge an
// obligation can complete between the refusal that produced it and its insertion
// into the queue, and a discharge that ran in that window saw an empty queue. Asking
// again after inserting is what closes it. An end-to-end fixture cannot arrange it --
// once the account is back, a Will simply publishes and never parks at all, which an
// earlier version of this test discovered the hard way -- so the function's contract
// is exercised directly: parking while the destination IS available must publish at
// once rather than wait for an event that has already happened.
func TestOwedWillKickCoversTheReconnectWindow(t *testing.T) {
	broker, _, _, srv := authzBrokerTrackingDials(t, authzConfOneAccount)

	got := make(chan pmqtt.Message, 4)
	watcher, err := authzClient(t, broker, "kick-watch", "alice", 4)
	if err != nil {
		t.Fatalf("watcher: %v", err)
	}
	if tok := watcher.Subscribe("kick/#", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	account, ok := srv.serviceAccountForWill()
	if !ok {
		t.Fatal("fixture: the service account must be established, or parking would refuse for " +
			"the ordinary reason and this test would prove nothing about the kick")
	}
	asm, err := srv.getOrCreateMQTTAccountSessionManager(srv.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}
	rec := &mqttWillOutbox{
		WillID: "kick-" + account, Account: account,
		Topic: "kick/direct", Msg: []byte("kicked"),
	}
	if !srv.parkOwedImmediateWill(asm, rec) {
		t.Fatal("parking must be admitted: the dispatch gate is open and the queue is empty")
	}

	select {
	case m := <-got:
		if string(m.Payload()) != "kicked" {
			t.Fatalf("payload %q", m.Payload())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a Will parked while its destination account was AVAILABLE was not published: " +
			"parking must re-ask after inserting, or an obligation created just after the " +
			"reconnect that would have discharged it waits for the next one")
	}
}

// TestOwedWillQueueHonoursItsByteBoundUnderConcurrentParks is the bound the
// CONFORMANCE residual claims, asserted rather than asserted-about.
//
// The claim is that a broker holds at most mqttOwedWillMaxBytes of Wills across an
// outage. An earlier version of this queue did not honour it under the exact
// workload it exists for: discharge removed the whole batch and reset the charge to
// zero while those records were still live, so concurrent parks were admitted
// against capacity that was still occupied. Many client teardowns during ONE outage
// is not an unusual shape - it is the shape a NATS outage produces.
func TestOwedWillQueueHonoursItsByteBoundUnderConcurrentParks(t *testing.T) {
	broker, _, _, srv := authzBrokerTrackingDials(t, authzConfOneAccount)
	// A client, only so an account manager exists to park against.
	if _, err := authzClient(t, broker, "bound-probe", "alice", 4); err != nil {
		t.Fatalf("probe: %v", err)
	}
	asm, err := srv.getOrCreateMQTTAccountSessionManager(srv.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}

	// The destination goes away, so every park below is refused by the fence and
	// retained rather than published.
	srv.svcID.mu.Lock()
	account := srv.svcID.account
	srv.svcID.valid = false
	srv.svcID.mu.Unlock()

	const each = 64 << 10
	payload := make([]byte, each)
	// Comfortably more than the bound, offered from many goroutines at once: the
	// serialisation, not the arithmetic, is what an unsound version gets wrong.
	const parkers, perParker = 16, 8
	var wg sync.WaitGroup
	var admitted int64
	for i := 0; i < parkers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perParker; j++ {
				rec := &mqttWillOutbox{
					WillID:  fmt.Sprintf("bound-%d-%d", i, j),
					Account: account,
					Topic:   "bound/will",
					Msg:     payload,
				}
				if srv.parkOwedImmediateWill(asm, rec) {
					atomic.AddInt64(&admitted, 1)
				}
			}
		}(i)
	}
	wg.Wait()

	srv.owedWills.mu.Lock()
	held, count, dropped := srv.owedWills.bytes, len(srv.owedWills.owed), srv.owedWills.dropped
	srv.owedWills.mu.Unlock()

	if held > mqttOwedWillMaxBytes {
		t.Fatalf("the queue holds %d bytes, past its %d bound: capacity was handed out against "+
			"records that were still occupying it", held, mqttOwedWillMaxBytes)
	}
	if dropped == 0 {
		t.Fatalf("the fixture offered %d bytes against a %d bound and nothing was refused, so "+
			"the bound was never exercised", parkers*perParker*each, mqttOwedWillMaxBytes)
	}
	if int64(count) != admitted {
		t.Fatalf("the queue holds %d records but %d parks reported success: the charge and the "+
			"contents disagree", count, admitted)
	}

	// And the retention is not a leak: once the destination returns, everything held
	// drains and the charge goes back to zero.
	srv.svcID.mu.Lock()
	srv.svcID.account, srv.svcID.valid = account, true
	srv.svcID.mu.Unlock()
	srv.dischargeOwedImmediateWills()
	srv.owedWills.mu.Lock()
	leftBytes, leftCount := srv.owedWills.bytes, len(srv.owedWills.owed)
	srv.owedWills.mu.Unlock()
	if leftCount != 0 || leftBytes != 0 {
		t.Fatalf("after the destination returned the queue still holds %d records / %d bytes: a "+
			"discharge must uncharge what it delivered", leftCount, leftBytes)
	}
}

// TestOwedWillDischargeRefusedAfterShutdownGateCloses pins that the reconnect-driven
// discharge is admitted like every other Will worker.
//
// A reconnect callback can finish identity discovery long after shutdown closed the
// dispatch gate and after willWorkerWG observed zero. An unadmitted discharge could
// then take the queue out from under discardOwedImmediateWills and publish through
// infrastructure being torn down -- or after shutdown reported completion.
func TestOwedWillDischargeRefusedAfterShutdownGateCloses(t *testing.T) {
	broker, _, _, srv := authzBrokerTrackingDials(t, authzConfOneAccount)
	if _, err := authzClient(t, broker, "gate-probe", "alice", 4); err != nil {
		t.Fatalf("probe: %v", err)
	}
	asm, err := srv.getOrCreateMQTTAccountSessionManager(srv.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}
	srv.svcID.mu.Lock()
	account := srv.svcID.account
	srv.svcID.valid = false
	srv.svcID.mu.Unlock()

	rec := &mqttWillOutbox{WillID: "gate-1", Account: account, Topic: "gate/will", Msg: []byte("held")}
	if !srv.parkOwedImmediateWill(asm, rec) {
		t.Fatal("the Will must be retained while the gate is open")
	}

	// Shutdown Phase 2. Nothing may be admitted behind it.
	srv.closeWillDispatch()

	if srv.parkOwedImmediateWill(asm, &mqttWillOutbox{WillID: "gate-2", Account: account, Topic: "gate/will"}) {
		t.Fatal("parking must be REFUSED once the dispatch gate is closed, or shutdown can never " +
			"reach a state where the queue stops growing")
	}

	// The destination returns and a late reconnect callback fires, through the same
	// entry point the reconnect handler uses. It must not act.
	srv.svcID.mu.Lock()
	srv.svcID.account, srv.svcID.valid = account, true
	srv.svcID.mu.Unlock()
	srv.kickOwedImmediateWills()
	// Settle: a kick that WAS admitted would publish asynchronously, so asserting
	// immediately would pass against the very thing this pins.
	time.Sleep(500 * time.Millisecond)

	srv.owedWills.mu.Lock()
	count := len(srv.owedWills.owed)
	srv.owedWills.mu.Unlock()
	if count != 1 {
		t.Fatalf("a discharge after the gate closed must leave the queue for shutdown to discard, "+
			"but the queue now holds %d records", count)
	}
}

// TestOwedWillReconnectDuringADischargeIsNotLost pins the wake-up a discharge
// request leaves when another one is already running.
//
// The runner takes a batch, drops the queue lock while it publishes, and decides on
// the way out whether another pass is owed. Before the wake flag it decided that on
// "did anything get parked while I ran?" alone -- so a reconnect arriving mid-batch
// was simply discarded: the runner finished, saw nothing new parked, and exited
// while the records it had just KEPT were publishable and still charged. Nothing
// else would come and ask. The Will then waited for an unrelated future park or
// reconnect, or was reported lost at shutdown.
//
// It is driven deterministically rather than by timing: a publish seam holds the
// first pass inside the batch, which is exactly the window, and the second discharge
// is issued while it is held.
func TestOwedWillReconnectDuringADischargeIsNotLost(t *testing.T) {
	broker, _, _, srv := authzBrokerTrackingDials(t, authzConfOneAccount)
	if _, err := authzClient(t, broker, "wake-probe", "alice", 4); err != nil {
		t.Fatalf("probe: %v", err)
	}
	asm, err := srv.getOrCreateMQTTAccountSessionManager(srv.createInternalAccountClient())
	if err != nil {
		t.Fatalf("account manager: %v", err)
	}

	srv.svcID.mu.Lock()
	account := srv.svcID.account
	srv.svcID.valid = false
	srv.svcID.mu.Unlock()

	rec := &mqttWillOutbox{WillID: "wake-1", Account: account, Topic: "wake/will", Msg: []byte("held")}
	if !srv.parkOwedImmediateWill(asm, rec) {
		t.Fatal("the Will must be retained while the destination is away")
	}

	// The first pass reports the destination still unavailable, so the record is
	// KEPT -- and it blocks there, holding the batch, until the test releases it.
	inFlight := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	asm.mu.Lock()
	asm.testPublishWill = func(*mqttWillOutbox) error {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(inFlight)
			<-release
			return fmt.Errorf("%w: held by the fixture", errMQTTWillDestinationUnavailable)
		}
		return nil // the destination is back on the pass the wake-up must trigger
	}
	asm.mu.Unlock()
	t.Cleanup(func() { asm.mu.Lock(); asm.testPublishWill = nil; asm.mu.Unlock() })

	// The destination returns, and a runner starts.
	srv.svcID.mu.Lock()
	srv.svcID.account, srv.svcID.valid = account, true
	srv.svcID.mu.Unlock()
	// Through the PRODUCTION entry point on both sides, which is the whole point:
	// the coalescing that decides whether the second request is remembered lives in
	// the kick, so a first pass started by calling the synchronous function directly
	// would never exercise it. An earlier version of this test did exactly that and
	// a mutation moving the coalescing clear to AFTER the pass survived it.
	srv.kickOwedImmediateWills()
	select {
	case <-inFlight:
	case <-time.After(10 * time.Second):
		t.Fatal("the first discharge pass never reached the publish seam")
	}

	// THE RECONNECT, landing while that runner holds the batch. Nothing is parked
	// here and nothing else will ask again, so this request is the only thing that
	// can cause the retained record to be retried.
	srv.kickOwedImmediateWills()
	close(release)

	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		srv.owedWills.mu.Lock()
		n, held := len(srv.owedWills.owed), srv.owedWills.bytes
		srv.owedWills.mu.Unlock()
		if n == 0 && held == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a discharge requested while another was running was LOST: %d records / %d "+
				"bytes are still held although the destination is established and the runner "+
				"has finished", n, held)
		}
	}
}
