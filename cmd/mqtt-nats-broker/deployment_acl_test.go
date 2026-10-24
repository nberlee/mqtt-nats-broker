package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// requireMQTTCannotAdministerJetStream attacks the broker's streams as an anonymous MQTT
// client at addr: an MQTT topic maps onto a NATS subject, so $JS/API/STREAM/DELETE/x is a
// stream DELETE sent as the default user. It seeds a retained message first, so a purge
// is observable, and closes the attack with a QoS 1 publish on the same connection whose
// PUBACK means the broker has processed the QoS 0 attempts before it. Both streams must
// be intact afterwards; svc is a service-user connection to read them with.
func requireMQTTCannotAdministerJetStream(t *testing.T, addr string, svc *nats.Conn) {
	t.Helper()
	js, err := jetstream.New(svc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seeder, ca, err := v5Connect(t, addr, "acl-seeder", "", "", nil)
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("seeder connect: %v %+v", err, ca)
	}
	if _, err := seeder.Publish(ctx, &paho.Publish{Topic: "acl/retained", QoS: 1, Retain: true, Payload: []byte("keep")}); err != nil {
		t.Fatalf("seed a retained message: %v", err)
	}
	retainedBefore := streamMsgs(ctx, t, js, "$MQTT_rmsgs")
	if retainedBefore == 0 {
		t.Fatal("the seeded retained message is not in $MQTT_rmsgs")
	}

	c, ca, err := v5Connect(t, addr, "acl-attacker", "", "", nil)
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("attacker connect: %v %+v", err, ca)
	}
	for _, topic := range []string{"$JS/API/STREAM/PURGE/$MQTT_rmsgs", "$JS/API/STREAM/DELETE/$MQTT_sess"} {
		if _, err := c.Publish(ctx, &paho.Publish{Topic: topic, QoS: 0, Payload: []byte("{}")}); err != nil {
			t.Fatalf("send %s: %v", topic, err)
		}
	}
	if _, err := c.Publish(ctx, &paho.Publish{Topic: "acl/marker", QoS: 1, Payload: []byte("after")}); err != nil {
		t.Fatalf("the QoS 1 marker after the attempts was not acknowledged: %v", err)
	}
	// The server applies an admitted DELETE asynchronously to the request; give it time.
	time.Sleep(time.Second)
	if _, err := js.Stream(ctx, "$MQTT_sess"); err != nil {
		t.Fatalf("an anonymous MQTT publish deleted the broker's session stream: %v", err)
	}
	if after := streamMsgs(ctx, t, js, "$MQTT_rmsgs"); after < retainedBefore {
		t.Fatalf("an anonymous MQTT publish purged $MQTT_rmsgs: %d messages, had %d", after, retainedBefore)
	}
}

func streamMsgs(ctx context.Context, t *testing.T, js jetstream.JetStream, name string) uint64 {
	t.Helper()
	s, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatalf("stream %s: %v", name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("stream %s info: %v", name, err)
	}
	return info.State.Msgs
}

// startConfiguredServer starts a server from a shipped configuration file with only what
// a test process needs changed: ephemeral ports, a temporary store, no signals, no
// monitoring port, and JetStream domain d so domain-qualified API subjects are real.
func startConfiguredServer(t *testing.T, path string, standalone bool) *server.Server {
	t.Helper()
	opts, err := server.ProcessConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	opts.Host, opts.Port, opts.HTTPPort = "127.0.0.1", -1, 0
	opts.StoreDir = t.TempDir()
	opts.NoSigs = true
	opts.JetStreamDomain = "d"
	if standalone {
		// A cluster example's routes name hosts that do not exist here; the
		// permissions under test do not depend on them.
		opts.Cluster, opts.Routes = server.ClusterOpts{}, nil
	}
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	return srv
}

// TestShippedACLsKeepMQTTIdentitiesOffBrokerSubjects checks every ACL the repository
// ships, as nats-server parses it: the identity MQTT clients act as can neither publish
// nor subscribe on the broker's namespace, the JetStream API (local and
// domain-qualified), or the KV and object-store data subjects; it can still use ordinary
// subjects and ask the server who it is (auth passthrough's identity request); and the
// service identity is answered by the JetStream API. Each check has its own connection:
// a connection's last error is sticky.
func TestShippedACLsKeepMQTTIdentitiesOffBrokerSubjects(t *testing.T) {
	t.Setenv("MQTTNB_NATS_USER", "svc")
	t.Setenv("MQTTNB_NATS_PASS", "svcpass")
	t.Setenv("MQTTNB_DEFAULT_USER", "anon")
	t.Setenv("MQTTNB_DEFAULT_PASS", "anonpass")
	t.Setenv("MQTTNB_EMBEDDED_SERVER_NAME", "acl-check")
	t.Setenv("MQTTNB_EMBEDDED_ROUTE_USER", "route")
	t.Setenv("MQTTNB_EMBEDDED_ROUTE_PASS", "routepass")
	t.Setenv("MQTTNB_EMBEDDED_ROUTES", "[nats-route://route:routepass@127.0.0.1:1]")
	for _, tc := range []struct {
		path               string
		standalone         bool
		svcUser, svcPass   string
		mqttUser, mqttPass string
	}{
		{"../../embedded/nats.conf", false, "svc", "svcpass", "anon", "anonpass"},
		{"../../embedded/nats-cluster.conf", true, "svc", "svcpass", "anon", "anonpass"},
		{"../../test/conformance/nats.conf", false, "service", "service", "mqtt", "mqtt"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			// A configuration the embedded image ships must pass the broker's own
			// enforcement as written (the cluster example's route authentication included).
			if strings.HasPrefix(tc.path, "../../embedded/") {
				opts, err := server.ProcessConfigFile(tc.path)
				if err != nil {
					t.Fatal(err)
				}
				if err := enforceEmbeddedOptions(opts, ""); err != nil {
					t.Fatalf("the shipped %s fails the broker's enforcement: %v", tc.path, err)
				}
			}
			srv := startConfiguredServer(t, tc.path, tc.standalone)
			dial := func(user, pass string, opts ...nats.Option) *nats.Conn {
				t.Helper()
				nc, err := nats.Connect(srv.ClientURL(), append([]nats.Option{nats.UserInfo(user, pass)}, opts...)...)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(nc.Close)
				return nc
			}
			for _, subj := range []string{"$MQTT.sess.x", "$JS.API.INFO", "$JS.API.STREAM.DELETE.$MQTT_sess", "$JS.d.API.INFO", "$JS.ACK.s.c.1.1.1.1.0", "$KV.b.k", "$O.b.C.x"} {
				pub := dial(tc.mqttUser, tc.mqttPass)
				_ = pub.Publish(subj, []byte("x"))
				_ = pub.Flush()
				time.Sleep(50 * time.Millisecond)
				if !errors.Is(pub.LastError(), nats.ErrPermissionViolation) {
					t.Errorf("%s may publish %s (last error %v)", tc.mqttUser, subj, pub.LastError())
				}
				sub := dial(tc.mqttUser, tc.mqttPass, nats.PermissionErrOnSubscribe(true))
				s, err := sub.SubscribeSync(subj)
				if err == nil {
					_, err = s.NextMsg(time.Second)
				}
				if !errors.Is(err, nats.ErrPermissionViolation) {
					t.Errorf("%s may subscribe %s (%v)", tc.mqttUser, subj, err)
				}
			}

			ok := dial(tc.mqttUser, tc.mqttPass)
			s, err := ok.SubscribeSync("app.x")
			if err != nil {
				t.Fatal(err)
			}
			_ = ok.Publish("app.x", []byte("x"))
			if _, err := s.NextMsg(2 * time.Second); err != nil {
				t.Errorf("%s cannot use an ordinary subject: %v", tc.mqttUser, err)
			}
			who := dial(tc.mqttUser, tc.mqttPass)
			if _, err := who.Request("$SYS.REQ.USER.INFO", nil, 2*time.Second); err != nil {
				t.Errorf("%s cannot ask who it is, which auth passthrough needs: %v", tc.mqttUser, err)
			}
			svc := dial(tc.svcUser, tc.svcPass)
			for _, subj := range []string{"$JS.API.INFO", "$JS.d.API.INFO"} {
				if _, err := svc.Request(subj, nil, 5*time.Second); err != nil {
					t.Errorf("the service identity is not answered on %s: %v", subj, err)
				}
			}
		})
	}
}

// TestRecommendedACLKeepsMQTTClientsOffTheJetStreamAPI runs the broker in external mode
// against the repository's recommended deployment ACL (test/conformance/nats.conf) and
// attacks the broker's streams through MQTT.
func TestRecommendedACLKeepsMQTTClientsOffTheJetStreamAPI(t *testing.T) {
	srv := startConfiguredServer(t, "../../test/conformance/nats.conf", false)
	cfg := config.Default()
	cfg.NATSURLs = srv.ClientURL()
	cfg.NATSUser, cfg.NATSPass = "service", "service"
	cfg.DefaultUser, cfg.DefaultPass = "mqtt", "mqtt"
	log := slog.New(slog.DiscardHandler)
	be, err := backend.Connect(context.Background(), cfg, nil, log)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(be.Close)
	addr := reservedAddr(t)
	cfg.MQTTHost, cfg.MQTTPort = "127.0.0.1", mustPort(t, addr)
	ms, err := serveMQTT(cfg, be, mqttOptions(cfg, nil), log)
	if err != nil {
		t.Fatalf("serveMQTT: %v", err)
	}
	t.Cleanup(func() { <-ms.BeginShutdown() })
	svc, err := nats.Connect(srv.ClientURL(), nats.UserInfo("service", "service"))
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	requireMQTTCannotAdministerJetStream(t, addr, svc)
}
