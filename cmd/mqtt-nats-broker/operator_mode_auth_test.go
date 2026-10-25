package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// The operator (JWT) deployment docs/AUTHENTICATION.md documents, built in-process.
//
// Accounts: SYS; APP, the JetStream account the broker's streams and its MQTT users live
// in; OTHER, a second account; AUTH, which holds the auth callout. The server's
// default_sentinel is a bearer user in AUTH with no permissions, so a connection that
// carries only a user name and password (which is all auth passthrough sends) goes to
// AUTH's callout. The responder below is a TEST FIXTURE standing in for an operator's
// authorization service: it checks a user name and password against a table and issues
// a user JWT, signed by the target account, carrying that user's permissions.

// mqttReservedDenies are the subjects every MQTT identity is denied in both directions.
var mqttReservedDenies = []string{"$MQTT.>", "$JS.>", "$KV.>", "$O.>"}

// calloutUser is one row of the fixture responder's table.
type calloutUser struct {
	password string
	account  string // "APP" or "OTHER": its key signs the issued user JWT, placing it there
	pubDeny  []string
	subDeny  []string
}

// calloutRequest is what the responder saw for one authorization request.
type calloutRequest struct {
	user, password, presentedJWT string
}

type operatorFixture struct {
	srv       *server.Server
	app       string // APP's public key, the account name USER.INFO reports
	other     string
	appKP     nkeys.KeyPair
	svcCreds  string // the broker's service identity, a user of APP
	mu        sync.Mutex
	requests  []calloutRequest
	responder *nats.Conn
}

func (f *operatorFixture) requested(user string) (calloutRequest, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.requests {
		if r.user == user {
			return r, true
		}
	}
	return calloutRequest{}, false
}

func mustKey(t *testing.T, create func() (nkeys.KeyPair, error)) (nkeys.KeyPair, string) {
	t.Helper()
	kp, err := create()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return kp, pub
}

func mustEncode(t *testing.T, c interface {
	Encode(nkeys.KeyPair) (string, error)
}, kp nkeys.KeyPair) string {
	t.Helper()
	s, err := c.Encode(kp)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// userCreds writes a .creds file for a new user of the account acc signs for.
func userCreds(t *testing.T, dir, name string, acc nkeys.KeyPair, edit func(*jwt.UserClaims)) (path, userJWT string, ukp nkeys.KeyPair) {
	t.Helper()
	ukp, upub := mustKey(t, nkeys.CreateUser)
	uc := jwt.NewUserClaims(upub)
	uc.Name = name
	if edit != nil {
		edit(uc)
	}
	userJWT = mustEncode(t, uc, acc)
	seed, err := ukp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	creds, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(dir, name+".creds")
	if err := os.WriteFile(path, creds, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, userJWT, ukp
}

func startOperatorFixture(t *testing.T, users map[string]calloutUser) *operatorFixture {
	t.Helper()
	dir := t.TempDir()
	okp, opub := mustKey(t, nkeys.CreateOperator)
	operatorJWT := mustEncode(t, jwt.NewOperatorClaims(opub), okp)

	account := func(name string, edit func(*jwt.AccountClaims)) (nkeys.KeyPair, string, string) {
		kp, pub := mustKey(t, nkeys.CreateAccount)
		ac := jwt.NewAccountClaims(pub)
		ac.Name = name
		if edit != nil {
			edit(ac)
		}
		return kp, pub, mustEncode(t, ac, okp)
	}
	_, sysPub, sysJWT := account("SYS", nil)
	appKP, appPub, appJWT := account("APP", func(ac *jwt.AccountClaims) {
		ac.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1}
	})
	otherKP, otherPub, otherJWT := account("OTHER", nil)
	authKP, authPub := mustKey(t, nkeys.CreateAccount)

	// The responder's own identity: a user of AUTH, named in AUTH's callout.
	responderCreds, responderJWT, _ := userCreds(t, dir, "auth-responder", authKP, nil)
	rc, err := jwt.DecodeUserClaims(responderJWT)
	if err != nil {
		t.Fatal(err)
	}
	authClaims := jwt.NewAccountClaims(authPub)
	authClaims.Name = "AUTH"
	authClaims.EnableExternalAuthorization(rc.Subject)
	authClaims.Authorization.AllowedAccounts.Add(appPub, otherPub)
	authJWT := mustEncode(t, authClaims, okp)

	// default_sentinel: a bearer user of AUTH that may do nothing; a connection without a
	// JWT presents it, and so is sent to AUTH's callout.
	_, sentinelJWT, _ := userCreds(t, dir, "sentinel", authKP, func(uc *jwt.UserClaims) {
		uc.BearerToken = true
		uc.Pub.Deny.Add(">")
		uc.Sub.Deny.Add(">")
	})
	// The broker's service identity: a user of APP with full rights in it.
	svcCreds, _, _ := userCreds(t, dir, "mqtt-broker", appKP, nil)

	conf := filepath.Join(dir, "nats.conf")
	body := fmt.Sprintf(`
listen: "127.0.0.1:-1"
server_name: operator-mode
jetstream { store_dir: %q, limits { default_max_consumers: -1 } }
operator: %s
system_account: %s
resolver: MEM
resolver_preload: {
  %s: %s
  %s: %s
  %s: %s
  %s: %s
}
default_sentinel: %s
`, filepath.Join(dir, "js"), operatorJWT, sysPub, sysPub, sysJWT, appPub, appJWT, otherPub, otherJWT, authPub, authJWT, sentinelJWT)
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(conf)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoSigs = true
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })

	f := &operatorFixture{srv: srv, app: appPub, other: otherPub, appKP: appKP, svcCreds: svcCreds}
	signers := map[string]nkeys.KeyPair{"APP": appKP, "OTHER": otherKP}
	f.responder, err = nats.Connect(srv.ClientURL(), nats.UserCredentials(responderCreds))
	if err != nil {
		t.Fatalf("responder connect: %v", err)
	}
	t.Cleanup(f.responder.Close)
	respond := func(m *nats.Msg, req *jwt.AuthorizationRequestClaims, userJWT, errMsg string) {
		rc := jwt.NewAuthorizationResponseClaims(req.UserNkey)
		rc.Audience = req.Server.ID
		rc.Jwt, rc.Error = userJWT, errMsg
		tok, err := rc.Encode(authKP)
		if err != nil {
			t.Errorf("encode authorization response: %v", err)
			return
		}
		_ = m.Respond([]byte(tok))
	}
	if _, err := f.responder.Subscribe("$SYS.REQ.USER.AUTH", func(m *nats.Msg) {
		req, err := jwt.DecodeAuthorizationRequestClaims(string(m.Data))
		if err != nil {
			t.Errorf("decode authorization request: %v", err)
			return
		}
		co := req.ConnectOptions
		f.mu.Lock()
		f.requests = append(f.requests, calloutRequest{user: co.Username, password: co.Password, presentedJWT: co.JWT})
		f.mu.Unlock()
		u, ok := users[co.Username]
		if co.Username == "" || !ok || co.Password != u.password {
			respond(m, req, "", "not authorized")
			return
		}
		uc := jwt.NewUserClaims(req.UserNkey)
		uc.Name = co.Username
		uc.Pub.Deny.Add(mqttReservedDenies...)
		uc.Pub.Deny.Add(u.pubDeny...)
		uc.Sub.Deny.Add(mqttReservedDenies...)
		uc.Sub.Deny.Add(u.subDeny...)
		ujwt, err := uc.Encode(signers[u.account])
		if err != nil {
			t.Errorf("encode issued user: %v", err)
			return
		}
		respond(m, req, ujwt, "")
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.responder.Flush(); err != nil {
		t.Fatal(err)
	}
	return f
}

// TestOperatorModeAuthCallout is the witness for the operator (JWT) example in
// docs/AUTHENTICATION.md. Every refusal is paired with an admitted control, and every
// refusal attributed to the server or the broker is checked against what the fixture
// responder saw, so a fixture rejection cannot pass for one.
func TestOperatorModeAuthCallout(t *testing.T) {
	f := startOperatorFixture(t, map[string]calloutUser{
		"alice": {password: "alicepass", account: "APP",
			pubDeny: []string{"alice.denied", "alice.denied.>"}, subDeny: []string{"alice.secret", "alice.secret.>"}},
		"anonymous": {password: "anonpass", account: "APP", pubDeny: []string{"anon.denied", "anon.denied.>"}},
		"mallory":   {password: "mallorypass", account: "OTHER"},
	})
	logs := recordBrokerLog(t)

	// W1: the service connects with a .creds file for a user of APP, and the broker serves.
	cfg := config.Default()
	cfg.NATSCreds = f.svcCreds
	cfg.DefaultUser, cfg.DefaultPass = "anonymous", "anonpass"
	addr := startExampleBroker(t, f.srv, cfg)

	// W2 and W3: alice is admitted through the callout and held to the permissions it
	// issued: her own topic round-trips, a denied topic and the JetStream API are refused.
	got := make(chan string, 4)
	alice, ca, err := v5Connect(t, addr, "alice", "alice", "alicepass", func(pr paho.PublishReceived) (bool, error) {
		got <- pr.Packet.Topic
		return true, nil
	})
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("alice through the callout: %v %+v", err, ca)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// W3b: a subscribe the issued JWT denies is refused beside one it allows.
	// alice also subscribes to the topic she may not publish on, so a refused
	// publication that was forwarded anyway would arrive here.
	sa, _ := alice.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{
		{Topic: "alice/secret/#", QoS: 1},
		{Topic: "alice/ok", QoS: 1},
		{Topic: "alice/denied", QoS: 1},
	}})
	if sa == nil || len(sa.Reasons) != 3 || sa.Reasons[0] != 0x80 || sa.Reasons[1] != 1 || sa.Reasons[2] != 1 {
		t.Fatalf("alice's SUBACK = %+v, want [0x80 1 1]", sa)
	}
	if rc := publishReason(t, alice, "alice/ok"); rc != 0 {
		t.Fatalf("alice/ok: PUBACK 0x%02x, want 0", rc)
	}
	select {
	case topic := <-got:
		if topic != "alice/ok" {
			t.Fatalf("received %q", topic)
		}
	case <-ctx.Done():
		t.Fatal("alice's allowed publication never arrived")
	}
	for _, topic := range []string{"alice/denied", "$JS/API/INFO"} {
		if rc := publishReason(t, alice, topic); rc != 0x87 {
			t.Errorf("alice publishing %s: PUBACK 0x%02x, want 0x87", topic, rc)
		}
	}
	select {
	case topic := <-got:
		t.Fatalf("a refused publication was delivered: %s", topic)
	case <-time.After(500 * time.Millisecond):
	}

	// W7: an MQTT client without credentials is the configured default identity, issued
	// by the callout, and holds exactly its rights.
	anonGot := make(chan string, 4)
	anon, ca, err := v5Connect(t, addr, "anon", "", "", func(pr paho.PublishReceived) (bool, error) {
		anonGot <- pr.Packet.Topic
		return true, nil
	})
	if err != nil || ca.ReasonCode != 0 {
		t.Fatalf("anonymous client: %v %+v", err, ca)
	}
	if sa, err := anon.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "anon/#", QoS: 1}}}); err != nil {
		t.Fatalf("anonymous subscribe: %v %+v", err, sa)
	}
	if rc := publishReason(t, anon, "anon/ok"); rc != 0 {
		t.Fatalf("anon/ok: PUBACK 0x%02x, want 0", rc)
	}
	select {
	case topic := <-anonGot:
		if topic != "anon/ok" {
			t.Fatalf("anonymous client received %q", topic)
		}
	case <-ctx.Done():
		t.Fatal("the anonymous client's allowed publication never arrived")
	}
	if rc := publishReason(t, anon, "anon/denied"); rc != 0x87 {
		t.Fatalf("anon/denied: PUBACK 0x%02x, want 0x87", rc)
	}
	select {
	case topic := <-anonGot:
		t.Fatalf("a refused publication was delivered: %s", topic)
	case <-time.After(500 * time.Millisecond):
	}

	// W4: the callout places mallory in OTHER. The identity is valid there (control), and
	// the broker refuses it: its USER.INFO reply is answered in OTHER, never reaches the
	// service connection in APP, and the request times out.
	t.Run("another account", func(t *testing.T) {
		if acc := natsUserAccount(t, f.srv.ClientURL(), nats.UserInfo("mallory", "mallorypass")); acc != f.other {
			t.Fatalf("mallory's own USER.INFO names %q, want OTHER (%s)", acc, f.other)
		}
		if _, ca, err := v5Connect(t, addr, "mallory", "mallory", "mallorypass", nil); err == nil && ca.ReasonCode == 0 {
			t.Fatal("an identity the callout placed in another account was admitted")
		}
		waitFor(t, 5*time.Second, func() bool {
			return logs.logged(slog.LevelWarn, `refusing CONNECT for client "mallory": $SYS.REQ.USER.INFO: nats: timeout`)
		})
	})

	// W5: a user JWT in the password is forwarded as an ordinary password. The JWT is
	// valid (control: it authenticates a direct NATS connection as a bearer user of APP);
	// through the broker the server presents the sentinel and hands the JWT to the
	// callout as a password, and the fixture, which does not know jwtuser, refuses it.
	t.Run("JWT in the password", func(t *testing.T) {
		dir := t.TempDir()
		_, bearerJWT, bkp := userCreds(t, dir, "bearer", f.appKP, func(uc *jwt.UserClaims) {
			uc.BearerToken = true
			uc.Pub.Deny.Add(mqttReservedDenies...)
			uc.Sub.Deny.Add(mqttReservedDenies...)
		})
		seed, _ := bkp.Seed()
		if acc := natsUserAccount(t, f.srv.ClientURL(), nats.UserJWTAndSeed(bearerJWT, string(seed))); acc != f.app {
			t.Fatalf("the bearer JWT's own USER.INFO names %q, want APP", acc)
		}
		if _, ca, err := v5Connect(t, addr, "jwtuser", "jwtuser", bearerJWT, nil); err == nil && ca.ReasonCode == 0 {
			t.Fatal("a CONNECT carrying a JWT as its password was admitted")
		}
		r, ok := f.requested("jwtuser")
		if !ok {
			t.Fatal("the callout never saw jwtuser: the server did not take the callout path")
		}
		if r.password != bearerJWT {
			t.Fatal("the callout did not receive the JWT as the password")
		}
		if r.presentedJWT == bearerJWT || r.presentedJWT == "" {
			t.Fatalf("the server presented %q as the client's JWT, want the default sentinel", r.presentedJWT)
		}
	})
}

// TestOperatorModeServiceNeedsItsCredentials is W1's control: without the .creds file
// the service connection carries no JWT, reaches the callout through the sentinel with
// no credentials, and the responder refuses it, so the broker does not start.
func TestOperatorModeServiceNeedsItsCredentials(t *testing.T) {
	f := startOperatorFixture(t, map[string]calloutUser{})
	cfg := config.Default()
	cfg.NATSURLs = f.srv.ClientURL()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	be, err := backend.Connect(ctx, cfg, nil, slog.New(slog.DiscardHandler))
	if err == nil {
		be.Close()
		t.Fatal("the service connected with no credentials")
	}
	if _, ok := f.requested(""); !ok {
		t.Fatal("the callout never saw the credential-free service connection")
	}
}
