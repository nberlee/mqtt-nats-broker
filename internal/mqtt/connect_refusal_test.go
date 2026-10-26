package mqtt

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// A refused CONNECT is told which refusal it was before the close (F-061). These fixtures
// read the CONNACK off a raw socket, so the code is the byte on the wire and not a client
// library's reading of it, and every refusal arm sits beside an accepted control on the same
// broker: a refusal assertion alone passes against a broker that refuses everyone.
//
// The identity seam (mqttTestUserInfoHookV) is process-wide, so the tests that use it are
// not parallel.

// testConnectPacketV5WithCredentials is a minimal MQTT 5.0 CONNECT with a user name and
// password and no properties. An empty user sends neither flag.
func testConnectPacketV5WithCredentials(clientID, user, pass string) []byte {
	flags := mqttConnFlagCleanSession
	if user != _EMPTY_ {
		flags |= mqttConnFlagUsernameFlag | mqttConnFlagPasswordFlag
	}
	body := newMQTTWriter(0)
	body.WriteBytes(mqttProtoName)
	body.WriteByte(mqttProtoLevel5)
	body.WriteByte(flags)
	body.WriteUint16(30)
	body.WriteVarInt(0) // CONNECT properties
	body.WriteString(clientID)
	if user != _EMPTY_ {
		body.WriteString(user)
		body.WriteBytes([]byte(pass))
	}
	pkt := newMQTTWriter(0)
	pkt.WriteByte(mqttPacketConnect)
	pkt.WriteVarInt(body.Len())
	pkt.Write(body.Bytes())
	return pkt.Bytes()
}

// testConnectPacketV311 is the 3.1.1 CONNECT; an empty user sends no credentials.
func testConnectPacketV311(clientID, user, pass string) []byte {
	if user == _EMPTY_ {
		return testConnectPacket(clientID, 30)
	}
	return testConnectPacketWithCredentials(clientID, user, pass)
}

// readConnAckCode connects at proto, sends the CONNECT and returns the CONNACK's return or
// reason code. For a refusal it then requires the close.
func readConnAckCode(t *testing.T, broker string, proto byte, clientID, user, pass string) byte {
	t.Helper()
	addr := strings.TrimPrefix(broker, "tcp://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	pkt := testConnectPacketV311(clientID, user, pass)
	if proto == mqttProtoLevel5 {
		pkt = testConnectPacketV5WithCredentials(clientID, user, pass)
	}
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	r := bufio.NewReader(conn)
	head, err := r.ReadByte()
	if err != nil {
		t.Fatalf("no CONNACK for %q (proto %d): %v", clientID, proto, err)
	}
	if head&mqttPacketMask != mqttPacketConnectAck {
		t.Fatalf("expected CONNACK, got packet 0x%02x", head)
	}
	remaining, mult := 0, 1
	for {
		b, err := r.ReadByte()
		if err != nil {
			t.Fatalf("reading CONNACK length: %v", err)
		}
		remaining += int(b&0x7f) * mult
		if b&0x80 == 0 {
			break
		}
		mult *= 128
	}
	body := make([]byte, remaining)
	if _, err := r.Read(body); err != nil || remaining < 2 {
		t.Fatalf("reading CONNACK body (%d bytes): %v", remaining, err)
	}
	if body[0] != 0 {
		t.Fatalf("session present %d on a fresh clean-session CONNECT", body[0])
	}
	code := body[1]
	if code != mqttConnAckRCConnectionAccepted {
		if _, err := r.ReadByte(); err == nil {
			t.Fatalf("bytes after a refusal CONNACK: the connection must close")
		} else {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("connection still open after a refusal CONNACK")
			}
		}
	}
	return code
}

// TestRefusedConnectIsToldWhy is F-061's witness through the real auth-passthrough route,
// one row per refusal class, each at both protocol versions.
func TestRefusedConnectIsToldWhy(t *testing.T) {
	oneAccount, _ := authzBroker(t, authzConfOneAccount)
	twoAccounts, _ := authzBroker(t, authzConfTwoAccounts)

	safe := &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: []string{mqttPrefix + ">"}},
		Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
	}
	for _, tc := range []struct {
		name        string
		broker      string
		user, pass  string
		hook        func(t *testing.T) []byte
		want311, w5 byte
	}{
		{
			name: "a valid identity is accepted (control)", broker: oneAccount, user: "alice", pass: "pw",
			want311: mqttConnAckRCConnectionAccepted, w5: mqttReasonSuccess,
		},
		{
			name: "a wrong password", broker: oneAccount, user: "alice", pass: "wrong",
			want311: mqttConnAckRCNotAuthorized, w5: mqttReasonBadUserOrPassword,
		},
		{
			// No user name: the dial runs under whatever the deployment supplies for an
			// anonymous client, which is not the client's credential to have got wrong.
			name: "no user name, refused", broker: oneAccount,
			want311: mqttConnAckRCNotAuthorized, w5: mqttReasonNotAuthorized,
		},
		{
			// The real two-account deployment cannot reach the comparison: mallory's
			// identity reply lands in her own account, so the request goes unanswered.
			name: "an identity request that goes unanswered", broker: twoAccounts, user: "mallory", pass: "pw",
			want311: mqttConnAckRCServerUnavailable, w5: mqttReasonServerUnavailable,
		},
		{
			name: "an identity in another account", broker: oneAccount, user: "alice", pass: "pw",
			hook: func(t *testing.T) []byte {
				return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Account: "SOMEWHERE_ELSE", Permissions: safe})
			},
			want311: mqttConnAckRCNotAuthorized, w5: mqttReasonNotAuthorized,
		},
		{
			name: "an undecodable identity answer", broker: oneAccount, user: "alice", pass: "pw",
			hook:    func(*testing.T) []byte { return []byte("not json") },
			want311: mqttConnAckRCNotAuthorized, w5: mqttReasonNotAuthorized,
		},
		{
			name: "an identity answer naming no identity", broker: oneAccount, user: "alice", pass: "pw",
			hook:    func(*testing.T) []byte { return []byte("{}") },
			want311: mqttConnAckRCNotAuthorized, w5: mqttReasonNotAuthorized,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.hook != nil {
				withUserInfoHook(t, func() ([]byte, error) { return tc.hook(t), nil })
			}
			id := strings.ReplaceAll(tc.name, " ", "-")
			if got := readConnAckCode(t, tc.broker, mqttProtoLevel, id+"-311", tc.user, tc.pass); got != tc.want311 {
				t.Errorf("3.1.1 CONNACK return code 0x%02x, want 0x%02x", got, tc.want311)
			}
			if got := readConnAckCode(t, tc.broker, mqttProtoLevel5, id+"-5", tc.user, tc.pass); got != tc.w5 {
				t.Errorf("5.0 CONNACK reason code 0x%02x, want 0x%02x", got, tc.w5)
			}
		})
	}
}

// TestRefusalDisarmsTheUnadoptedConnectionsHandlers pins why the refusal CONNACK reaches
// the wire. The per-client NATS connection a refusal never adopts carries the handlers that
// tear the MQTT connection down when an adopted one goes away; closing it with them armed
// fired the closed handler, which closed the MQTT socket asynchronously under the CONNACK.
// The seam runs immediately before that close and reads both handlers back from nats.go: they
// must already be gone, which fails deterministically if the disarm is removed, with no
// race against the callback to win or lose.
func TestRefusalDisarmsTheUnadoptedConnectionsHandlers(t *testing.T) {
	broker, _ := authzBroker(t, authzConfOneAccount)
	withUserInfoHook(t, func() ([]byte, error) {
		return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Account: "SOMEWHERE_ELSE"}), nil
	})
	type seen struct{ closed, disconnected bool }
	got := make(chan seen, 4)
	hook := func(nc *nats.Conn) {
		got <- seen{closed: nc.ClosedHandler() != nil, disconnected: nc.DisconnectErrHandler() != nil}
	}
	mqttTestRefusalCloseV.Store(&hook)
	t.Cleanup(func() { mqttTestRefusalCloseV.Store(nil) })

	if code := readConnAckCode(t, broker, mqttProtoLevel5, "disarm", "alice", "pw"); code != mqttReasonNotAuthorized {
		t.Fatalf("5.0 CONNACK reason code 0x%02x, want 0x%02x", code, mqttReasonNotAuthorized)
	}
	select {
	case s := <-got:
		if s.closed || s.disconnected {
			t.Fatalf("the unadopted connection was closed with its handlers still armed (closed=%v, disconnect=%v): "+
				"its closed handler closes the MQTT socket under the refusal CONNACK", s.closed, s.disconnected)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the refusal never reached the unadopted connection's close")
	}
}

// TestAuthorizationRefusalClassification drives authorizeClient directly for the branches a
// wire fixture cannot place: the refusal value is the classification, and a CONNACK is only
// its projection.
func TestAuthorizationRefusalClassification(t *testing.T) {
	newClient := func(t *testing.T, s *Server, user string) *client {
		broker, peer := net.Pipe()
		t.Cleanup(func() { broker.Close(); peer.Close() })
		c := &client{srv: s, nc: broker, rawConn: broker, mqtt: &mqtt{cid: "classify"}}
		c.opts.Username, c.opts.Password = user, "pw"
		return c
	}
	check := func(t *testing.T, got mqttConnectRefusal, ok bool, want mqttConnectRefusal) {
		t.Helper()
		if ok {
			t.Fatal("the CONNECT was authorized")
		}
		if got != want {
			t.Fatalf("refusal %+v, want %+v", got, want)
		}
	}

	t.Run("the service account is not established", func(t *testing.T) {
		_, s := authzBroker(t, authzConfOneAccount)
		s.nrc.Close()
		got, ok := s.authorizeClient(newClient(t, s, "alice"))
		check(t, got, ok, mqttRefusalServerUnavailable)
	})

	t.Run("the per-client connection is down before discovery", func(t *testing.T) {
		_, s := authzBroker(t, authzConfOneAccount)
		url := s.nrc.ConnectedUrl()
		s.dialClient = func(_, user, pass string) (*nats.Conn, error) {
			nc, err := nats.Connect(url, nats.UserInfo(user, pass))
			if err != nil {
				return nil, err
			}
			nc.Close()
			return nc, nil
		}
		got, ok := s.authorizeClient(newClient(t, s, "alice"))
		check(t, got, ok, mqttRefusalServerUnavailable)
	})

	t.Run("the per-client connection drops during discovery", func(t *testing.T) {
		_, s := authzBroker(t, authzConfOneAccount)
		url := s.nrc.ConnectedUrl()
		dialed := make(chan *nats.Conn, 1)
		s.dialClient = func(_, user, pass string) (*nats.Conn, error) {
			nc, err := nats.Connect(url, nats.UserInfo(user, pass))
			if err == nil {
				dialed <- nc
			}
			return nc, err
		}
		withUserInfoHook(t, func() ([]byte, error) {
			(<-dialed).Close()
			return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Account: "APP"}), nil
		})
		got, ok := s.authorizeClient(newClient(t, s, "alice"))
		check(t, got, ok, mqttRefusalServerUnavailable)
	})

	t.Run("a refused dial", func(t *testing.T) {
		_, s := authzBroker(t, authzConfOneAccount)
		got, ok := s.authorizeClient(newClient(t, s, "alice-not-a-user"))
		check(t, got, ok, mqttRefusalBadCredentials)
		got, ok = s.authorizeClient(newClient(t, s, _EMPTY_))
		check(t, got, ok, mqttRefusalNotAuthorized)
	})
}
