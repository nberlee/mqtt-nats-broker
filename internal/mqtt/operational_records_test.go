package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// The MQTT core's frozen records (docs/OPERATIONS.md, "Records"): each producer site is
// driven and its record read back as JSON, with its message, level, keys and values given
// here as literals rather than read from the definition.

// recordLog is a JSON log the test reads back.
type recordLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *recordLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func captureServerLog(s *Server) *recordLog {
	l := &recordLog{}
	s.log.Store(slog.New(slog.NewJSONHandler(l, nil)))
	return l
}

// records is every record with exactly msg.
func (l *recordLog) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(l.buf.Bytes()))
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("not a JSON record: %q", sc.Text())
		}
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

// requireFrozenRecord waits for a record with exactly msg and requires its level and keys,
// and each field's value where one is given (a nil value: present, a non-empty string).
func (l *recordLog) requireFrozenRecord(t *testing.T, msg, level string, fields map[string]any) map[string]any {
	t.Helper()
	var rs []map[string]any
	deadline := time.Now().Add(10 * time.Second)
	for {
		if rs = l.records(t, msg); len(rs) > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(rs) == 0 {
		t.Fatalf("no record %q", msg)
	}
	m := rs[len(rs)-1]
	if m["level"] != level {
		t.Errorf("%q: level %v, want %s", msg, m["level"], level)
	}
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	want := []string{"time", "level", "msg"}
	for k, v := range fields {
		want = append(want, k)
		got, ok := m[k]
		switch {
		case !ok:
		case v == nil:
			if s, _ := got.(string); s == "" {
				t.Errorf("%q: %s = %#v, want a non-empty string", msg, k, got)
			}
		case got != v:
			t.Errorf("%q: %s = %#v, want %#v", msg, k, got, v)
		}
	}
	sort.Strings(keys)
	sort.Strings(want)
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Errorf("%q: keys %v, want %v", msg, keys, want)
	}
	return m
}

// TestConnectRefusalsAreFrozenRecords drives each CONNECT refusal producer separately.
func TestConnectRefusalsAreFrozenRecords(t *testing.T) {
	broker, s := authzBroker(t, authzConfOneAccount)
	logs := captureServerLog(s)
	svcAccount, ok := s.serviceAccount()
	if !ok {
		t.Fatal("the service account is not established")
	}
	safe := &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: []string{mqttPrefix + ">"}},
		Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
	}

	t.Run("the identity cannot be established", func(t *testing.T) {
		withUserInfoHook(t, func() ([]byte, error) { return nil, errors.New("synthetic identity failure") })
		if _, err := authzClient(t, broker, "refused-unidentified", "alice", 4); err == nil {
			t.Fatal("admitted")
		}
		logs.requireFrozenRecord(t, "MQTT: refusing CONNECT", "WARN",
			map[string]any{"client_id": "refused-unidentified", "reason": "synthetic identity failure"})
	})
	t.Run("the identity is bound to another account", func(t *testing.T) {
		withUserInfoHook(t, func() ([]byte, error) {
			return userInfoBody(t, &natsserver.UserInfo{UserID: "alice", Account: "SOMEWHERE_ELSE", Permissions: safe}), nil
		})
		if _, err := authzClient(t, broker, "refused-elsewhere", "alice", 4); err == nil {
			t.Fatal("admitted")
		}
		logs.requireFrozenRecord(t, "MQTT: refusing CONNECT", "WARN", map[string]any{"client_id": "refused-elsewhere",
			"reason": fmt.Sprintf("identity %q is bound to account %q, not the service account %q", "alice", "SOMEWHERE_ELSE", svcAccount)})
	})
	t.Run("the service account is not established", func(t *testing.T) {
		s.svcID.mu.Lock()
		s.svcID.valid = false
		s.svcID.mu.Unlock()
		t.Cleanup(func() {
			s.svcID.mu.Lock()
			s.svcID.account, s.svcID.valid = svcAccount, true
			s.svcID.mu.Unlock()
		})
		if _, err := authzClient(t, broker, "refused-no-account", "alice", 4); err == nil {
			t.Fatal("admitted")
		}
		logs.requireFrozenRecord(t, "MQTT: refusing CONNECT: the service connection's account is not currently established", "WARN", nil)
	})
}

// TestServiceReconnectOutcomesAreFrozenRecords drives the service connection's reconnect
// handler through each outcome that leaves CONNECTs refused or clients disconnected.
func TestServiceReconnectOutcomesAreFrozenRecords(t *testing.T) {
	_, s := authzBroker(t, authzConfOneAccount)
	logs := captureServerLog(s)
	svcAccount, _ := s.serviceAccount()
	reconnected := s.nrc.Opts.ReconnectedCB
	if reconnected == nil {
		t.Fatal("no reconnect handler on the service connection")
	}

	t.Run("not identified", func(t *testing.T) {
		withUserInfoHook(t, func() ([]byte, error) { return nil, errors.New("synthetic reconnect identity failure") })
		reconnected(s.nrc)
		logs.requireFrozenRecord(t, "MQTT: the service connection reconnected but could not be identified; no CONNECT is accepted",
			"ERROR", map[string]any{"err": "synthetic reconnect identity failure"})
	})
	t.Run("dropped while being identified", func(t *testing.T) {
		nc, err := nats.Connect(s.nrc.ConnectedUrl(), nats.UserInfo("svc", "svcpw"))
		if err != nil {
			t.Fatal(err)
		}
		withUserInfoHook(t, func() ([]byte, error) {
			nc.Close()
			return userInfoBody(t, &natsserver.UserInfo{UserID: "svc", Account: svcAccount}), nil
		})
		reconnected(nc)
		logs.requireFrozenRecord(t, "MQTT: the service connection dropped while it was being re-identified; the account stays unknown", "WARN", nil)
	})
	t.Run("into another account", func(t *testing.T) {
		withUserInfoHook(t, func() ([]byte, error) {
			return userInfoBody(t, &natsserver.UserInfo{UserID: "svc", Account: "ANOTHER"}), nil
		})
		reconnected(s.nrc)
		logs.requireFrozenRecord(t, "MQTT: the service connection reconnected into another account; disconnecting every auth-passthrough client",
			"ERROR", map[string]any{"account": "ANOTHER", "previous_account": svcAccount})
	})
}

// TestInitializationRetryIsAFrozenRecord: a retried initialization writes its record with
// the attempt as a number.
func TestInitializationRetryIsAFrozenRecord(t *testing.T) {
	ns := runJetStreamServerHA(t)
	setStartupBackoff(t, time.Minute)
	entered := make(chan struct{})
	var once sync.Once
	setStartupBackoffHook(t, func(int, error) { once.Do(func() { close(entered) }) })
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if isObserverCreate(ccr) {
			return lostResponse()
		}
		return err
	}))
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = 15 * time.Second })
	logs := captureServerLog(s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	select {
	case <-entered:
	case <-time.After(60 * time.Second):
		cancel()
		t.Fatal("the initialization was never retried")
	}
	logs.requireFrozenRecord(t, "MQTT initialization did not complete; retrying", "WARN",
		map[string]any{"attempt": float64(1), "err": nil})
	cancel()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("Start did not return")
	}
}
