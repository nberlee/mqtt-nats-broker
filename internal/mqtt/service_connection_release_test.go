// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

// The service connection is the caller's: a server that stops, or fails to start, hands it
// back as it was handed over — no broker subscriptions left on it, and its own disconnect and
// reconnect handlers back in place. Left behind, both keep the stopped server reachable from a
// connection that outlives it.

import (
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// serviceReleaseNATS starts a nats-server from conf and returns its URL.
func serviceReleaseNATS(t *testing.T, conf string) string {
	t.Helper()
	path := t.TempDir() + "/svc.conf"
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoSigs = true
	opts.StoreDir = t.TempDir()
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// callerConnection is a service connection as a caller hands it over: its own handlers
// installed and one subscription of its own.
type callerConnection struct {
	nc          *nats.Conn
	disconnect  nats.ConnErrHandler
	reconnect   nats.ConnHandler
	ownSubCount int
}

func newCallerConnection(t *testing.T, url string) *callerConnection {
	t.Helper()
	cc := &callerConnection{
		disconnect: func(*nats.Conn, error) {},
		reconnect:  func(*nats.Conn) {},
	}
	nc, err := nats.Connect(url, nats.UserInfo("svc", "svcpw"),
		nats.DisconnectErrHandler(cc.disconnect), nats.ReconnectHandler(cc.reconnect))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if _, err := nc.Subscribe("caller.own", func(*nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	cc.nc, cc.ownSubCount = nc, nc.NumSubscriptions()
	return cc
}

func sameFunc(a, b any) bool {
	return reflect.ValueOf(a).Pointer() == reflect.ValueOf(b).Pointer()
}

// assertHandedBack checks the connection is as the caller handed it over.
func (cc *callerConnection) assertHandedBack(t *testing.T, when string) {
	t.Helper()
	if got := cc.nc.NumSubscriptions(); got != cc.ownSubCount {
		t.Errorf("%s: the service connection holds %d subscriptions, the caller's own are %d", when, got, cc.ownSubCount)
	}
	if !sameFunc(cc.nc.DisconnectErrHandler(), cc.disconnect) {
		t.Errorf("%s: the caller's disconnect handler was not restored", when)
	}
	if !sameFunc(cc.nc.ReconnectHandler(), cc.reconnect) {
		t.Errorf("%s: the caller's reconnect handler was not restored", when)
	}
}

func passthroughServer(cc *callerConnection, url string, port int) *Server {
	s := NewServer(cc.nc, &Options{MQTT: MQTTOpts{Host: "127.0.0.1", Port: port}}, "svc-release-node")
	s.SetClientDialer(func(_, user, pass string) (*nats.Conn, error) {
		return nats.Connect(url, nats.UserInfo(user, pass))
	})
	return s
}

func TestShutdownReleasesServiceConnection(t *testing.T) {
	url := serviceReleaseNATS(t, authzConfOneAccount)

	t.Run("after a successful start", func(t *testing.T) {
		cc := newCallerConnection(t, url)
		s := passthroughServer(cc, url, -1)
		if err := s.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		// The control: the broker really did install subscriptions and handlers on it.
		if cc.nc.NumSubscriptions() <= cc.ownSubCount {
			t.Fatal("the running broker holds no subscriptions on the service connection; " +
				"the release below would prove nothing")
		}
		if sameFunc(cc.nc.ReconnectHandler(), cc.reconnect) {
			t.Fatal("the running broker did not arm its identity handlers")
		}
		select {
		case <-s.BeginShutdown():
		case <-time.After(30 * time.Second):
			t.Fatal("shutdown did not complete")
		}
		cc.assertHandedBack(t, "after shutdown")
	})

	t.Run("after failed starts", func(t *testing.T) {
		cc := newCallerConnection(t, url)

		// Identity discovery fails: the handlers were armed before the request.
		failIdentity := func() ([]byte, error) { return nil, errors.New("identity unavailable") }
		mqttTestUserInfoHookV.Store(&failIdentity)
		for i := 0; i < 3; i++ {
			if err := passthroughServer(cc, url, -1).Start(); err == nil {
				t.Fatal("start succeeded with identity discovery failing")
			}
			cc.assertHandedBack(t, fmt.Sprintf("identity failure %d", i+1))
		}
		mqttTestUserInfoHookV.Store(nil)

		// The listener cannot bind: identity was established and the handlers armed.
		taken, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer taken.Close()
		port := taken.Addr().(*net.TCPAddr).Port
		if err := passthroughServer(cc, url, port).Start(); err == nil {
			t.Fatal("start succeeded on a port that is taken")
		}
		cc.assertHandedBack(t, "listener failure")
	})

	t.Run("after a failed eager init", func(t *testing.T) {
		// An account without JetStream: identity and the listener succeed, the eager account
		// manager init cannot create its streams.
		noJS := serviceReleaseNATS(t, strings.Replace(authzConfOneAccount,
			"  APP: {\n    jetstream: enabled\n", "  APP: {\n", 1))
		cc := newCallerConnection(t, noJS)
		s := passthroughServer(cc, noJS, -1)
		s.opts.MQTT.JSAPITimeout = time.Second
		err := s.Start()
		if err == nil {
			s.Shutdown()
			t.Fatal("start succeeded on an account without JetStream")
		}
		// Past identity and the listener, or this subtest is a copy of the one above.
		if strings.Contains(err.Error(), "auth passthrough") || s.mqtt.listener == nil {
			t.Fatalf("start failed before the eager init: %v", err)
		}
		cc.assertHandedBack(t, "eager init failure")
	})
}
