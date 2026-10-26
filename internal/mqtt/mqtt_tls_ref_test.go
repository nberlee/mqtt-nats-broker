//go:build mqtt_tls_reference

// Copyright 2020-2026 The NATS Authors
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

// Upstream TLS tests kept as an unbuilt reference; they need TLSConfigOpts /
// GenTLSConfig / cert-file-backed config (o.Accounts, o.MQTT.TLSMap) that the
// standalone shim does not implement, and the referenced cert fixtures
// (../test/configs/certs/...) do not exist in this repo. See CONTRIBUTING.md.

package mqtt

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
)

func testMQTTDefaultTLSOptions(t *testing.T, verify bool) *Options {
	t.Helper()
	o := testMQTTDefaultOptions()
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/server-cert.pem",
		KeyFile:  "../test/configs/certs/server-key.pem",
		CaFile:   "../test/configs/certs/ca.pem",
		Verify:   verify,
	}
	var err error
	o.MQTT.TLSConfig, err = GenTLSConfig(tc)
	o.MQTT.TLSTimeout = 2.0
	if err != nil {
		t.Fatalf("Error creating tls config: %v", err)
	}
	return o
}

func TestMQTTTLS(t *testing.T) {
	o := testMQTTDefaultTLSOptions(t, false)
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{tls: true}, o.MQTT.Host, o.MQTT.Port, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	c = nil
	testMQTTShutdownServer(s)

	// Force client cert verification
	o = testMQTTDefaultTLSOptions(t, true)
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	c, _, err = testMQTTConnectRetryWithError(t, &mqttConnInfo{tls: true}, o.MQTT.Host, o.MQTT.Port, 0)
	if err == nil || c != nil {
		if c != nil {
			c.Close()
		}
		t.Fatal("Handshake expected to fail since client did not provide cert")
	}

	// Add client cert.
	tc := &TLSConfigOpts{
		CertFile: "../test/configs/certs/client-cert.pem",
		KeyFile:  "../test/configs/certs/client-key.pem",
	}
	tlsc, err := GenTLSConfig(tc)
	if err != nil {
		t.Fatalf("Error generating tls config: %v", err)
	}
	tlsc.InsecureSkipVerify = true
	c, _, err = testMQTTConnectRetryWithError(t, &mqttConnInfo{
		tls:  true,
		tlsc: tlsc,
	}, o.MQTT.Host, o.MQTT.Port, 0)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	c = nil
	testMQTTShutdownServer(s)

	// Lower TLS timeout so low that we should fail
	o.MQTT.TLSTimeout = 0.001
	s = testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	nc, err := net.Dial("tcp", net.JoinHostPort(o.MQTT.Host, fmt.Sprintf("%d", o.MQTT.Port)))
	if err != nil {
		t.Fatalf("Unable to create tcp connection to mqtt port: %v", err)
	}
	defer nc.Close()
	time.Sleep(100 * time.Millisecond)
	tlsConn := tls.Client(nc, tlsc)
	tlsConn.SetDeadline(time.Now().Add(time.Second))
	if err := tlsConn.Handshake(); err == nil {
		t.Fatal("Expected failure, did not get one")
	}
}

func TestMQTTTLSVerifyAndMap(t *testing.T) {
	accName := "MyAccount"
	acc := NewAccount(accName)
	certUserName := "CN=example.com,OU=NATS.io"
	users := []*User{{Username: certUserName, Account: acc}}

	for _, test := range []struct {
		name        string
		filtering   bool
		provideCert bool
	}{
		{"no filtering, client provides cert", false, true},
		{"no filtering, client does not provide cert", false, false},
		{"filtering, client provides cert", true, true},
		{"filtering, client does not provide cert", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			o.Host = "localhost"
			o.Accounts = []*Account{acc}
			o.Users = users
			if test.filtering {
				o.Users[0].AllowedConnectionTypes = testCreateAllowedConnectionTypes([]string{jwt.ConnectionTypeStandard, jwt.ConnectionTypeMqtt})
			}
			tc := &TLSConfigOpts{
				CertFile: "../test/configs/certs/tlsauth/server.pem",
				KeyFile:  "../test/configs/certs/tlsauth/server-key.pem",
				CaFile:   "../test/configs/certs/tlsauth/ca.pem",
				Verify:   true,
			}
			tlsc, err := GenTLSConfig(tc)
			if err != nil {
				t.Fatalf("Error creating tls config: %v", err)
			}
			o.MQTT.TLSConfig = tlsc
			o.MQTT.TLSTimeout = 2.0
			o.MQTT.TLSMap = true
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			testMQTTEnableJSForAccount(t, s, accName)

			tlscc := &tls.Config{}
			if test.provideCert {
				tc := &TLSConfigOpts{
					CertFile: "../test/configs/certs/tlsauth/client.pem",
					KeyFile:  "../test/configs/certs/tlsauth/client-key.pem",
				}
				var err error
				tlscc, err = GenTLSConfig(tc)
				if err != nil {
					t.Fatalf("Error generating tls config: %v", err)
				}
			}
			tlscc.InsecureSkipVerify = true
			if test.provideCert {
				tlscc.MinVersion = tls.VersionTLS13
			}
			mc, r, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{
				cleanSess: true,
				tls:       true,
				tlsc:      tlscc,
			}, o.MQTT.Host, o.MQTT.Port, 0)
			if !test.provideCert {
				if err == nil {
					t.Fatal("Expected error, did not get one")
				} else if !strings.Contains(err.Error(), "bad certificate") && !strings.Contains(err.Error(), "certificate required") {
					t.Fatalf("Unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Error reading: %v", err)
			}
			defer mc.Close()
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			var c *client
			s.mu.Lock()
			for _, sc := range s.clients {
				sc.mu.Lock()
				if sc.isMqtt() {
					c = sc
				}
				sc.mu.Unlock()
				if c != nil {
					break
				}
			}
			s.mu.Unlock()
			if c == nil {
				t.Fatal("Client not found")
			}

			var uname string
			var accname string
			c.mu.Lock()
			uname = c.opts.Username
			if c.acc != nil {
				accname = c.acc.GetName()
			}
			c.mu.Unlock()
			if uname != certUserName {
				t.Fatalf("Expected username %q, got %q", certUserName, uname)
			}
			if accname != accName {
				t.Fatalf("Expected account %q, got %v", accName, accname)
			}
		})
	}
}
