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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pahov3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
)

// The runtime witness for the permission contract (docs/ARCHITECTURE.md, "The permission
// contract"). A broker runs against a nats-server whose service identity holds EXACTLY the
// contract's grant for the cell (auth passthrough on or off, a JetStream domain or none),
// plus the application subjects this test uses, and serves every scenario below.
//
// What it shows, and what it does not (the contract section states the same): the asserted
// scenario outcomes under the stated grant, and, in TestEveryContractGrantIsExercised, an
// attributed denial when each grant entry is removed in turn, so each entry is used. It
// does NOT prove that every write the broker makes stays inside the grant: the trailing
// violation check is best-effort (a write still queued when the connection closes can be
// missed; nothing it reports is false), and an unobserved asynchronous operation can leave
// the grant without failing anything here.

const (
	contractServiceUser = "svc"
	contractMQTTUser    = "mqtt"
	// The positive controls: subjects outside every grant, used only to prove the collector
	// sees and attributes both kinds of violation.
	controlPubSubject = "control.pub"
	controlSubSubject = "control.sub"
)

// grantEntry is one permission in one direction.
type grantEntry struct {
	publish bool
	subject string
}

func (g grantEntry) String() string {
	if g.publish {
		return "publish " + g.subject
	}
	return "subscribe " + g.subject
}

// contractCell is one deployment shape the contract covers.
type contractCell struct {
	passthrough bool
	domain      string
}

func (c contractCell) String() string {
	return fmt.Sprintf("passthrough=%t/domain=%q", c.passthrough, c.domain)
}

func contractCells() []contractCell {
	return []contractCell{{true, ""}, {true, "hub"}, {false, ""}, {false, "hub"}}
}

// The application subjects the scenarios use. The MQTT topic spBv1.0/... escapes its dot,
// so its NATS subject starts spBv1//0.
var (
	appSubjects         = []string{"t", "t.>"}
	sparkplugBirth      = "spBv1//0.>"
	sparkplugCertParent = "$sparkplug.certificates"
	sparkplugCert       = "$sparkplug.certificates.>"
)

// serviceGrant is the cell's complete service grant: the contract's fixed part, and the
// application subjects the service connection carries in this cell (Wills and the derived
// Sparkplug certificate always; every client's publications and subscriptions without
// passthrough).
func (c contractCell) serviceGrant() []grantEntry {
	var g []grantEntry
	for _, s := range mqtt.ServicePublish(c.domain, c.passthrough) {
		g = append(g, grantEntry{true, s})
	}
	for _, s := range append(slices.Clone(appSubjects), sparkplugCert) {
		g = append(g, grantEntry{true, s})
	}
	for _, s := range mqtt.ServiceSubscribe() {
		g = append(g, grantEntry{false, s})
	}
	if !c.passthrough {
		g = append(g, grantEntry{true, sparkplugBirth})
		for _, s := range append(slices.Clone(appSubjects), sparkplugCertParent, sparkplugCert) {
			g = append(g, grantEntry{false, s})
		}
	}
	return g
}

// confList renders subjects as a NATS configuration array.
func confList(subjects []string) string {
	q := make([]string, len(subjects))
	for i, s := range subjects {
		q[i] = strconv.Quote(s)
	}
	return "[" + strings.Join(q, ", ") + "]"
}

// confDirection renders one direction of a grant; an emptied grant is written as an explicit
// deny-all, because nats-server reads an EMPTY allow list as allow-all.
func confDirection(allow []string) string {
	if len(allow) == 0 {
		return `{ deny: [">"] }`
	}
	return "{ allow: " + confList(allow) + " }"
}

// contractConfig is the nats-server configuration for a cell whose service grant is grant.
// clientSub replaces the MQTT identity's subscribe allows when it is not nil; an entry may be
// queue-scoped ("<subject> <queue>").
func contractConfig(grant []grantEntry, clientSub []string) string {
	var pub, sub []string
	for _, g := range grant {
		if g.publish {
			pub = append(pub, g.subject)
		} else {
			sub = append(sub, g.subject)
		}
	}
	denies := append(slices.Clone(mqtt.ClientDeniedSubjects), mqtt.ClientRecommendedDeniedSubjects...)
	clientPub := append(slices.Clone(appSubjects), sparkplugBirth, sparkplugCert, mqtt.ClientRequiredPublish)
	if clientSub == nil {
		clientSub = append(slices.Clone(appSubjects), sparkplugCertParent, sparkplugCert)
	}
	return fmt.Sprintf(`
jetstream { limits { default_max_consumers: -1 } }
accounts {
  APP {
    jetstream: enabled
    users: [
      { user: %q, password: "svcpass", permissions: { publish: %s, subscribe: %s } }
      { user: %q, password: "mqttpass", permissions: {
          publish: { allow: %s, deny: %s }
          subscribe: { allow: %s, deny: %s } } }
    ]
  }
}
`, contractServiceUser, confDirection(pub), confDirection(sub),
		contractMQTTUser, confList(clientPub), confList(denies), confList(clientSub), confList(denies))
}

// violationLog collects nats-server's permission violations. The server writes each with
// the connection's prefix, which carries "<account>/user:<name>", so a line is attributed
// to the identity that caused it.
type violationLog struct {
	mu    sync.Mutex
	lines []string
}

var violationSubject = regexp.MustCompile(`(?:Publish|Subscription) Violation - (?:Subject|Reply) "([^"]+)"`)

func (v *violationLog) record(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	if strings.Contains(line, "Violation") {
		v.mu.Lock()
		v.lines = append(v.lines, line)
		v.mu.Unlock()
	}
}

func (v *violationLog) Noticef(format string, args ...any) { v.record(format, args...) }
func (v *violationLog) Warnf(format string, args ...any)   { v.record(format, args...) }
func (v *violationLog) Fatalf(format string, args ...any)  { v.record(format, args...) }
func (v *violationLog) Errorf(format string, args ...any)  { v.record(format, args...) }
func (v *violationLog) Debugf(format string, args ...any)  {}
func (v *violationLog) Tracef(format string, args ...any)  {}

// violation is one attributed denial.
type violation struct {
	user, subject, line string
}

func (v *violationLog) violations() []violation {
	v.mu.Lock()
	defer v.mu.Unlock()
	var out []violation
	for _, l := range v.lines {
		m := violationSubject.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		user := ""
		if i := strings.Index(l, "/user:"); i >= 0 {
			rest := l[i+len("/user:"):]
			if j := strings.IndexByte(rest, '"'); j >= 0 {
				user = rest[:j]
			}
		}
		out = append(out, violation{user, m[1], l})
	}
	return out
}

// waitViolation polls for an attributed violation on a subject the filter matches.
func (v *violationLog) waitViolation(user, filter string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		for _, x := range v.violations() {
			if x.user == user && server.SubjectMatchesFilter(x.subject, filter) {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// contractRig is one cell's server and broker.
type contractRig struct {
	cell    contractCell
	srv     *server.Server
	log     *violationLog
	addr    string
	be      *backend.Backend
	ms      *mqtt.Server
	started error
}

// startContractRig starts the server with grant and a broker in the cell's mode. A broker
// that cannot start is recorded in started rather than failing the test: a removal arm
// expects some startups to fail.
func startContractRig(t *testing.T, cell contractCell, grant []grantEntry, startup time.Duration) *contractRig {
	t.Helper()
	return startContractRigWithClient(t, cell, grant, nil, startup)
}

// startContractRigWithClient is startContractRig with the MQTT identity's subscribe allows
// replaced (see contractConfig).
func startContractRigWithClient(t *testing.T, cell contractCell, grant []grantEntry, clientSub []string, startup time.Duration) *contractRig {
	t.Helper()
	conf := contractConfig(grant, clientSub)
	file := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(file, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(file)
	if err != nil {
		t.Fatalf("the contract configuration does not parse: %v\n%s", err, conf)
	}
	opts.Host, opts.Port, opts.HTTPPort = "127.0.0.1", -1, 0
	opts.StoreDir, opts.NoSigs = t.TempDir(), true
	opts.JetStream, opts.JetStreamDomain = true, cell.domain
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	r := &contractRig{cell: cell, srv: srv, log: &violationLog{}}
	srv.SetLoggerV2(r.log, false, false, false)
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })

	cfg := testConfig()
	cfg.NATSURLs = srv.ClientURL()
	cfg.NATSUser, cfg.NATSPass = contractServiceUser, "svcpass"
	cfg.AuthPassthrough = cell.passthrough
	if cell.passthrough {
		cfg.DefaultUser, cfg.DefaultPass = contractMQTTUser, "mqttpass"
	}
	cfg.JSDomain = cell.domain
	ctx, cancel := context.WithTimeout(context.Background(), startup)
	defer cancel()
	log := slog.New(slog.DiscardHandler)
	be, err := backend.Connect(ctx, cfg, nil, log)
	if err != nil {
		r.started = err
		return r
	}
	r.be = be
	t.Cleanup(be.Close)
	r.addr = reservedAddr(t)
	cfg.MQTTHost, cfg.MQTTPort = "127.0.0.1", mustPort(t, r.addr)
	ms, err := serveMQTT(ctx, cfg, be, mqttOptions(cfg, nil), log)
	if err != nil {
		r.started = err
		return r
	}
	r.ms = ms
	t.Cleanup(func() { r.stop() })
	return r
}

// stop shuts the broker down and closes its service connection; idempotent.
func (r *contractRig) stop() {
	if r.ms != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = r.ms.ShutdownContext(ctx)
		cancel()
		r.ms = nil
	}
	if r.be != nil {
		r.be.Close()
		r.be = nil
	}
}

// requireControls proves the collector sees and attributes both kinds of violation.
func (r *contractRig) requireControls(t *testing.T) {
	t.Helper()
	nc, err := nats.Connect(r.srv.ClientURL(), nats.UserInfo(contractServiceUser, "svcpass"), serviceInbox)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_ = nc.Publish(controlPubSubject, []byte("x"))
	_, _ = nc.SubscribeSync(controlSubSubject)
	_ = nc.Flush()
	for _, s := range []string{controlPubSubject, controlSubSubject} {
		if !r.log.waitViolation(contractServiceUser, s, 5*time.Second) {
			t.Fatalf("the violation collector did not see the control on %s: the witness cannot observe a denial", s)
		}
	}
}

// requireNoViolations is the best-effort trailing check: after the broker's shutdown and
// the close of its service connection, any attributed violation other than the controls
// fails. It can miss a write still queued at the close; it cannot report a false one.
func (r *contractRig) requireNoViolations(t *testing.T) {
	t.Helper()
	r.stop()
	for _, v := range r.log.violations() {
		if v.subject == controlPubSubject || v.subject == controlSubSubject {
			continue
		}
		if v.user == contractServiceUser || v.user == contractMQTTUser {
			t.Errorf("%s was denied %s under the contract grant: %s", v.user, v.subject, v.line)
		}
	}
}

// contractClient is one MQTT 5.0 connection with its received messages.
type contractClient struct {
	c    *paho.Client
	conn net.Conn
	ca   *paho.Connack
	got  chan *paho.Publish
	// held keeps what expect read while waiting for something else: deliveries on
	// different subscriptions are not ordered against each other.
	held []*paho.Publish
}

func dialContract(addr, id string, cp paho.Connect) (*contractClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	cc := &contractClient{conn: conn, got: make(chan *paho.Publish, 64)}
	cc.c = paho.NewClient(paho.ClientConfig{Conn: conn, OnPublishReceived: []func(paho.PublishReceived) (bool, error){
		func(pr paho.PublishReceived) (bool, error) {
			select {
			case cc.got <- pr.Packet:
			default:
			}
			return true, nil
		},
	}})
	cp.ClientID = id
	if cp.KeepAlive == 0 {
		cp.KeepAlive = 30
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca, err := cc.c.Connect(ctx, &cp)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if ca.ReasonCode != 0 {
		conn.Close()
		return nil, fmt.Errorf("CONNACK reason 0x%02x", ca.ReasonCode)
	}
	cc.ca = ca
	return cc, nil
}

func (cc *contractClient) close() {
	_ = cc.c.Disconnect(&paho.Disconnect{})
	cc.conn.Close()
}

func (cc *contractClient) subscribe(filter string, qos byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sa, err := cc.c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos}}})
	if err != nil {
		return err
	}
	if len(sa.Reasons) != 1 || sa.Reasons[0] > 2 {
		return fmt.Errorf("SUBACK for %s: %v", filter, sa.Reasons)
	}
	return nil
}

func (cc *contractClient) publish(topic string, qos byte, retain bool, payload string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cc.c.Publish(ctx, &paho.Publish{Topic: topic, QoS: qos, Retain: retain, Payload: []byte(payload)})
	if err != nil {
		return err
	}
	if resp != nil && resp.ReasonCode >= 0x80 {
		return fmt.Errorf("publish %s: reason 0x%02x", topic, resp.ReasonCode)
	}
	return nil
}

// expect waits for a message on topic with payload.
func (cc *contractClient) expect(topic, payload string, d time.Duration) error {
	match := func(p *paho.Publish) bool { return p.Topic == topic && string(p.Payload) == payload }
	if i := slices.IndexFunc(cc.held, match); i >= 0 {
		cc.held = slices.Delete(cc.held, i, i+1)
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case p := <-cc.got:
			if match(p) {
				return nil
			}
			cc.held = append(cc.held, p)
		case <-timer.C:
			return fmt.Errorf("no %q on %s within %s", payload, topic, d)
		}
	}
}

func u32(v uint32) *uint32 { return &v }

// contractScenario is one broker behavior the contract must allow, with its delivery
// asserted. It returns an error rather than failing, so a removal arm can run it under a
// grant that is meant to break it.
type contractScenario struct {
	name string
	run  func(r *contractRig, wait time.Duration) error
}

func contractScenarios() map[string]contractScenario {
	all := []contractScenario{
		{"startup", func(r *contractRig, _ time.Duration) error { return r.started }},
		{"qos0-1-2 and the parent level", func(r *contractRig, wait time.Duration) error {
			sub, err := dialContract(r.addr, "sc-sub", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer sub.close()
			if err := sub.subscribe("t/#", 2); err != nil {
				return err
			}
			pub, err := dialContract(r.addr, "sc-pub", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer pub.close()
			for i, topic := range []string{"t", "t/a", "t/b"} {
				if err := pub.publish(topic, byte(i), false, "q"+strconv.Itoa(i)); err != nil {
					return err
				}
				if err := sub.expect(topic, "q"+strconv.Itoa(i), wait); err != nil {
					return err
				}
			}
			return nil
		}},
		{"mqtt 3.1.1", func(r *contractRig, wait time.Duration) error {
			got := make(chan string, 4)
			o := pahov3.NewClientOptions().AddBroker("tcp://" + r.addr).SetClientID("sc-v3").SetCleanSession(true).SetAutoReconnect(false)
			c := pahov3.NewClient(o)
			if tok := c.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
				return fmt.Errorf("3.1.1 connect: %v", tok.Error())
			}
			defer c.Disconnect(100)
			if tok := c.Subscribe("t/v3", 1, func(_ pahov3.Client, m pahov3.Message) { got <- string(m.Payload()) }); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
				return fmt.Errorf("3.1.1 subscribe: %v", tok.Error())
			}
			if tok := c.Publish("t/v3", 1, false, "v3"); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
				return fmt.Errorf("3.1.1 publish: %v", tok.Error())
			}
			select {
			case <-got:
				return nil
			case <-time.After(wait):
				return errors.New("3.1.1 QoS 1 round trip not delivered")
			}
		}},
		{"retained", func(r *contractRig, wait time.Duration) error {
			pub, err := dialContract(r.addr, "sc-ret", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer pub.close()
			if err := pub.publish("t/r", 1, true, "kept"); err != nil {
				return err
			}
			late, err := dialContract(r.addr, "sc-ret-late", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer late.close()
			if err := late.subscribe("t/r", 1); err != nil {
				return err
			}
			if err := late.expect("t/r", "kept", wait); err != nil {
				return err
			}
			return pub.publish("t/r", 1, true, "")
		}},
		{"wills", func(r *contractRig, wait time.Duration) error {
			watch, err := dialContract(r.addr, "sc-will-watch", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer watch.close()
			if err := watch.subscribe("t/#", 1); err != nil {
				return err
			}
			now, err := dialContract(r.addr, "sc-will-now", paho.Connect{CleanStart: true,
				WillMessage: &paho.WillMessage{Topic: "t", QoS: 1, Payload: []byte("gone")}})
			if err != nil {
				return err
			}
			later, err := dialContract(r.addr, "sc-will-later", paho.Connect{CleanStart: true,
				Properties:     &paho.ConnectProperties{SessionExpiryInterval: u32(30)},
				WillMessage:    &paho.WillMessage{Topic: "t/w", QoS: 1, Payload: []byte("late")},
				WillProperties: &paho.WillProperties{WillDelayInterval: u32(1)}})
			if err != nil {
				return err
			}
			now.conn.Close()
			later.conn.Close()
			if err := watch.expect("t", "gone", wait); err != nil {
				return err
			}
			return watch.expect("t/w", "late", wait+2*time.Second)
		}},
		{"shared", func(r *contractRig, wait time.Duration) error {
			m, err := dialContract(r.addr, "sc-share", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer m.close()
			for _, f := range []string{"$share/g0/t/s0", "$share/g1/t/s1"} {
				if err := m.subscribe(f, 1); err != nil {
					return err
				}
			}
			pub, err := dialContract(r.addr, "sc-share-pub", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer pub.close()
			for i, topic := range []string{"t/s0", "t/s1"} {
				if err := pub.publish(topic, byte(i), false, "s"); err != nil {
					return err
				}
				if err := m.expect(topic, "s", wait); err != nil {
					return err
				}
			}
			return nil
		}},
		{"resume, unsubscribe and discard", func(r *contractRig, wait time.Duration) error {
			keep := paho.Connect{CleanStart: false, Properties: &paho.ConnectProperties{SessionExpiryInterval: u32(60)}}
			s, err := dialContract(r.addr, "sc-resume", keep)
			if err != nil {
				return err
			}
			for _, f := range []string{"t/p1", "t/p2", "t/u"} {
				if err := s.subscribe(f, 2); err != nil {
					s.close()
					return err
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err = s.c.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{"t/u"}})
			cancel()
			if err != nil {
				s.close()
				return err
			}
			s.close()
			pub, err := dialContract(r.addr, "sc-resume-pub", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer pub.close()
			for i, topic := range []string{"t/u", "t/p1", "t/p2"} {
				if err := pub.publish(topic, byte(min(i+1, 2)), false, "held"); err != nil {
					return err
				}
			}
			s, err = dialContract(r.addr, "sc-resume", keep)
			if err != nil {
				return err
			}
			if !s.ca.SessionPresent {
				s.close()
				return errors.New("the resumed session is not present")
			}
			for _, topic := range []string{"t/p1", "t/p2"} {
				if err := s.expect(topic, "held", wait); err != nil {
					s.close()
					return err
				}
			}
			s.close()
			d, err := dialContract(r.addr, "sc-resume", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer d.close()
			if d.ca.SessionPresent {
				return errors.New("a clean start found the session present")
			}
			return nil
		}},
		{"sparkplug", func(r *contractRig, wait time.Duration) error {
			certs, err := dialContract(r.addr, "sc-certs", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer certs.close()
			if err := certs.subscribe("$sparkplug/certificates/#", 0); err != nil {
				return err
			}
			node, err := dialContract(r.addr, "sc-node", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer node.close()
			const birth = "spBv1.0/g/NBIRTH/e"
			if err := node.publish(birth, 0, false, "birth"); err != nil {
				return err
			}
			if err := certs.expect("$sparkplug/certificates/"+birth, "birth", wait); err != nil {
				return err
			}
			// A QoS 1 publication on the same connection completes after the NBIRTH's
			// retained store, so the replay below is not a race with it.
			if err := node.publish("t/fence", 1, false, "fence"); err != nil {
				return err
			}
			late, err := dialContract(r.addr, "sc-certs-late", paho.Connect{CleanStart: true})
			if err != nil {
				return err
			}
			defer late.close()
			if err := late.subscribe("$sparkplug/certificates/#", 0); err != nil {
				return err
			}
			return late.expect("$sparkplug/certificates/"+birth, "birth", wait)
		}},
	}
	m := map[string]contractScenario{}
	for _, s := range all {
		m[s.name] = s
	}
	return m
}

// contractScenarioOrder is the order the full witness runs them in.
var contractScenarioOrder = []string{"startup", "qos0-1-2 and the parent level", "mqtt 3.1.1", "retained", "wills", "shared", "resume, unsubscribe and discard", "sparkplug"}

func TestBrokerServesUnderThePermissionContract(t *testing.T) {
	scenarios := contractScenarios()
	for _, cell := range contractCells() {
		t.Run(cell.String(), func(t *testing.T) {
			r := startContractRig(t, cell, cell.serviceGrant(), 30*time.Second)
			if r.started != nil {
				t.Fatalf("the broker does not start under the contract grant: %v\n%v", r.started, r.log.violations())
			}
			r.requireControls(t)
			for _, name := range contractScenarioOrder {
				if err := scenarios[name].run(r, 10*time.Second); err != nil {
					t.Errorf("%s: %v", name, err)
				}
			}
			r.requireNoViolations(t)
		})
	}
}

// exercisedBy names the shortest scenario that uses a grant entry in a cell.
func exercisedBy(cell contractCell, g grantEntry) string {
	switch {
	case g.subject == sparkplugCert || g.subject == sparkplugCertParent || g.subject == sparkplugBirth:
		return "sparkplug"
	case g.publish && g.subject == "$JS.ACK.>":
		return "qos0-1-2 and the parent level"
	case g.publish && g.subject == "t":
		return "wills"
	case g.publish && g.subject == "t.>":
		return "wills"
	case !g.publish && (g.subject == "t" || g.subject == "t.>"):
		return "qos0-1-2 and the parent level"
	case g.publish && g.subject == mqtt.ReservedNamespace:
		return "qos0-1-2 and the parent level"
	default:
		// The JetStream API, the identity request and the reserved subscriptions are all
		// used before the listener opens.
		return "startup"
	}
}

// TestEveryContractGrantIsExercised removes each entry of each cell's grant in turn and
// requires the attributed denial on the removed subject: every entry the contract states is
// used by what the witness exercises. It shows use, not that every write is covered:
// another write under the same entry can produce the denial while a moved one escapes.
func TestEveryContractGrantIsExercised(t *testing.T) {
	if testing.Short() {
		t.Skip("runs a broker per grant entry")
	}
	scenarios := contractScenarios()
	for _, cell := range contractCells() {
		full := cell.serviceGrant()
		for i, g := range full {
			if cell.domain != "" && g.subject == "$JS."+cell.domain+".API.>" {
				// The server here runs the domain itself and maps the request onto
				// $JS.API. before judging it, so this entry is used only where the domain is
				// reached over a leaf node, which this witness does not build.
				continue
			}
			t.Run(cell.String()+"/without "+g.String(), func(t *testing.T) {
				reduced := slices.Delete(slices.Clone(full), i, i+1)
				r := startContractRig(t, cell, reduced, 8*time.Second)
				if r.started == nil {
					_ = scenarios[exercisedBy(cell, g)].run(r, 3*time.Second)
				}
				if !r.log.waitViolation(contractServiceUser, g.subject, 10*time.Second) {
					t.Errorf("without %s the broker produced no denial on it (startup: %v): the entry is not exercised", g, r.started)
				}
			})
		}
	}
}

// TestResponsePermissionsBypassTheReservedDeny is the evidence for the contract's rule that
// MQTT identities carry no response permissions: nats-server consults an identity's dynamic
// response grant AFTER its explicit publish deny, so an identity allowed responses answers a
// request whose reply is in $MQTT.> despite denying it.
func TestResponsePermissionsBypassTheReservedDeny(t *testing.T) {
	for _, tc := range []struct {
		name      string
		responses string
		bypass    bool
	}{
		{"with allow_responses", "allow_responses: true", true},
		{"without", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			denies := confList(append(slices.Clone(mqtt.ClientDeniedSubjects), mqtt.ClientRecommendedDeniedSubjects...))
			conf := fmt.Sprintf(`
accounts {
  APP {
    users: [
      { user: requester, password: r }
      { user: client, password: c, permissions: {
          publish: { deny: %s }
          subscribe: { allow: ["svc.ask"], deny: %s }
          %s } }
    ]
  }
}
`, denies, denies, tc.responses)
			file := filepath.Join(t.TempDir(), "nats.conf")
			if err := os.WriteFile(file, []byte(conf), 0o600); err != nil {
				t.Fatal(err)
			}
			opts, err := server.ProcessConfigFile(file)
			if err != nil {
				t.Fatal(err)
			}
			opts.Host, opts.Port, opts.HTTPPort, opts.NoSigs = "127.0.0.1", -1, 0, true
			srv, err := server.NewServer(opts)
			if err != nil {
				t.Fatal(err)
			}
			vl := &violationLog{}
			srv.SetLoggerV2(vl, false, false, false)
			go srv.Start()
			if !srv.ReadyForConnections(10 * time.Second) {
				t.Fatal("server not ready")
			}
			defer func() { srv.Shutdown(); srv.WaitForShutdown() }()

			requester, err := nats.Connect(srv.ClientURL(), nats.UserInfo("requester", "r"))
			if err != nil {
				t.Fatal(err)
			}
			defer requester.Close()
			client, err := nats.Connect(srv.ClientURL(), nats.UserInfo("client", "c"))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if _, err := client.Subscribe("svc.ask", func(m *nats.Msg) { _ = m.Respond([]byte("forged")) }); err != nil {
				t.Fatal(err)
			}
			if err := client.Flush(); err != nil {
				t.Fatal(err)
			}
			reply := mqtt.ReservedNamespace[:len(mqtt.ReservedNamespace)-1] + "uinfo.witness"
			got, err := requester.SubscribeSync(reply)
			if err != nil {
				t.Fatal(err)
			}
			if err := requester.PublishRequest("svc.ask", reply, nil); err != nil {
				t.Fatal(err)
			}
			_, err = got.NextMsg(2 * time.Second)
			if received := err == nil; received != tc.bypass {
				t.Fatalf("the reply into %s was received: %t, want %t", reply, received, tc.bypass)
			}
			if !tc.bypass && !vl.waitViolation("client", reply, 5*time.Second) {
				t.Fatal("the refused reply produced no attributed denial")
			}
		})
	}
}

// TestSharedSubscriptionsNeedTheirQueueGroupAdmitted witnesses the contract's queue rule: a
// shared subscription joins the queue group $mqttshare.<key> on the client's own connection,
// so a subscribe permission that names queue groups must admit it, and a queue-scoped allow
// on the subject replaces a plain allow's verdict for queue subscriptions.
func TestSharedSubscriptionsNeedTheirQueueGroupAdmitted(t *testing.T) {
	cell := contractCell{passthrough: true}
	for _, tc := range []struct {
		name      string
		clientSub []string
		served    bool
	}{
		{"a queue-scoped allow admitting the shared groups", []string{"t.q.> " + mqtt.SharedQueueGroups}, true},
		{"a queue-scoped allow naming another group", []string{"t.q.> other"}, false},
		{"a plain allow overridden by a queue-scoped one", []string{"t.q.>", "t.q.> other"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := startContractRigWithClient(t, cell, cell.serviceGrant(), tc.clientSub, 30*time.Second)
			if r.started != nil {
				t.Fatalf("the broker did not start: %v", r.started)
			}
			m, err := dialContract(r.addr, "queue-member", paho.Connect{CleanStart: true})
			if err != nil {
				t.Fatal(err)
			}
			defer m.close()
			err = m.subscribe("$share/g/t/q/x", 0)
			if served := err == nil; served != tc.served {
				t.Fatalf("the shared subscription was admitted: %t (%v), want %t", served, err, tc.served)
			}
			if !tc.served {
				return
			}
			pub, err := dialContract(r.addr, "queue-pub", paho.Connect{CleanStart: true})
			if err != nil {
				t.Fatal(err)
			}
			defer pub.close()
			if err := pub.publish("t/q/x", 0, false, "shared"); err != nil {
				t.Fatal(err)
			}
			if err := m.expect("t/q/x", "shared", 10*time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}
