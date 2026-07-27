package mqtt

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestPrefixDomain pins the JS API subject rewrite. With no domain the subject
// is untouched; with one, "$JS.API.<x>" becomes "$JS.<domain>.API.<x>". The
// rewrite must also be idempotent, because newRequestExMulti and the purge/
// delete helpers each call it on subjects that may already be prefixed.
func TestPrefixDomain(t *testing.T) {
	for _, tc := range []struct {
		name, domain, subject, want string
	}{
		{"no domain", "", "$JS.API.STREAM.INFO.x", "$JS.API.STREAM.INFO.x"},
		{"domain", "HUB", "$JS.API.STREAM.INFO.x", "$JS.HUB.API.STREAM.INFO.x"},
		{"non-api subject", "HUB", "$MQTT.sess.abc", "$MQTT.sess.abc"},
		{"api prefix without dot", "HUB", "$JS.APIX.foo", "$JS.APIX.foo"},
		{"already prefixed", "HUB", "$JS.HUB.API.STREAM.INFO.x", "$JS.HUB.API.STREAM.INFO.x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			jsa := &mqttJSA{domain: tc.domain}
			if got := jsa.prefixDomain(tc.subject); got != tc.want {
				t.Errorf("prefixDomain(%q) with domain %q = %q, want %q",
					tc.subject, tc.domain, got, tc.want)
			}
		})
	}
}

// serveJSDomain makes a plain test server answer domain-qualified JS API
// requests, standing in for the remote domain a real -js-domain deployment
// points at: it forwards "$JS.<domain>.API.<x>" to the local "$JS.API.<x>" and
// relays the reply. It returns a counter of forwarded requests so a test can
// prove traffic actually took the domain-qualified path.
func serveJSDomain(t *testing.T, nc *nats.Conn, domain string) func() int {
	t.Helper()
	prefix := "$JS." + domain + ".API."
	var forwarded atomic.Int64
	sub, err := nc.Subscribe(prefix+">", func(m *nats.Msg) {
		forwarded.Add(1)
		fwd := nats.NewMsg(JSApiPrefix + "." + strings.TrimPrefix(m.Subject, prefix))
		fwd.Header = m.Header
		fwd.Data = m.Data
		resp, err := nc.RequestMsg(fwd, 5*time.Second)
		if err != nil {
			return
		}
		resp.Subject = m.Reply
		_ = nc.PublishMsg(resp)
	})
	if err != nil {
		t.Fatalf("Subscribe %s>: %v", prefix, err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return func() int { return int(forwarded.Load()) }
}

// TestJSDomainResolution covers the single source of truth for the JetStream
// domain: -js-domain, surfaced here as MQTTOpts.JsDomain. The domain must reach
// both the JS API prefix (jsa.domain) and the session subject token (domainTk),
// since a mismatch between the two would store sessions under a namespace the
// broker cannot query back.
func TestJSDomainResolution(t *testing.T) {
	for _, tc := range []struct {
		name, jsDomain, wantDomainTk string
	}{
		{"unset", "", ""},
		{"set", "HUB", "HUB."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, nc := runNATS(t)
			// With a domain set, every JS API request is rewritten to
			// "$JS.HUB.API.>", which a plain server does not serve. Bridge it so
			// bring-up completes and the assertions below run for real.
			forwarded := func() int { return 0 }
			if tc.jsDomain != "" {
				forwarded = serveJSDomain(t, nc, tc.jsDomain)
			}

			s := &Server{
				nrc:      nc,
				opts:     &Options{MQTT: MQTTOpts{JsDomain: tc.jsDomain}},
				quitCh:   make(chan struct{}),
				nodeName: "node1",
			}
			s.running.Store(true)
			s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)

			c := s.createInternalAccountClient()
			asm, err := s.getOrCreateMQTTAccountSessionManager(c)
			if err != nil {
				t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
			}
			if asm.jsa.domain != tc.jsDomain {
				t.Errorf("jsa.domain = %q, want %q", asm.jsa.domain, tc.jsDomain)
			}
			if asm.domainTk != tc.wantDomainTk {
				t.Errorf("domainTk = %q, want %q", asm.domainTk, tc.wantDomainTk)
			}
			if tc.jsDomain != "" && forwarded() == 0 {
				t.Error("no request took the $JS.<domain>.API path; the domain was resolved but never applied")
			}

			// The session subject must carry the same token, or sessions are
			// stored where they cannot be read back.
			subject := mqttSessStreamSubjectPrefix + asm.domainTk + "abc"
			want := mqttSessStreamSubjectPrefix + tc.wantDomainTk + "abc"
			if subject != want {
				t.Errorf("session subject = %q, want %q", subject, want)
			}
		})
	}
}

// TestJSARequestHeaderClaimsNoCluster inspects the header sendJSAPIrequests
// actually builds, before processInboundClientMsg strips it. That distinction
// matters: the wire-level test below cannot fail for a fabricated cluster name,
// because the strip removes the whole header either way. This one can, and is
// the regression guard for the broker advertising its node id as a NATS cluster.
func TestJSARequestHeaderClaimsNoCluster(t *testing.T) {
	hdr := mqttJSARequestHeader("$G")

	// The header block is "NATS/1.0\r\n" + MIME headers + CRLF CRLF.
	_, rest, ok := bytes.Cut(hdr, []byte(CR_LF))
	if !ok {
		t.Fatalf("malformed header block: %q", hdr)
	}
	mh, err := textproto.NewReader(bufio.NewReader(bytes.NewReader(rest))).ReadMIMEHeader()
	if err != nil {
		t.Fatalf("parse header: %v (%q)", err, hdr)
	}
	raw := mh.Get(ClientInfoHdr)
	if raw == "" {
		t.Fatalf("no %s in %q", ClientInfoHdr, hdr)
	}

	var ci ClientInfo
	if err := json.Unmarshal([]byte(raw), &ci); err != nil {
		t.Fatalf("unmarshal ClientInfo %q: %v", raw, err)
	}
	if ci.Account != "$G" {
		t.Errorf("ClientInfo.Account = %q, want $G", ci.Account)
	}
	if ci.Cluster != "" {
		t.Errorf("ClientInfo.Cluster = %q, want empty: the broker has no cluster and must not claim one", ci.Cluster)
	}
}

// TestJSAPIRequestsCarryNoClientInfo pins the other half: whatever
// sendJSAPIrequests builds, no Nats-Request-Info reaches NATS at all. It is a
// trusted server-internal header, and the broker is an ordinary external client
// with no standing to assert anything through it. This guards the strip in
// processInboundClientMsg, not the ClientInfo contents.
func TestJSAPIRequestsCarryNoClientInfo(t *testing.T) {
	_, nc := runNATS(t)

	var (
		mu   sync.Mutex
		seen []*nats.Msg
	)
	sub, err := nc.Subscribe("$JS.API.>", func(m *nats.Msg) {
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	s := &Server{
		nrc:      nc,
		opts:     &Options{},
		quitCh:   make(chan struct{}),
		nodeName: "node1",
	}
	s.running.Store(true)
	s.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)

	c := s.createInternalAccountClient()
	if _, err := s.getOrCreateMQTTAccountSessionManager(c); err != nil {
		t.Fatalf("getOrCreateMQTTAccountSessionManager: %v", err)
	}

	// Stop delivery before reading, so the slice is not racing the handler.
	_ = sub.Unsubscribe()
	mu.Lock()
	defer mu.Unlock()
	for _, m := range seen {
		if v := m.Header.Get(ClientInfoHdr); v != "" {
			t.Errorf("JS API request to %q carried %s = %q, want it stripped",
				m.Subject, ClientInfoHdr, v)
		}
	}
	if len(seen) == 0 {
		t.Fatal("observed no JS API requests; the assertion above proved nothing")
	}
}
