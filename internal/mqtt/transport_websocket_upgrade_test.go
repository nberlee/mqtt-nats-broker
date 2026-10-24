package mqtt

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// wsTestRequest is an upgrade request as fields in wire order, so a test can repeat, drop or
// malform any of them.
type wsTestRequest struct {
	line   string
	fields [][2]string
	extra  string // bytes sent right after the head
}

func wsValidRequest() *wsTestRequest {
	return &wsTestRequest{
		line: "GET /mqtt HTTP/1.1",
		fields: [][2]string{
			{"Host", "broker.example:8080"},
			{"Upgrade", "websocket"},
			{"Connection", "Upgrade"},
			{"Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ=="},
			{"Sec-WebSocket-Version", "13"},
			{"Sec-WebSocket-Protocol", "mqtt"},
		},
	}
}

func (r *wsTestRequest) set(name, value string) *wsTestRequest {
	for i, f := range r.fields {
		if f[0] == name {
			r.fields[i][1] = value
			return r
		}
	}
	r.fields = append(r.fields, [2]string{name, value})
	return r
}

func (r *wsTestRequest) add(name, value string) *wsTestRequest {
	r.fields = append(r.fields, [2]string{name, value})
	return r
}

func (r *wsTestRequest) drop(name string) *wsTestRequest {
	var out [][2]string
	for _, f := range r.fields {
		if f[0] != name {
			out = append(out, f)
		}
	}
	r.fields = out
	return r
}

func (r *wsTestRequest) bytes() []byte {
	var b strings.Builder
	b.WriteString(r.line + "\r\n")
	for _, f := range r.fields {
		b.WriteString(f[0] + ": " + f[1] + "\r\n")
	}
	b.WriteString("\r\n")
	b.WriteString(r.extra)
	return []byte(b.String())
}

// wsCountingConn counts the bytes the server side read.
type wsCountingConn struct {
	net.Conn
	n atomic.Int64
}

func (c *wsCountingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// wsTestResponse is the server's answer to an upgrade request: status and header fields.
type wsTestResponse struct {
	StatusCode int
	Header     http.Header
}

// wsTestHandshake runs the server side of a handshake against raw request bytes and returns
// the parsed response (nil when none was written) and the server's verdict.
func wsTestHandshake(t *testing.T, cfg *wsListenerConfig, req []byte) (*wsTestResponse, *wsCountingConn, error) {
	t.Helper()
	cli, srv := wsTestTCPPair(t)
	cs := &wsCountingConn{Conn: srv}
	done := make(chan error, 1)
	go func() { done <- wsHandshake(cs, cfg) }()
	go func() { _, _ = cli.Write(req) }()
	_ = cli.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, rerr := http.ReadResponse(bufio.NewReader(cli), nil)
	herr := <-done
	if rerr != nil {
		return nil, cs, herr
	}
	_ = resp.Body.Close()
	return &wsTestResponse{StatusCode: resp.StatusCode, Header: resp.Header}, cs, herr
}

func wsTestCfg() *wsListenerConfig {
	return &wsListenerConfig{path: "/mqtt", timeout: 2 * time.Second}
}

func TestWSHandshakeRefusals(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		req    func() *wsTestRequest
		status int
	}{
		{"HTTP/1.0", func() *wsTestRequest { r := wsValidRequest(); r.line = "GET /mqtt HTTP/1.0"; return r }, 505},
		{"POST", func() *wsTestRequest { r := wsValidRequest(); r.line = "POST /mqtt HTTP/1.1"; return r }, 405},
		{"lowercase method", func() *wsTestRequest { r := wsValidRequest(); r.line = "get /mqtt HTTP/1.1"; return r }, 405},
		{"malformed request line", func() *wsTestRequest { r := wsValidRequest(); r.line = "GET  /mqtt HTTP/1.1"; return r }, 400},
		{"absolute-form with Host", func() *wsTestRequest {
			r := wsValidRequest()
			r.line = "GET http://allowed.example/mqtt HTTP/1.1"
			return r
		}, 400},
		{"absolute-form without Host", func() *wsTestRequest {
			r := wsValidRequest().drop("Host")
			r.line = "GET http://allowed.example/mqtt HTTP/1.1"
			return r
		}, 400},
		{"authority-form", func() *wsTestRequest { r := wsValidRequest(); r.line = "GET broker.example:80 HTTP/1.1"; return r }, 400},
		{"asterisk-form", func() *wsTestRequest { r := wsValidRequest(); r.line = "GET * HTTP/1.1"; return r }, 400},
		{"no Host", func() *wsTestRequest { return wsValidRequest().drop("Host") }, 400},
		{"two Hosts", func() *wsTestRequest { return wsValidRequest().add("Host", "other.example") }, 400},
		{"empty Host", func() *wsTestRequest { return wsValidRequest().set("Host", "") }, 400},
		{"Host bad port", func() *wsTestRequest { return wsValidRequest().set("Host", "broker.example:99999") }, 400},
		{"Host empty port", func() *wsTestRequest { return wsValidRequest().set("Host", "broker.example:") }, 400},
		{"Content-Length 5", func() *wsTestRequest { return wsValidRequest().add("Content-Length", "5") }, 400},
		{"two Content-Length 0", func() *wsTestRequest {
			return wsValidRequest().add("Content-Length", "0").add("Content-Length", "0")
		}, 400},
		{"chunked", func() *wsTestRequest { return wsValidRequest().add("Transfer-Encoding", "chunked") }, 400},
		{"no Upgrade", func() *wsTestRequest { return wsValidRequest().drop("Upgrade") }, 400},
		{"no Connection", func() *wsTestRequest { return wsValidRequest().drop("Connection") }, 400},
		{"no key", func() *wsTestRequest { return wsValidRequest().drop("Sec-WebSocket-Key") }, 400},
		{"two keys", func() *wsTestRequest { return wsValidRequest().add("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==") }, 400},
		{"short key", func() *wsTestRequest { return wsValidRequest().set("Sec-WebSocket-Key", "c2hvcnQ=") }, 400},
		{"version 8", func() *wsTestRequest { return wsValidRequest().set("Sec-WebSocket-Version", "8") }, 426},
		{"two versions", func() *wsTestRequest { return wsValidRequest().add("Sec-WebSocket-Version", "13") }, 426},
		{"wrong path", func() *wsTestRequest { r := wsValidRequest(); r.line = "GET /other HTTP/1.1"; return r }, 404},
		{"differently escaped path", func() *wsTestRequest { r := wsValidRequest(); r.line = "GET /mq%74t HTTP/1.1"; return r }, 404},
		{"no subprotocol", func() *wsTestRequest { return wsValidRequest().drop("Sec-WebSocket-Protocol") }, 400},
		{"MQTT uppercase", func() *wsTestRequest { return wsValidRequest().set("Sec-WebSocket-Protocol", "MQTT") }, 400},
		{"mqttv3.1 alone", func() *wsTestRequest { return wsValidRequest().set("Sec-WebSocket-Protocol", "mqttv3.1") }, 400},
		{"mqtt twice", func() *wsTestRequest { return wsValidRequest().set("Sec-WebSocket-Protocol", "mqtt, mqtt") }, 400},
		{"mqtt and a bad token", func() *wsTestRequest { return wsValidRequest().set("Sec-WebSocket-Protocol", "mqtt, bad token") }, 400},
		{"empty element", func() *wsTestRequest { return wsValidRequest().set("Sec-WebSocket-Protocol", "mqtt,,x") }, 400},
		{"duplicate across fields", func() *wsTestRequest { return wsValidRequest().add("Sec-WebSocket-Protocol", "mqtt") }, 400},
		{"whitespace in Transfer-Encoding name", func() *wsTestRequest { return wsValidRequest().add("Transfer-Encoding ", "chunked") }, 400},
		{"whitespace in Origin name", func() *wsTestRequest { return wsValidRequest().add("Origin ", "https://foreign.example") }, 400},
		{"whitespace in an unknown name", func() *wsTestRequest { return wsValidRequest().add("X Bad", "1") }, 400},
		{"data before the 101", func() *wsTestRequest { r := wsValidRequest(); r.extra = string(wsBin("early")); return r }, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, _, herr := wsTestHandshake(t, wsTestCfg(), tc.req().bytes())
			if resp == nil || resp.StatusCode != tc.status {
				t.Fatalf("response = %+v, want %d", resp, tc.status)
			}
			if herr == nil {
				t.Fatal("the handshake reported success")
			}
			if tc.status == 426 && resp.Header.Get("Sec-WebSocket-Version") != "13" {
				t.Fatal("426 without Sec-WebSocket-Version: 13")
			}
			if tc.status == 405 && resp.Header.Get("Allow") != "GET" {
				t.Fatal("405 without Allow: GET")
			}
		})
	}
}

func TestWSHandshakeAcceptsOfferedMqtt(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		req  *wsTestRequest
	}{
		{"mqtt", wsValidRequest()},
		{"mqtt among others", wsValidRequest().set("Sec-WebSocket-Protocol", "foo, mqtt")},
		{"mqtt in a second field", wsValidRequest().set("Sec-WebSocket-Protocol", "foo").add("Sec-WebSocket-Protocol", "mqtt")},
		{"token lists in Upgrade and Connection", wsValidRequest().set("Upgrade", "h2c, WebSocket").set("Connection", "keep-alive, upgrade")},
		{"permessage-deflate offered", wsValidRequest().add("Sec-WebSocket-Extensions", "permessage-deflate; client_max_window_bits")},
		{"query ignored", func() *wsTestRequest { r := wsValidRequest(); r.line = "GET /mqtt?x=1 HTTP/1.1"; return r }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, _, herr := wsTestHandshake(t, wsTestCfg(), tc.req.bytes())
			if herr != nil || resp == nil || resp.StatusCode != 101 {
				t.Fatalf("response %+v, error %v", resp, herr)
			}
			// RFC 6455 §1.3's worked example.
			if got := resp.Header.Get("Sec-WebSocket-Accept"); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
				t.Fatalf("Sec-WebSocket-Accept = %q", got)
			}
			if got := resp.Header.Values("Sec-WebSocket-Protocol"); len(got) != 1 || got[0] != "mqtt" {
				t.Fatalf("Sec-WebSocket-Protocol = %q, want exactly mqtt [MQTT-6.0.0-4]", got)
			}
			if resp.Header.Get("Sec-WebSocket-Extensions") != "" {
				t.Fatal("an extension was negotiated")
			}
		})
	}
}

// The handshake never reads more than its budget from the socket, so the parser cannot see
// more either.
func TestWSHandshakeHeadBudget(t *testing.T) {
	t.Parallel()
	req := wsValidRequest().add("X-Big", strings.Repeat("a", 20<<10))
	resp, cs, herr := wsTestHandshake(t, wsTestCfg(), req.bytes())
	if resp == nil || resp.StatusCode != 431 || herr == nil {
		t.Fatalf("response %+v, error %v; want 431", resp, herr)
	}
	if n := cs.n.Load(); n > wsMaxRequestHead {
		t.Fatalf("read %d bytes, budget %d", n, wsMaxRequestHead)
	}
}

// A malformed or truncated head is refused even when the parser returned part of a map.
func TestWSHandshakeParserErrorsRefuse(t *testing.T) {
	t.Parallel()
	head := wsValidRequest().bytes()
	colonless := append(head[:len(head)-2:len(head)-2], []byte("badline\r\n\r\n")...)
	resp, _, herr := wsTestHandshake(t, wsTestCfg(), colonless)
	if resp == nil || resp.StatusCode != 400 || herr == nil {
		t.Fatalf("colonless line: %+v, %v", resp, herr)
	}

	cli, srv := wsTestTCPPair(t)
	done := make(chan error, 1)
	go func() { done <- wsHandshake(srv, wsTestCfg()) }()
	_, _ = cli.Write(head[:len(head)-10])
	_ = cli.(*net.TCPConn).CloseWrite()
	if err := <-done; err == nil {
		t.Fatal("a truncated head was accepted")
	}
	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	if b, _ := io.ReadAll(cli); strings.HasPrefix(string(b), "HTTP/1.1 101") {
		t.Fatal("a truncated head got a 101")
	}
}

// A peer that sends half a request is cut off at the handshake timeout.
func TestWSHandshakeTimeout(t *testing.T) {
	t.Parallel()
	cli, srv := wsTestTCPPair(t)
	cfg := wsTestCfg()
	cfg.timeout = 200 * time.Millisecond
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- wsHandshake(srv, cfg) }()
	_, _ = cli.Write([]byte("GET /mqtt HTTP/1.1\r\nHost: x\r\n"))
	select {
	case err := <-done:
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("handshake ended with %v, want a timeout", err)
		}
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("took %v", el)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handshake outlived its timeout")
	}
}

func TestWSHandshakeOriginPolicy(t *testing.T) {
	t.Parallel()
	allow := func(list ...string) []wsOrigin {
		o, err := parseWSAllowedOrigins(list)
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	for _, tc := range []struct {
		name    string
		tls     bool
		allowed []wsOrigin
		host    string
		origins []string
		ok      bool
	}{
		{"absent", false, nil, "h:8080", nil, true},
		{"ws same host and port", false, nil, "h:8080", []string{"http://h:8080"}, true},
		{"ws default port both sides", false, nil, "h", []string{"http://h"}, true},
		{"ws case-insensitive", false, nil, "H:8080", []string{"HTTP://h:8080"}, true},
		{"wss default port", true, nil, "h", []string{"https://h"}, true},
		{"wss explicit 443", true, nil, "h", []string{"https://h:443"}, true},
		{"wss refuses an http page", true, nil, "h", []string{"http://h"}, false},
		{"wss refuses another port", true, nil, "h", []string{"https://h:8443"}, false},
		{"ws refuses an https page", false, nil, "h", []string{"https://h"}, false},
		{"different host", false, nil, "h:8080", []string{"http://evil:8080"}, false},
		{"null", false, nil, "h", []string{"null"}, false},
		{"two Origin fields", false, nil, "h", []string{"http://h", "http://h"}, false},
		{"trailing slash", false, nil, "h", []string{"http://h/"}, false},
		{"path", false, nil, "h", []string{"http://h/x"}, false},
		{"query", false, nil, "h", []string{"http://h?x"}, false},
		{"fragment", false, nil, "h", []string{"http://h#x"}, false},
		{"userinfo", false, nil, "h", []string{"http://u@h"}, false},
		{"empty host", false, nil, "h", []string{"http://"}, false},
		{"port 0", false, nil, "h", []string{"http://h:0"}, false},
		{"port 65536", false, nil, "h", []string{"http://h:65536"}, false},
		{"port not decimal", false, nil, "h", []string{"http://h:8o"}, false},
		{"scheme ws", false, nil, "h", []string{"ws://h"}, false},
		{"ipv6 same origin", false, nil, "[::1]:8080", []string{"http://[::1]:8080"}, true},
		{"allowlist admits a listed foreign origin", false, allow("https://app.example"), "h", []string{"https://app.example"}, true},
		{"allowlist replaces same-origin", false, allow("https://app.example"), "h", []string{"http://h"}, false},
		{"allowlist compares the scheme", false, allow("https://app.example"), "h", []string{"http://app.example"}, false},
		{"allowlist effective port", false, allow("https://app.example:443"), "h", []string{"https://app.example"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := wsTestCfg()
			cfg.tls, cfg.allowed = tc.tls, tc.allowed
			req := wsValidRequest().set("Host", tc.host)
			for _, o := range tc.origins {
				req.add("Origin", o)
			}
			resp, _, herr := wsTestHandshake(t, cfg, req.bytes())
			if tc.ok && (herr != nil || resp == nil || resp.StatusCode != 101) {
				t.Fatalf("refused: %+v, %v", resp, herr)
			}
			if !tc.ok && (resp == nil || resp.StatusCode != 403) {
				t.Fatalf("response %+v, want 403", resp)
			}
		})
	}
}

// Forwarded headers never make a foreign Origin pass the same-origin default.
func TestWSHandshakeOriginIgnoresForwardedHeaders(t *testing.T) {
	t.Parallel()
	req := wsValidRequest().set("Host", "h").add("Origin", "https://evil.example").
		add("X-Forwarded-Host", "evil.example").add("X-Forwarded-Proto", "https").add("Forwarded", "host=evil.example;proto=https")
	resp, _, _ := wsTestHandshake(t, wsTestCfg(), req.bytes())
	if resp == nil || resp.StatusCode != 403 {
		t.Fatalf("response %+v, want 403", resp)
	}
}
