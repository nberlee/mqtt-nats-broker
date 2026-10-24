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

// The WebSocket opening handshake (RFC 6455 §4.2) and the Origin policy. Adapted from
// nats-server v2.15.1 server/websocket.go (wsUpgrade, wsAcceptKey, checkOrigin), but read
// straight off the accepted connection inside connection setup rather than through
// net/http: the handshake then lives in the same lifecycle as the TLS handshake (one
// registry, one shutdown gate), and the policy sees the request exactly as sent.

package mqtt

import (
	"bufio"
	"crypto/fips140"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// mqttDefaultWSPath is the request path the WebSocket listener serves when none is
	// configured, the one MQTT clients commonly default to.
	mqttDefaultWSPath = "/mqtt"
	// mqttDefaultWSHandshakeTimeout bounds the opening handshake when none is configured.
	mqttDefaultWSHandshakeTimeout = 5 * time.Second
	// wsMaxRequestHead is the most the handshake reads from the socket: request line and
	// header block together.
	wsMaxRequestHead = 8 << 10
	// wsSubprotocol is the only subprotocol this server selects, and it must have been
	// offered [MQTT-6.0.0-3, MQTT-6.0.0-4].
	wsSubprotocol = "mqtt"
)

// RFC 6455 §1.3.
var wsGUID = []byte("258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

var errWSRequestHeadTooLarge = errors.New("websocket: request head exceeds the read budget")

// wsAcceptKey is Sec-WebSocket-Accept for a client key (RFC 6455 §4.2.2). SHA-1 is the
// protocol's, not a security choice, hence outside FIPS enforcement as upstream does.
func wsAcceptKey(key string) string {
	var sum []byte
	fips140.WithoutEnforcement(func() {
		h := sha1.New()
		h.Write([]byte(key))
		h.Write(wsGUID)
		sum = h.Sum(nil)
	})
	return base64.StdEncoding.EncodeToString(sum)
}

// wsListenerConfig is what one WebSocket listener's handshake is judged against.
type wsListenerConfig struct {
	path    string
	tls     bool       // the listener serves TLS; decides the scheme of the same-origin default
	allowed []wsOrigin // when non-nil it replaces the same-origin default
	timeout time.Duration
}

// wsOrigin is an origin as RFC 6454 compares it: scheme, host and effective port.
type wsOrigin struct {
	scheme, host string
	port         int
}

// wsHandshakeError is a refused upgrade: the HTTP status sent and why.
type wsHandshakeError struct {
	status int
	reason string
	extra  string // additional response header lines, CRLF-terminated
}

func (e *wsHandshakeError) Error() string {
	return fmt.Sprintf("websocket upgrade refused (%d): %s", e.status, e.reason)
}

func wsRefuse(status int, format string, v ...any) *wsHandshakeError {
	return &wsHandshakeError{status: status, reason: fmt.Sprintf(format, v...)}
}

// wsHeadLimiter sits BENEATH the bufio.Reader, so the budget bounds what is read from the
// socket and therefore what the parser can ever see, not just what it returns.
type wsHeadLimiter struct {
	r io.Reader
	n int
}

func (l *wsHeadLimiter) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, errWSRequestHeadTooLarge
	}
	if len(p) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	l.n -= n
	return n, err
}

// wsHandshake reads the opening handshake from conn and answers it. It returns nil only
// once the whole 101 has been written and the handshake deadline cleared; any refusal has
// been answered (best effort) before it returns. The handshake deadline covers the read and
// every response write.
func wsHandshake(conn net.Conn, cfg *wsListenerConfig) error {
	timeout := cfg.timeout
	if timeout <= 0 {
		timeout = mqttDefaultWSHandshakeTimeout
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	br := bufio.NewReader(&wsHeadLimiter{r: conn, n: wsMaxRequestHead})
	key, herr := wsReadUpgradeRequest(textproto.NewReader(br), cfg)
	if herr == nil && br.Buffered() > 0 {
		// A client must not send frames before it has the 101. This sees only bytes
		// already buffered, which is the check available.
		herr = wsRefuse(400, "data sent before the handshake completed")
	}
	if herr != nil {
		var refused *wsHandshakeError
		if errors.As(herr, &refused) {
			body := refused.reason + "\n"
			resp := fmt.Sprintf("HTTP/1.1 %d %s\r\nConnection: close\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: %d\r\n%s\r\n%s",
				refused.status, wsStatusText(refused.status), len(body), refused.extra, body)
			_ = wsWriteAll(conn, []byte(resp))
		}
		return herr
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		wsAcceptKey(key) + "\r\nSec-WebSocket-Protocol: " + wsSubprotocol + "\r\n\r\n"
	if err := wsWriteAll(conn, []byte(resp)); err != nil {
		return err
	}
	return conn.SetDeadline(time.Time{})
}

func wsWriteAll(conn net.Conn, b []byte) error {
	n, err := conn.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return err
}

func wsStatusText(status int) string {
	switch status {
	case 400:
		return "Bad Request"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 405:
		return "Method Not Allowed"
	case 426:
		return "Upgrade Required"
	case 431:
		return "Request Header Fields Too Large"
	case 505:
		return "HTTP Version Not Supported"
	}
	return "Error"
}

// wsReadUpgradeRequest parses and validates the request head, returning the client key. It
// uses net/textproto rather than http.ReadRequest because the policy must see the request as
// sent: ReadRequest removes Host from the header map, takes the authority of an
// absolute-form target instead of the Host field, consumes a chunked Transfer-Encoding and
// hides repeated Host fields. ReadMIMEHeader keeps every field and repetition, but it is no
// validator, so every field name is checked as a token before any named lookup, and any
// parser error refuses even when a partial map came back. The checks run in order and the
// first failure decides the status.
func wsReadUpgradeRequest(tp *textproto.Reader, cfg *wsListenerConfig) (string, error) {
	line, err := tp.ReadLine()
	if err != nil {
		return "", wsReadError(err)
	}
	hdr, err := tp.ReadMIMEHeader()
	if err != nil {
		return "", wsReadError(err)
	}
	for name := range hdr {
		// ReadMIMEHeader accepts whitespace before the colon and leaves such a name
		// uncanonicalized, where the lookups below would never see it (RFC 9112 §5.1).
		if !wsIsToken(name) {
			return "", wsRefuse(400, "invalid header field name %q", name)
		}
	}
	parts := strings.Split(line, " ")
	if len(parts) != 3 {
		return "", wsRefuse(400, "malformed request line")
	}
	method, target, proto := parts[0], parts[1], parts[2]
	if proto != "HTTP/1.1" {
		return "", wsRefuse(505, "protocol must be HTTP/1.1")
	}
	if method != "GET" {
		return "", &wsHandshakeError{status: 405, reason: "request method must be GET", extra: "Allow: GET\r\n"}
	}
	// Origin-form only, so the Host field is the one authority the policy reads.
	if !strings.HasPrefix(target, "/") {
		return "", wsRefuse(400, "request target must be in origin-form")
	}
	u, err := url.ParseRequestURI(target)
	if err != nil {
		return "", wsRefuse(400, "malformed request target")
	}
	hosts := hdr.Values("Host")
	if len(hosts) != 1 {
		return "", wsRefuse(400, "exactly one Host field is required")
	}
	hostName, hostPort, ok := wsSplitAuthority(hosts[0])
	if !ok {
		return "", wsRefuse(400, "malformed Host field")
	}
	if len(hdr.Values("Transfer-Encoding")) > 0 {
		return "", wsRefuse(400, "an upgrade request carries no body")
	}
	if cl := hdr.Values("Content-Length"); len(cl) > 1 || (len(cl) == 1 && cl[0] != "0") {
		return "", wsRefuse(400, "an upgrade request carries no body")
	}
	if !wsListContainsFold(hdr.Values("Upgrade"), "websocket") {
		return "", wsRefuse(400, "Upgrade must name websocket")
	}
	if !wsListContainsFold(hdr.Values("Connection"), "Upgrade") {
		return "", wsRefuse(400, "Connection must name Upgrade")
	}
	keys := hdr.Values("Sec-Websocket-Key")
	if len(keys) != 1 {
		return "", wsRefuse(400, "exactly one Sec-WebSocket-Key field is required")
	}
	if raw, err := base64.StdEncoding.DecodeString(keys[0]); err != nil || len(raw) != 16 {
		return "", wsRefuse(400, "Sec-WebSocket-Key is not 16 base64-encoded bytes")
	}
	if v := hdr.Values("Sec-Websocket-Version"); len(v) != 1 || v[0] != "13" {
		return "", &wsHandshakeError{status: 426, reason: "Sec-WebSocket-Version must be 13", extra: "Sec-WebSocket-Version: 13\r\n"}
	}
	if u.EscapedPath() != cfg.path {
		return "", wsRefuse(404, "no MQTT WebSocket endpoint at this path")
	}
	if err := cfg.checkOrigin(hdr.Values("Origin"), hostName, hostPort); err != nil {
		return "", err
	}
	if err := wsCheckSubprotocols(hdr.Values("Sec-Websocket-Protocol")); err != nil {
		return "", err
	}
	return keys[0], nil
}

// wsReadError maps a failure to read the head. A malformed or oversized head is answered;
// an I/O failure (timeout, peer gone) is not, since nobody is there to read an answer.
func wsReadError(err error) error {
	var perr textproto.ProtocolError
	switch {
	case errors.Is(err, errWSRequestHeadTooLarge):
		return wsRefuse(431, "request head over %d bytes", wsMaxRequestHead)
	case errors.As(err, &perr):
		return wsRefuse(400, "malformed request head: %v", err)
	}
	return err
}

// wsCheckSubprotocols validates the whole offered list, across every field, before choosing:
// each element must be a token and none may repeat (RFC 6455 §4.1), and the exact,
// case-sensitive token "mqtt" must be among them [MQTT-6.0.0-3]. A legacy "mqttv3.1" alone
// is therefore refused.
func wsCheckSubprotocols(fields []string) error {
	seen := map[string]bool{}
	for _, f := range fields {
		for _, p := range strings.Split(f, ",") {
			p = strings.Trim(p, " \t")
			if !wsIsToken(p) {
				return wsRefuse(400, "invalid subprotocol %q", p)
			}
			if seen[p] {
				return wsRefuse(400, "subprotocol %q offered twice", p)
			}
			seen[p] = true
		}
	}
	if !seen[wsSubprotocol] {
		return wsRefuse(400, "the client must offer the %q subprotocol", wsSubprotocol)
	}
	return nil
}

// wsListContainsFold reports whether a comma-separated list, across every field, contains
// tok case-insensitively.
func wsListContainsFold(fields []string, tok string) bool {
	for _, f := range fields {
		for _, p := range strings.Split(f, ",") {
			if strings.EqualFold(strings.Trim(p, " \t"), tok) {
				return true
			}
		}
	}
	return false
}

// wsIsToken reports whether s is an HTTP token (RFC 9110 §5.6.2).
func wsIsToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

// wsSplitAuthority splits host[:port]. The host is a bracketed IPv6 literal or a nonempty
// reg-name/IPv4 of URI characters; a port, when the colon is present, is decimal 1-65535.
// port is 0 when absent.
func wsSplitAuthority(s string) (host string, port int, ok bool) {
	rest := ""
	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return "", 0, false
		}
		host, rest = s[:end+1], s[end+1:]
		if net.ParseIP(host[1:end]) == nil {
			return "", 0, false
		}
	} else {
		host, rest = s, ""
		if i := strings.IndexByte(s, ':'); i >= 0 {
			host, rest = s[:i], s[i:]
		}
		if host == "" {
			return "", 0, false
		}
		for i := 0; i < len(host); i++ {
			c := host[i]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			case strings.IndexByte("-._~!$&'()*+,;=%", c) >= 0:
			default:
				return "", 0, false
			}
		}
	}
	if rest != "" {
		if rest[0] != ':' {
			return "", 0, false
		}
		p, err := strconv.Atoi(rest[1:])
		if err != nil || p < 1 || p > 65535 || rest[1:] != strconv.Itoa(p) {
			return "", 0, false
		}
		port = p
	}
	return strings.ToLower(host), port, true
}

func wsDefaultPort(scheme string) int {
	if scheme == "https" {
		return 443
	}
	return 80
}

// parseWSOrigin parses a serialized origin, scheme "://" host [":" port], and nothing else:
// no userinfo, path (not even "/"), query or fragment, and only http or https.
func parseWSOrigin(s string) (wsOrigin, bool) {
	i := strings.Index(s, "://")
	if i < 0 {
		return wsOrigin{}, false
	}
	scheme := strings.ToLower(s[:i])
	if scheme != "http" && scheme != "https" {
		return wsOrigin{}, false
	}
	auth := s[i+3:]
	if strings.ContainsAny(auth, "/?#@") {
		return wsOrigin{}, false
	}
	host, port, ok := wsSplitAuthority(auth)
	if !ok {
		return wsOrigin{}, false
	}
	if port == 0 {
		port = wsDefaultPort(scheme)
	}
	return wsOrigin{scheme: scheme, host: host, port: port}, true
}

// parseWSAllowedOrigins parses a configured allowlist; every entry must be a serialized
// origin.
func parseWSAllowedOrigins(list []string) ([]wsOrigin, error) {
	var out []wsOrigin
	for _, s := range list {
		o, ok := parseWSOrigin(s)
		if !ok {
			return nil, fmt.Errorf("mqtt websocket allowed origin %q is not scheme://host[:port] with scheme http or https", s)
		}
		out = append(out, o)
	}
	return out, nil
}

// checkOrigin is a browser boundary, not authentication: a native client sends no Origin
// and passes. A browser page passes when its origin is the listener's own - the scheme and
// default port come from the listener, never from the supplied Origin, and forwarded headers
// are never consulted - or, when an allowlist is configured, when it is listed.
func (cfg *wsListenerConfig) checkOrigin(fields []string, hostName string, hostPort int) error {
	if len(fields) == 0 {
		return nil
	}
	if len(fields) > 1 {
		return wsRefuse(403, "more than one Origin field")
	}
	got, ok := parseWSOrigin(fields[0])
	if !ok {
		return wsRefuse(403, "origin %q is not a serialized http(s) origin", fields[0])
	}
	if cfg.allowed != nil {
		for _, a := range cfg.allowed {
			if a == got {
				return nil
			}
		}
		return wsRefuse(403, "origin %q is not allowed", fields[0])
	}
	want := wsOrigin{scheme: "http", host: hostName, port: hostPort}
	if cfg.tls {
		want.scheme = "https"
	}
	if want.port == 0 {
		want.port = wsDefaultPort(want.scheme)
	}
	if got != want {
		return wsRefuse(403, "origin %q is not this listener's origin", fields[0])
	}
	return nil
}
