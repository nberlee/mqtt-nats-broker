//go:build mqttport_fulltests

package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
	"github.com/nberlee/mqtt-nats-broker/internal/tlsreload"
)

// tlsGeneration is one renewal's material: a CA, the listener pair it issued,
// and a client certificate it issued.
type tlsGeneration struct {
	ca        *testcerts.Authority
	cert, key []byte
	client    tls.Certificate
}

func newTLSGeneration(t testing.TB) *tlsGeneration {
	t.Helper()
	ca, err := testcerts.NewAuthority("reload-ca")
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue("mqtt-server", "127.0.0.1", "localhost")
	if err != nil {
		t.Fatal(err)
	}
	cc, ck, err := ca.Issue("mqtt-client")
	if err != nil {
		t.Fatal(err)
	}
	client, err := tls.X509KeyPair(cc, ck)
	if err != nil {
		t.Fatal(err)
	}
	return &tlsGeneration{ca: ca, cert: cert, key: key, client: client}
}

// tlsProjection lays the files out as the kubelet's atomic writer does for a
// projected Secret: a timestamped payload directory, a ..data symlink to it,
// and visible symlinks through ..data. swap renames a fresh ..data_tmp over
// ..data and deletes the previous payload.
type tlsProjection struct {
	t   testing.TB
	dir string
	n   int
}

func newTLSProjection(t testing.TB, cert, key, ca []byte) *tlsProjection {
	t.Helper()
	p := &tlsProjection{t: t, dir: t.TempDir()}
	p.swap(cert, key, ca)
	for _, name := range []string{"tls.crt", "tls.key", "ca.crt"} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(p.dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func (p *tlsProjection) files(withCA bool) tlsreload.Files {
	f := tlsreload.Files{Cert: filepath.Join(p.dir, "tls.crt"), Key: filepath.Join(p.dir, "tls.key")}
	if withCA {
		f.CA = filepath.Join(p.dir, "ca.crt")
	}
	return f
}

func (p *tlsProjection) swap(cert, key, ca []byte) {
	p.t.Helper()
	p.n++
	old, _ := os.Readlink(filepath.Join(p.dir, "..data"))
	gen := fmt.Sprintf("..payload_%d", p.n)
	if err := os.Mkdir(filepath.Join(p.dir, gen), 0o700); err != nil {
		p.t.Fatal(err)
	}
	for name, data := range map[string][]byte{"tls.crt": cert, "tls.key": key, "ca.crt": ca} {
		if err := os.WriteFile(filepath.Join(p.dir, gen, name), data, 0o600); err != nil {
			p.t.Fatal(err)
		}
	}
	tmp := filepath.Join(p.dir, "..data_tmp")
	if err := os.Symlink(gen, tmp); err != nil {
		p.t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(p.dir, "..data")); err != nil {
		p.t.Fatal(err)
	}
	if old != "" {
		if err := os.RemoveAll(filepath.Join(p.dir, old)); err != nil {
			p.t.Fatal(err)
		}
	}
}

// tlsReloadClient is a client config trusting every given generation's CA,
// offering client's certificate when it is set, and holding no session cache,
// so every handshake is a full one that presents the listener's certificate.
func tlsReloadClient(client *tls.Certificate, trust ...*tlsGeneration) *tls.Config {
	pool := x509.NewCertPool()
	for _, g := range trust {
		pool.AppendCertsFromPEM(g.ca.CertPEM)
	}
	cfg := &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	if client != nil {
		// See TestMQTTTLSRejectsUntrustedClientCert: Certificates would send nothing
		// when the server's request names another CA.
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return client, nil }
	}
	return cfg
}

func servedTLSSerial(t testing.TB, c net.Conn) string {
	t.Helper()
	tc, ok := c.(*tls.Conn)
	if !ok {
		t.Fatalf("expected a TLS connection, got %T", c)
	}
	return tc.ConnectionState().PeerCertificates[0].SerialNumber.String()
}

func tlsSerialOf(t testing.TB, certPEM []byte) string {
	t.Helper()
	b, _ := pem.Decode(certPEM)
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c.SerialNumber.String()
}

// TestMQTTTLSReloadKeepsExistingConnections: a renewed listener certificate,
// projected the way the kubelet updates a Secret, reaches the next handshake,
// while a client admitted under the previous certificate stays connected and
// keeps exchanging messages.
func TestMQTTTLSReloadKeepsExistingConnections(t *testing.T) {
	a, b := newTLSGeneration(t), newTLSGeneration(t)
	proj := newTLSProjection(t, a.cert, a.key, nil)
	src, err := tlsreload.Load(proj.files(false), nil)
	if err != nil {
		t.Fatal(err)
	}
	o := testMQTTDefaultOptions()
	o.MQTT.TLSConfig = src.ServerConfig()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	clientCfg := tlsReloadClient(nil, a, b)

	ca, ra := testMQTTConnect(t, &mqttConnInfo{clientID: "reload-a", cleanSess: true, tls: true, tlsc: clientCfg}, o.MQTT.Host, o.MQTT.Port)
	defer ca.Close()
	testMQTTCheckConnAck(t, ra, mqttConnAckRCConnectionAccepted, false)
	if got, want := servedTLSSerial(t, ca), tlsSerialOf(t, a.cert); got != want {
		t.Fatalf("first client was served %s, want %s", got, want)
	}
	testMQTTSub(t, 1, ca, ra, []*mqttFilter{{filter: "reload/a", qos: 0}}, []byte{0})
	testMQTTFlush(t, ca, nil, ra)

	proj.swap(b.cert, b.key, nil)
	if out := src.Check(); out != tlsreload.Applied {
		t.Fatalf("Check after the renewal = %v, want applied", out)
	}

	cb, rb := testMQTTConnect(t, &mqttConnInfo{clientID: "reload-b", cleanSess: true, tls: true, tlsc: clientCfg}, o.MQTT.Host, o.MQTT.Port)
	defer cb.Close()
	testMQTTCheckConnAck(t, rb, mqttConnAckRCConnectionAccepted, false)
	if got, want := servedTLSSerial(t, cb), tlsSerialOf(t, b.cert); got != want {
		t.Fatalf("a client connecting after the reload was served %s, want the renewal %s", got, want)
	}
	testMQTTSub(t, 1, cb, rb, []*mqttFilter{{filter: "reload/b", qos: 0}}, []byte{0})
	testMQTTFlush(t, cb, nil, rb)

	// Both directions across the reload, the old connection still on its own certificate.
	testMQTTPublish(t, cb, rb, 0, false, false, "reload/a", 0, []byte("to a"))
	testMQTTCheckPubMsg(t, ca, ra, "reload/a", 0, []byte("to a"))
	testMQTTPublish(t, ca, ra, 0, false, false, "reload/b", 0, []byte("to b"))
	testMQTTCheckPubMsg(t, cb, rb, "reload/b", 0, []byte("to b"))
	if got, want := servedTLSSerial(t, ca), tlsSerialOf(t, a.cert); got != want {
		t.Fatalf("the connection admitted before the reload reports %s, want %s", got, want)
	}
}

// TestMQTTTLSReloadClientCARollover: replacing the client CA refuses a new
// connection whose certificate the removed CA issued and admits one from the
// new CA, while a client admitted under the removed CA stays connected (the
// stated residual in docs/CONFORMANCE.md).
func TestMQTTTLSReloadClientCARollover(t *testing.T) {
	a, b := newTLSGeneration(t), newTLSGeneration(t)
	proj := newTLSProjection(t, a.cert, a.key, a.ca.CertPEM)
	src, err := tlsreload.Load(proj.files(true), nil)
	if err != nil {
		t.Fatal(err)
	}
	o := testMQTTDefaultOptions()
	o.MQTT.TLSConfig = src.ServerConfig()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	ca, ra := testMQTTConnect(t, &mqttConnInfo{clientID: "ca-old", cleanSess: true, tls: true, tlsc: tlsReloadClient(&a.client, a)}, o.MQTT.Host, o.MQTT.Port)
	defer ca.Close()
	testMQTTCheckConnAck(t, ra, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, ca, ra, []*mqttFilter{{filter: "rollover/old", qos: 0}}, []byte{0})
	testMQTTFlush(t, ca, nil, ra)

	proj.swap(a.cert, a.key, b.ca.CertPEM)
	if out := src.Check(); out != tlsreload.Applied {
		t.Fatalf("Check after the CA rollover = %v, want applied", out)
	}

	refused, _, err := testMQTTConnectRetryWithError(t, &mqttConnInfo{clientID: "ca-old-2", cleanSess: true, tls: true, tlsc: tlsReloadClient(&a.client, a)}, o.MQTT.Host, o.MQTT.Port, 0)
	if refused != nil {
		refused.Close()
	}
	if err == nil {
		t.Fatal("a new connection with a certificate from the removed CA was admitted")
	}

	cb, rb := testMQTTConnect(t, &mqttConnInfo{clientID: "ca-new", cleanSess: true, tls: true, tlsc: tlsReloadClient(&b.client, a)}, o.MQTT.Host, o.MQTT.Port)
	defer cb.Close()
	testMQTTCheckConnAck(t, rb, mqttConnAckRCConnectionAccepted, false)
	testMQTTPublish(t, cb, rb, 0, false, false, "rollover/old", 0, []byte("still here"))
	testMQTTCheckPubMsg(t, ca, ra, "rollover/old", 0, []byte("still here"))
}
