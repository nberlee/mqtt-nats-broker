package tlsreload

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
)

// generation is one complete set of TLS material: a CA, a server pair it
// issued, and a client pair it issued.
type generation struct {
	ca                   *testcerts.Authority
	cert, key            []byte
	client               tls.Certificate
	serial, clientSerial string
}

func newGeneration(t testing.TB) *generation {
	t.Helper()
	return newGenerationValid(t, time.Now().Add(-time.Minute), time.Now().Add(time.Hour))
}

func newGenerationValid(t testing.TB, notBefore, notAfter time.Time) *generation {
	t.Helper()
	ca, err := testcerts.NewAuthority("reload-ca")
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.IssueWithValidity("reload-server", notBefore, notAfter, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	cc, ck, err := ca.Issue("reload-client")
	if err != nil {
		t.Fatal(err)
	}
	client, err := tls.X509KeyPair(cc, ck)
	if err != nil {
		t.Fatal(err)
	}
	return &generation{ca: ca, cert: cert, key: key, client: client,
		serial: serialOf(t, cert), clientSerial: client.Leaf.SerialNumber.String()}
}

func serialOf(t testing.TB, certPEM []byte) string {
	t.Helper()
	b, _ := pem.Decode(certPEM)
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c.SerialNumber.String()
}

func (g *generation) contents() contents { return contents{cert: g.cert, key: g.key, ca: g.ca.CertPEM} }

// projection lays files out the way the kubelet's atomic writer does: the
// payload in a timestamped directory, a ..data symlink to it, and each visible
// file a symlink through ..data. swap publishes a new payload by renaming
// ..data_tmp over ..data and then deletes the old directory.
type projection struct {
	t   testing.TB
	dir string
	n   int
}

func newProjection(t testing.TB, c contents) *projection {
	t.Helper()
	p := &projection{t: t, dir: t.TempDir()}
	p.swap(c)
	for _, name := range []string{"tls.crt", "tls.key", "ca.crt"} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(p.dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func (p *projection) files(withCA bool) Files {
	f := Files{Cert: filepath.Join(p.dir, "tls.crt"), Key: filepath.Join(p.dir, "tls.key")}
	if withCA {
		f.CA = filepath.Join(p.dir, "ca.crt")
	}
	return f
}

func (p *projection) swap(c contents) {
	p.t.Helper()
	p.swapFiles(map[string][]byte{"tls.crt": c.cert, "tls.key": c.key, "ca.crt": c.ca})
}

// swapFiles publishes a payload; a nil entry leaves that file out of it.
func (p *projection) swapFiles(files map[string][]byte) {
	p.t.Helper()
	p.n++
	old, _ := os.Readlink(filepath.Join(p.dir, "..data"))
	gen := fmt.Sprintf("..2026_10_23_00_00_%02d.%d", p.n, p.n)
	if err := os.Mkdir(filepath.Join(p.dir, gen), 0o700); err != nil {
		p.t.Fatal(err)
	}
	for name, data := range files {
		if data == nil {
			continue
		}
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

// logCount counts log records by level.
type logCount struct {
	mu     sync.Mutex
	levels map[slog.Level]int
}

func (l *logCount) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCount) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.levels == nil {
		l.levels = map[slog.Level]int{}
	}
	l.levels[r.Level]++
	return nil
}
func (l *logCount) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *logCount) WithGroup(string) slog.Handler      { return l }
func (l *logCount) count(level slog.Level) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.levels[level]
}

// clock is an injectable time source.
type clock struct{ at atomic.Int64 }

func newClock(t time.Time) *clock {
	c := &clock{}
	c.set(t)
	return c
}
func (c *clock) set(t time.Time)     { c.at.Store(t.UnixNano()) }
func (c *clock) now() time.Time      { return time.Unix(0, c.at.Load()) }
func (c *clock) add(d time.Duration) { c.set(c.now().Add(d)) }

type fixture struct {
	src  *Source
	proj *projection
	log  *logCount
	clk  *clock
}

// loaded builds a Source over a projection of g with the test seams in place
// before the first load.
func loaded(t testing.TB, g *generation, withCA bool) *fixture {
	t.Helper()
	f := &fixture{proj: newProjection(t, g.contents()), log: &logCount{}, clk: newClock(time.Now())}
	f.src = newSource(f.proj.files(withCA), slog.New(f.log))
	f.src.now = f.clk.now
	if err := f.src.load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	return f
}

// handshake runs one handshake over a pipe and returns the serial the server
// presented and the server's error. client is the client certificate to offer,
// if any. cache, when set, lets the client resume.
type handshakeResult struct {
	served    string
	serverErr error
	clientErr error
	resumed   bool
}

func handshake(t testing.TB, server *tls.Config, client *tls.Certificate, version uint16, cache tls.ClientSessionCache) handshakeResult {
	t.Helper()
	sc, cc := net.Pipe()
	defer sc.Close()
	defer cc.Close()
	ccfg := &tls.Config{InsecureSkipVerify: true, ServerName: "localhost", ClientSessionCache: cache} //nolint:gosec // the test reads the served certificate, it does not trust it
	if version != 0 {
		ccfg.MinVersion, ccfg.MaxVersion = version, version
	}
	if client != nil {
		// GetClientCertificate rather than Certificates: Go's client sends no
		// certificate at all when the server's request names a different CA, which
		// would let a CA test pass without the server ever verifying a chain.
		ccfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return client, nil }
	}
	srv, cli := tls.Server(sc, server), tls.Client(cc, ccfg)
	_ = sc.SetDeadline(time.Now().Add(10 * time.Second))
	_ = cc.SetDeadline(time.Now().Add(10 * time.Second))
	var res handshakeResult
	done := make(chan struct{})
	go func() {
		defer close(done)
		res.serverErr = srv.Handshake()
		if res.serverErr == nil {
			// Writing after the handshake delivers TLS 1.3 session tickets with it.
			_, res.serverErr = srv.Write([]byte{1})
		}
		if res.serverErr != nil {
			sc.Close()
		}
	}()
	res.clientErr = cli.Handshake()
	if res.clientErr == nil {
		st := cli.ConnectionState()
		res.served = st.PeerCertificates[0].SerialNumber.String()
		res.resumed = st.DidResume
		var b [1]byte
		if _, err := cli.Read(b[:]); err != nil && res.clientErr == nil {
			res.clientErr = err
		}
	}
	cc.Close()
	<-done
	return res
}

func servedSerial(t testing.TB, src *Source, client *tls.Certificate) string {
	t.Helper()
	res := handshake(t, src.ServerConfig(), client, 0, nil)
	if res.clientErr != nil || res.serverErr != nil {
		t.Fatalf("handshake failed: client %v, server %v", res.clientErr, res.serverErr)
	}
	return res.served
}

func pemOf(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}

// selfSigned mints a self-signed certificate for key with the given validity.
func selfSigned(t testing.TB, key crypto.Signer, signer crypto.Signer, isCA bool, notBefore, notAfter time.Time) []byte {
	t.Helper()
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "self"}, NotBefore: notBefore,
		NotAfter: notAfter, DNSNames: []string{"localhost"}, BasicConstraintsValid: true, IsCA: isCA}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), signer)
	if err != nil {
		t.Fatal(err)
	}
	return pemOf("CERTIFICATE", der)
}

func truncate(b []byte) []byte { return b[:len(b)/2] }

// invalidMaterial is every shape a load or a reload must refuse. Each row
// derives its files from a valid generation g, with other another generation.
// A nil entry in a row's files is a file left out of the projection.
func invalidMaterial(t testing.TB, g, other *generation) map[string]map[string][]byte {
	t.Helper()
	now := time.Now()
	expired := newGenerationValid(t, now.Add(-2*time.Hour), now.Add(-time.Hour))
	future := newGenerationValid(t, now.Add(time.Hour), now.Add(2*time.Hour))
	headers := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: map[string]string{"X": "y"},
		Bytes: func() []byte { b, _ := pem.Decode(g.cert); return b.Bytes }()})
	badDER := pemOf("CERTIFICATE", []byte("not a certificate"))
	ca := g.ca.CertPEM
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	row := func(cert, key, caf []byte) map[string][]byte {
		return map[string][]byte{"tls.crt": cert, "tls.key": key, "ca.crt": caf}
	}
	return map[string]map[string][]byte{
		"missing certificate":                 row(nil, g.key, ca),
		"missing key":                         row(g.cert, nil, ca),
		"missing CA":                          row(g.cert, g.key, nil),
		"mismatched key":                      row(g.cert, other.key, ca),
		"certificate garbage prefix":          row(cat([]byte("garbage\n"), g.cert), g.key, ca),
		"certificate garbage middle":          row(cat(g.cert, []byte("garbage\n"), ca), g.key, ca),
		"certificate trailing garbage":        row(cat(g.cert, []byte("garbage")), g.key, ca),
		"truncated intermediate after leaf":   row(cat(g.cert, truncate(ca)), g.key, ca),
		"leaf, truncated block, valid block":  row(cat(g.cert, truncate(ca), []byte("\n"), ca), g.key, ca),
		"malformed DER intermediate":          row(cat(g.cert, badDER), g.key, ca),
		"certificate block with headers":      row(headers, g.key, ca),
		"certificate file holding a key":      row(g.key, g.key, ca),
		"truncated key":                       row(g.cert, truncate(g.key), ca),
		"key file with two blocks":            row(g.cert, cat(g.key, g.key), ca),
		"expired leaf":                        row(expired.cert, expired.key, ca),
		"not-yet-valid leaf":                  row(future.cert, future.key, ca),
		"empty CA":                            row(g.cert, g.key, []byte("\n")),
		"CA valid then truncated":             row(g.cert, g.key, cat(ca, truncate(other.ca.CertPEM))),
		"CA truncated then valid":             row(g.cert, g.key, cat(truncate(other.ca.CertPEM), []byte("\n"), ca)),
		"CA garbage prefix":                   row(g.cert, g.key, cat([]byte("garbage\n"), ca)),
		"CA holding a key":                    row(g.cert, g.key, cat(ca, g.key)),
		"CA malformed DER only":               row(g.cert, g.key, badDER),
		"CA one valid plus one malformed-DER": row(g.cert, g.key, cat(ca, badDER)),
	}
}

func TestLoadRefusesInvalidMaterial(t *testing.T) {
	g, other := newGeneration(t), newGeneration(t)
	for name, files := range invalidMaterial(t, g, other) {
		t.Run(name, func(t *testing.T) {
			p := newProjection(t, contents{})
			p.swapFiles(files)
			src, err := Load(p.files(true), nil)
			if err == nil {
				t.Fatalf("Load accepted %s", name)
			}
			if src != nil {
				t.Errorf("Load returned a Source with its error")
			}
			if !strings.Contains(err.Error(), "TLS") {
				t.Errorf("error %q does not name the TLS file at fault", err)
			}
		})
	}
}

func TestLoadServesTheFiles(t *testing.T) {
	g := newGeneration(t)
	for _, withCA := range []bool{false, true} {
		t.Run(fmt.Sprintf("withCA=%v", withCA), func(t *testing.T) {
			f := loaded(t, g, withCA)
			cfg, pool, gen := f.src.Active()
			if gen != 1 || len(cfg.Certificates) != 1 {
				t.Fatalf("Active = %d certificates, generation %d; want 1, 1", len(cfg.Certificates), gen)
			}
			if withCA != (pool != nil) || withCA != (cfg.ClientAuth == tls.RequireAndVerifyClientCert) {
				t.Fatalf("client auth %v with pool %v, want verified client certificates exactly when a CA is configured", cfg.ClientAuth, pool)
			}
			if got := servedSerial(t, f.src, &g.client); got != g.serial {
				t.Fatalf("served %s, want %s", got, g.serial)
			}
			if f.src.ServerConfig().GetConfigForClient == nil || f.src.ServerConfig().Certificates != nil {
				t.Fatal("the listener config must defer to the active configuration")
			}
		})
	}
}

func TestPEMFramingAccepts(t *testing.T) {
	g := newGeneration(t)
	crlf := func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("\n"), []byte("\r\n")) }
	now := time.Now()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	expiredCA := selfSigned(t, caKey, caKey, true, now.Add(-2*time.Hour), now.Add(-time.Hour))
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	rsaCert := selfSigned(t, rsaKey, rsaKey, false, now.Add(-time.Minute), now.Add(time.Hour))
	ecKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ecCert := selfSigned(t, ecKey, ecKey, false, now.Add(-time.Minute), now.Add(time.Hour))
	pkcs8, err := x509.MarshalPKCS8PrivateKey(ecKey)
	if err != nil {
		t.Fatal(err)
	}
	other := newGeneration(t)
	cases := map[string]contents{
		"CRLF":                      {crlf(g.cert), crlf(g.key), crlf(g.ca.CertPEM)},
		"no trailing newline":       {bytes.TrimRight(g.cert, "\n"), bytes.TrimRight(g.key, "\n"), bytes.TrimRight(g.ca.CertPEM, "\n")},
		"blank lines between":       {append(append(append([]byte{}, g.cert...), "\n\n"...), g.ca.CertPEM...), g.key, append(append([]byte("\n"), g.ca.CertPEM...), "\n\n"...)},
		"CA bundle with expired CA": {g.cert, g.key, bytes.Join([][]byte{expiredCA, g.ca.CertPEM, other.ca.CertPEM}, []byte("\n"))},
		"RSA PKCS#1 key":            {rsaCert, pemOf("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(rsaKey)), g.ca.CertPEM},
		"PKCS#8 key":                {ecCert, pemOf("PRIVATE KEY", pkcs8), g.ca.CertPEM},
		"EC key":                    {g.cert, g.key, g.ca.CertPEM},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p := newProjection(t, c)
			if _, err := Load(p.files(true), nil); err != nil {
				t.Fatalf("Load refused legitimate material: %v", err)
			}
		})
	}
}

func TestCheckAppliesValidReplacement(t *testing.T) {
	a, b := newGeneration(t), newGeneration(t)
	f := loaded(t, a, false)
	if out := f.src.Check(); out != Unchanged {
		t.Fatalf("Check over the loaded files = %v, want unchanged", out)
	}
	f.proj.swap(b.contents())
	if out := f.src.Check(); out != Applied {
		t.Fatalf("Check after a valid swap = %v, want applied", out)
	}
	if got := servedSerial(t, f.src, nil); got != b.serial {
		t.Fatalf("a new handshake was served %s, want the replacement %s", got, b.serial)
	}
	if _, _, gen := f.src.Active(); gen != 2 {
		t.Fatalf("generation %d after one reload, want 2", gen)
	}
	if out := f.src.Check(); out != Unchanged {
		t.Fatalf("Check after applying = %v, want unchanged", out)
	}
}

func TestCheckKeepsLastValidOnRefusal(t *testing.T) {
	a, other := newGeneration(t), newGeneration(t)
	for name, files := range invalidMaterial(t, a, other) {
		t.Run(name, func(t *testing.T) {
			f := loaded(t, a, true)
			f.proj.swapFiles(files)
			if out := f.src.Check(); out != Refused {
				t.Fatalf("Check = %v, want refused", out)
			}
			if got := servedSerial(t, f.src, &a.client); got != a.serial {
				t.Fatalf("after a refused update a handshake was served %s, want the last valid %s", got, a.serial)
			}
			if _, _, gen := f.src.Active(); gen != 1 {
				t.Fatalf("generation %d after a refusal, want 1", gen)
			}
		})
	}
	// The kubelet deletes the old generation directory right after the swap; a
	// check whose second read lands after the deletion must keep the active
	// configuration.
	t.Run("old generation deleted between reads", func(t *testing.T) {
		b := newGeneration(t)
		f := loaded(t, a, true)
		reads := 0
		f.src.read = func(path string) ([]byte, error) {
			reads++
			if reads == 4 { // the second read's certificate
				return nil, os.ErrNotExist
			}
			return os.ReadFile(path)
		}
		f.proj.swap(b.contents())
		if out := f.src.Check(); out != Refused && out != InFlux {
			t.Fatalf("Check = %v, want refused or in-flux", out)
		}
		if got := servedSerial(t, f.src, &a.client); got != a.serial {
			t.Fatalf("served %s, want the last valid %s", got, a.serial)
		}
	})
}

func TestCheckNeverPublishesBeforeValidation(t *testing.T) {
	a := newGeneration(t)
	expired := newGenerationValid(t, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	f := loaded(t, a, false)
	held, release := make(chan struct{}), make(chan struct{})
	f.src.validating = func() {
		close(held)
		<-release
	}
	f.proj.swap(expired.contents())
	out := make(chan Outcome, 1)
	go func() { out <- f.src.Check() }()
	<-held
	if got := servedSerial(t, f.src, nil); got != a.serial {
		t.Errorf("while the candidate was being validated a handshake was served %s, want the active %s", got, a.serial)
	}
	close(release)
	if o := <-out; o != Refused {
		t.Fatalf("Check = %v, want refused", o)
	}
}

func TestCheckAppliesWhenRefusedMaterialBecomesValid(t *testing.T) {
	a := newGeneration(t)
	future := newGenerationValid(t, time.Now().Add(time.Hour), time.Now().Add(3*time.Hour))
	f := loaded(t, a, false)
	f.proj.swap(future.contents())
	for i := 0; i < 2; i++ {
		if out := f.src.Check(); out != Refused {
			t.Fatalf("Check before NotBefore = %v, want refused", out)
		}
	}
	f.clk.add(90 * time.Minute)
	if out := f.src.Check(); out != Applied {
		t.Fatalf("Check inside the window with the files unchanged = %v, want applied", out)
	}
	if got := servedSerial(t, f.src, nil); got != future.serial {
		t.Fatalf("served %s, want %s", got, future.serial)
	}
}

func TestRefusalLoggingIsConsecutive(t *testing.T) {
	a, other := newGeneration(t), newGeneration(t)
	expired := newGenerationValid(t, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	mismatched := contents{cert: a.cert, key: other.key}
	stale := contents{cert: expired.cert, key: expired.key}
	errLines := func(f *fixture) int { return f.log.count(slog.LevelError) }

	t.Run("same refusal", func(t *testing.T) {
		f := loaded(t, a, false)
		f.proj.swap(mismatched)
		for i := 0; i < 3; i++ {
			f.src.Check()
		}
		if n := errLines(f); n != 1 {
			t.Fatalf("%d error lines for one standing refusal, want 1", n)
		}
	})
	t.Run("persistent missing file", func(t *testing.T) {
		f := loaded(t, a, false)
		f.proj.swapFiles(map[string][]byte{"tls.crt": a.cert})
		for i := 0; i < 3; i++ {
			if out := f.src.Check(); out != Refused {
				t.Fatalf("Check = %v, want refused", out)
			}
		}
		if n := errLines(f); n != 1 {
			t.Fatalf("%d error lines for one missing file, want 1", n)
		}
	})
	t.Run("alternating causes", func(t *testing.T) {
		f := loaded(t, a, false)
		for _, c := range []contents{mismatched, stale, mismatched} {
			f.proj.swap(c)
			f.src.Check()
		}
		if n := errLines(f); n != 3 {
			t.Fatalf("%d error lines for three alternating refusals, want 3", n)
		}
	})
	t.Run("in-flux ends the run", func(t *testing.T) {
		f := loaded(t, a, false)
		f.proj.swap(mismatched)
		f.src.Check()
		flux := true
		reads := 0
		f.src.read = func(path string) ([]byte, error) {
			reads++
			if flux && reads == 4 {
				return []byte("changing"), nil
			}
			return os.ReadFile(path)
		}
		if out := f.src.Check(); out != InFlux {
			t.Fatalf("Check = %v, want in-flux", out)
		}
		flux = false
		f.src.Check()
		if n := errLines(f); n != 2 {
			t.Fatalf("%d error lines for refused, in-flux, refused, want 2", n)
		}
	})
	t.Run("unchanged ends the run", func(t *testing.T) {
		f := loaded(t, a, false)
		f.proj.swap(mismatched)
		f.src.Check()
		f.proj.swap(contents{cert: a.cert, key: a.key})
		if out := f.src.Check(); out != Unchanged {
			t.Fatalf("Check = %v, want unchanged", out)
		}
		f.proj.swap(mismatched)
		f.src.Check()
		if n := errLines(f); n != 2 {
			t.Fatalf("%d error lines for refused, unchanged, refused, want 2", n)
		}
	})
	t.Run("applied ends the run", func(t *testing.T) {
		f := loaded(t, a, false)
		f.proj.swap(mismatched)
		f.src.Check()
		f.proj.swap(contents{cert: other.cert, key: other.key})
		if out := f.src.Check(); out != Applied {
			t.Fatalf("Check = %v, want applied", out)
		}
		f.proj.swap(contents{cert: a.cert, key: other.key})
		f.src.Check()
		if n := errLines(f); n != 2 {
			t.Fatalf("%d error lines for refused, applied, refused, want 2", n)
		}
	})
}

func TestCheckInFlux(t *testing.T) {
	a, b := newGeneration(t), newGeneration(t)
	f := loaded(t, a, false)
	f.proj.swap(b.contents())
	reads := 0
	f.src.read = func(path string) ([]byte, error) {
		reads++
		data, err := os.ReadFile(path)
		if reads == 3 { // the second read's certificate differs from the first's
			return append(data, '\n'), err
		}
		return data, err
	}
	if out := f.src.Check(); out != InFlux {
		t.Fatalf("Check with disagreeing reads = %v, want in-flux", out)
	}
	if got := servedSerial(t, f.src, nil); got != a.serial {
		t.Fatalf("served %s while the files were changing, want the active %s", got, a.serial)
	}
	if out := f.src.Check(); out != Applied {
		t.Fatalf("Check once the reads agree = %v, want applied", out)
	}
}

func TestClientCARollover(t *testing.T) {
	a, b := newGeneration(t), newGeneration(t)
	f := loaded(t, a, true)

	// A connection admitted under CA a, still open across the reload.
	sc, cc := net.Pipe()
	defer sc.Close()
	defer cc.Close()
	srv := tls.Server(sc, f.src.ServerConfig())
	cli := tls.Client(cc, &tls.Config{InsecureSkipVerify: true, //nolint:gosec // see handshake
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &a.client, nil }})
	errc := make(chan error, 1)
	go func() { errc <- srv.Handshake() }()
	if err := cli.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}

	f.proj.swap(contents{cert: a.cert, key: a.key, ca: b.ca.CertPEM})
	if out := f.src.Check(); out != Applied {
		t.Fatalf("Check after a CA swap = %v, want applied", out)
	}
	if res := handshake(t, f.src.ServerConfig(), &a.client, 0, nil); res.serverErr == nil {
		t.Fatal("a new handshake with a certificate from the removed CA was accepted")
	}
	if res := handshake(t, f.src.ServerConfig(), &b.client, 0, nil); res.serverErr != nil || res.clientErr != nil {
		t.Fatalf("a new handshake with a certificate from the new CA failed: %v / %v", res.serverErr, res.clientErr)
	}
	go func() { _, err := srv.Write([]byte("still here")); errc <- err }()
	buf := make([]byte, 10)
	if _, err := cli.Read(buf); err != nil || string(buf) != "still here" {
		t.Fatalf("the connection admitted before the reload stopped working: %q, %v", buf, err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestResumptionAcrossReload(t *testing.T) {
	for _, v := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		name := tls.VersionName(v)
		t.Run(name+" server pair reloaded", func(t *testing.T) {
			a, b := newGeneration(t), newGeneration(t)
			f := loaded(t, a, false)
			cache := tls.NewLRUClientSessionCache(4)
			if res := handshake(t, f.src.ServerConfig(), nil, v, cache); res.clientErr != nil || res.resumed {
				t.Fatalf("first handshake: %v, resumed %v", res.clientErr, res.resumed)
			}
			f.proj.swap(contents{cert: b.cert, key: b.key})
			if out := f.src.Check(); out != Applied {
				t.Fatalf("Check = %v, want applied", out)
			}
			res := handshake(t, f.src.ServerConfig(), nil, v, cache)
			if res.clientErr != nil || res.serverErr != nil || !res.resumed {
				t.Fatalf("a session from before the reload did not resume: %v / %v, resumed %v", res.clientErr, res.serverErr, res.resumed)
			}
		})
		t.Run(name+" client CA removed", func(t *testing.T) {
			a, b := newGeneration(t), newGeneration(t)
			f := loaded(t, a, true)
			cache := tls.NewLRUClientSessionCache(4)
			if res := handshake(t, f.src.ServerConfig(), &a.client, v, cache); res.clientErr != nil || res.serverErr != nil {
				t.Fatalf("first handshake: %v / %v", res.clientErr, res.serverErr)
			}
			f.proj.swap(contents{cert: a.cert, key: a.key, ca: b.ca.CertPEM})
			if out := f.src.Check(); out != Applied {
				t.Fatalf("Check = %v, want applied", out)
			}
			res := handshake(t, f.src.ServerConfig(), &a.client, v, cache)
			if res.serverErr == nil {
				t.Fatalf("a session admitted under a removed CA was accepted again (resumed %v)", res.resumed)
			}
		})
	}
}

func TestConcurrentHandshakesDuringReloads(t *testing.T) {
	a, b := newGeneration(t), newGeneration(t)
	f := loaded(t, a, true)
	gens := map[string]*generation{a.serial: a, b.serial: b}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var handshakes atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			client := a
			if w%2 == 1 {
				client = b
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				res := handshake(t, f.src.ServerConfig(), &client.client, 0, nil)
				served := gens[res.served]
				if res.clientErr != nil && served == nil {
					continue // refused before the client saw the certificate
				}
				handshakes.Add(1)
				if served == nil {
					t.Errorf("served a certificate from neither generation: %s", res.served)
					return
				}
				// The client is accepted exactly when the CA that judged it is the
				// served pair's own: certificate, key and CA came from one snapshot.
				if (res.serverErr == nil) != (served == client) {
					t.Errorf("served %s's pair but judged the client against the other CA (server error %v)", res.served, res.serverErr)
					return
				}
			}
		}(w)
	}
	for i := 0; i < 40; i++ {
		next := b
		if i%2 == 1 {
			next = a
		}
		f.proj.swap(next.contents())
		if out := f.src.Check(); out != Applied {
			t.Errorf("Check = %v, want applied", out)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if handshakes.Load() == 0 {
		t.Fatal("no handshake completed")
	}
}

// watcherGoroutines counts goroutines running this package's watcher.
func watcherGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "tlsreload.(*Source).watch(")
}

// awaitWatchers polls until the watcher count is want: a goroutine just
// started, or just exited, is not in the dump at once.
func awaitWatchers(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := watcherGoroutines()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d watcher goroutines, want %d", n, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWatcherAppliesAndStopJoins(t *testing.T) {
	a, b := newGeneration(t), newGeneration(t)
	f := loaded(t, a, false)
	var armed atomic.Bool
	held, release := make(chan struct{}), make(chan struct{})
	f.src.read = func(path string) ([]byte, error) {
		if armed.CompareAndSwap(true, false) {
			close(held)
			<-release
		}
		return os.ReadFile(path)
	}
	f.src.Start(5 * time.Millisecond)
	t.Cleanup(func() { close(release); f.src.Stop() })
	awaitWatchers(t, 1)
	f.proj.swap(b.contents())
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, _, gen := f.src.Active(); gen == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the watcher never applied the swap")
		}
		time.Sleep(5 * time.Millisecond)
	}
	armed.Store(true)
	<-held
	stopped := make(chan struct{})
	go func() {
		f.src.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while the watcher was inside Check")
	case <-time.After(500 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the watcher's Check finished")
	}
	awaitWatchers(t, 0)
	f.src.Stop()
	loaded(t, a, false).src.Stop()
}

func TestActiveExpiryWarnsOnce(t *testing.T) {
	a, other := newGeneration(t), newGeneration(t)
	warnings := func(f *fixture) int { return f.log.count(slog.LevelWarn) }
	firstTick := map[string]func(f *fixture) Outcome{
		"unchanged": func(f *fixture) Outcome { return f.src.Check() },
		"refused": func(f *fixture) Outcome {
			f.proj.swap(contents{cert: a.cert, key: other.key})
			return f.src.Check()
		},
		"in-flux": func(f *fixture) Outcome {
			reads := 0
			f.src.read = func(path string) ([]byte, error) {
				reads++
				data, err := os.ReadFile(path)
				if reads == 3 {
					data = append(data, '\n')
				}
				return data, err
			}
			return f.src.Check()
		},
	}
	want := map[string]Outcome{"unchanged": Unchanged, "refused": Refused, "in-flux": InFlux}
	for name, tick := range firstTick {
		t.Run(name, func(t *testing.T) {
			f := loaded(t, a, false)
			f.clk.add(2 * time.Hour)
			if out := tick(f); out != want[name] {
				t.Fatalf("first tick = %v, want %v", out, want[name])
			}
			if n := warnings(f); n != 1 {
				t.Fatalf("%d warnings after the first %s tick past NotAfter, want 1", n, name)
			}
			f.src.read = os.ReadFile
			f.src.Check()
			f.src.Check()
			if n := warnings(f); n != 1 {
				t.Fatalf("%d warnings after further ticks, want still 1", n)
			}
			if got := servedSerial(t, f.src, nil); got != a.serial {
				t.Fatalf("served %s, want the expired-but-retained %s", got, a.serial)
			}
		})
	}
	t.Run("a new material warns again", func(t *testing.T) {
		f := loaded(t, a, false)
		f.clk.add(2 * time.Hour)
		f.src.Check()
		fresh := newGenerationValid(t, f.clk.now().Add(-time.Minute), f.clk.now().Add(time.Hour))
		f.proj.swap(contents{cert: fresh.cert, key: fresh.key})
		if out := f.src.Check(); out != Applied {
			t.Fatalf("Check = %v, want applied", out)
		}
		f.clk.add(2 * time.Hour)
		f.src.Check()
		if n := warnings(f); n != 2 {
			t.Fatalf("%d warnings for two expired materials, want 2", n)
		}
	})
}

func TestStopWithoutStart(t *testing.T) {
	f := loaded(t, newGeneration(t), false)
	done := make(chan struct{})
	go func() { f.src.Stop(); f.src.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop without Start blocked")
	}
	f.src.Start(time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if n := watcherGoroutines(); n != 0 {
		t.Fatalf("Start after Stop started %d watcher goroutines", n)
	}
}
