// Package tlsreload owns the MQTT listener's TLS material: the certificate, the
// private key and the optional client CA, read from files, validated as one
// unit, and replaced while the process runs when the files change. A
// Kubernetes Secret projected into the pod is updated in place by the kubelet,
// so a renewed certificate reaches new handshakes without a restart and without
// a rollout controller.
//
// The listener holds one *tls.Config for its whole life (ServerConfig). It is
// never mutated: each handshake asks it, through GetConfigForClient, for the
// active per-handshake configuration, loaded from an atomic pointer. That gives
// every handshake one consistent certificate/key/CA triple, and it leaves every
// established connection on the configuration it was admitted under, because a
// tls.Conn keeps the configuration its handshake selected.
//
// The active configuration is replaced only by a complete replacement that has
// validated; anything else (a missing file, a malformed block, a key that does
// not match the certificate, a leaf outside its validity window) is refused and
// the last valid configuration stays. The same validation decides startup, where
// a refusal is fatal.
package tlsreload

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Files names the listener's TLS material. An empty CA means no client
// authentication; a configured one makes client certificates required and
// verified, and stays required: a CA file that later goes missing is a refused
// update, never a downgrade to a listener that verifies nothing.
type Files struct{ Cert, Key, CA string }

// Outcome is what one Check did.
type Outcome int

const (
	// Unchanged: the files hold exactly the active material.
	Unchanged Outcome = iota
	// InFlux: two consecutive reads disagreed, so the files are being replaced;
	// the active configuration stays and the next check reads again.
	InFlux
	// Applied: a validated replacement is now active.
	Applied
	// Refused: the files could not be read or did not validate; the active
	// configuration stays.
	Refused
)

func (o Outcome) String() string {
	switch o {
	case Unchanged:
		return "unchanged"
	case InFlux:
		return "in-flux"
	case Applied:
		return "applied"
	case Refused:
		return "refused"
	}
	return fmt.Sprintf("Outcome(%d)", int(o))
}

// contents is the raw bytes of the three files as one read saw them.
type contents struct{ cert, key, ca []byte }

func (c contents) equal(o contents) bool {
	return bytes.Equal(c.cert, o.cert) && bytes.Equal(c.key, o.key) && bytes.Equal(c.ca, o.ca)
}

// material is one validated configuration and the bytes it was built from. It
// is immutable once published, except expiryWarned, which only Check touches
// under Source.mu.
type material struct {
	cfg          *tls.Config
	pool         *x509.CertPool // nil without a client CA
	gen          uint64         // publication count; 1 for the startup material
	src          contents
	notAfter     time.Time
	subject      string
	serial       string
	expiryWarned bool
}

// Source holds the active validated configuration and replaces it as the files
// change.
type Source struct {
	files Files
	log   *slog.Logger
	outer *tls.Config

	// Injection points for tests: the clock validation runs against, the file
	// reader, and a barrier inside build between parsing and validation.
	now        func() time.Time
	read       func(string) ([]byte, error)
	validating func()

	cur      atomic.Pointer[material]
	refusals atomic.Uint64

	// mu serializes Check as a whole: the reads, the build, the publication, the
	// refusal-log state and the expiry warning. Handshakes never take it; they
	// load cur.
	mu          sync.Mutex
	gen         uint64
	lastRefusal string // the cause of the immediately preceding outcome, if it was a refusal

	lifeMu sync.Mutex
	quit   chan struct{}
	done   chan struct{}
	closed bool
}

// Load reads and validates the files and returns a Source serving them. Any
// refusal is returned as the error: at startup there is no last valid
// configuration to keep, and a broken TLS setup must never fall back to
// plaintext. Load starts nothing and holds no descriptor; Start begins watching.
func Load(files Files, log *slog.Logger) (*Source, error) {
	s := newSource(files, log)
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func newSource(files Files, log *slog.Logger) *Source {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	s := &Source{files: files, log: log, now: time.Now, read: os.ReadFile}
	// The listener's configuration. Only GetConfigForClient and the session
	// ticket keys Go keeps on it are used: the handshake continues on the
	// configuration the callback returns, and tickets stay decryptable across
	// reloads because their keys live here, not on the replaced configurations.
	s.outer = &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return s.cur.Load().cfg, nil
		},
	}
	return s
}

func (s *Source) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := s.readAll()
	if err != nil {
		return err
	}
	m, err := s.build(c, s.now())
	if err != nil {
		return err
	}
	s.publish(m)
	return nil
}

// ServerConfig is the listener's configuration. It is the same object for the
// Source's whole life and must not be modified.
func (s *Source) ServerConfig() *tls.Config { return s.outer }

// Active returns the active per-handshake configuration, its client-CA pool
// (nil without a client CA) and its publication generation.
func (s *Source) Active() (*tls.Config, *x509.CertPool, uint64) {
	m := s.cur.Load()
	return m.cfg, m.pool, m.gen
}

// Refusals counts the checks that refused what the files held.
func (s *Source) Refusals() uint64 { return s.refusals.Load() }

// Check reads the files once and replaces the active configuration if they hold
// a different, valid triple.
func (s *Source) Check() Outcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.check()
	if out != Refused {
		s.lastRefusal = ""
	}
	s.warnIfActiveExpired()
	return out
}

func (s *Source) check() Outcome {
	// Two reads must agree before anything is judged. The kubelet replaces a
	// projection by retargeting the ..data symlink and then deleting the old
	// generation, so a read that crosses the swap sees bytes from both, or a
	// missing file. This is a stability check, not a transaction: a mixed triple
	// that is stable and valid is applied, and the next check replaces it.
	first, err := s.readAll()
	if err != nil {
		return s.refuse(err)
	}
	second, err := s.readAll()
	if err != nil {
		return s.refuse(err)
	}
	if !first.equal(second) {
		return InFlux
	}
	if first.equal(s.cur.Load().src) {
		return Unchanged
	}
	// Refused content is validated again on every check rather than remembered:
	// validity depends on the clock, so a replacement refused before its
	// NotBefore applies once that time arrives, with the files unchanged.
	m, err := s.build(first, s.now())
	if err != nil {
		return s.refuse(err)
	}
	s.publish(m)
	s.log.Info("MQTT TLS configuration reloaded", "subject", m.subject, "serial", m.serial,
		"not_after", m.notAfter, "generation", m.gen)
	return Applied
}

// publish makes m the active configuration. It is the only store to cur, and
// it runs only with a material build has fully validated.
func (s *Source) publish(m *material) {
	s.gen++
	m.gen = s.gen
	s.cur.Store(m)
}

func (s *Source) refuse(err error) Outcome {
	s.refusals.Add(1)
	cause := err.Error()
	// One line per run of identical refusals: a projection that stays broken
	// does not log every interval, and any other outcome in between ends the run.
	if cause != s.lastRefusal {
		s.log.Error("MQTT TLS reload refused; keeping the active configuration", "err", cause)
	}
	s.lastRefusal = cause
	return Refused
}

// warnIfActiveExpired warns once per material when the active leaf has passed
// its NotAfter. The material stays: there is nothing valid to replace it with,
// and the warning is the operator's signal that renewal has stopped arriving.
func (s *Source) warnIfActiveExpired() {
	m := s.cur.Load()
	if m.expiryWarned || !s.now().After(m.notAfter) {
		return
	}
	m.expiryWarned = true
	s.log.Warn("MQTT TLS certificate has expired and no valid replacement is on disk",
		"subject", m.subject, "serial", m.serial, "not_after", m.notAfter)
}

func (s *Source) readAll() (contents, error) {
	var c contents
	var err error
	if c.cert, err = s.read(s.files.Cert); err != nil {
		return contents{}, fmt.Errorf("read TLS certificate: %w", err)
	}
	if c.key, err = s.read(s.files.Key); err != nil {
		return contents{}, fmt.Errorf("read TLS key: %w", err)
	}
	if s.files.CA != "" {
		if c.ca, err = s.read(s.files.CA); err != nil {
			return contents{}, fmt.Errorf("read TLS CA: %w", err)
		}
	}
	return c, nil
}

// build validates one triple as a unit and returns the configuration it
// describes. Nothing is published here; the caller publishes only a nil-error
// result.
func (s *Source) build(c contents, now time.Time) (*material, error) {
	certBlocks, err := pemBlocks(c.cert, "CERTIFICATE")
	if err != nil {
		return nil, fmt.Errorf("TLS certificate %s: %w", s.files.Cert, err)
	}
	keyBlocks, err := pemBlocks(c.key, "PRIVATE KEY", "RSA PRIVATE KEY", "EC PRIVATE KEY")
	if err != nil {
		return nil, fmt.Errorf("TLS key %s: %w", s.files.Key, err)
	}
	if len(keyBlocks) != 1 {
		return nil, fmt.Errorf("TLS key %s: %d PEM blocks, want exactly one private key", s.files.Key, len(keyBlocks))
	}
	// X509KeyPair checks that the key matches the leaf's public key.
	pair, err := tls.X509KeyPair(c.cert, c.key)
	if err != nil {
		return nil, fmt.Errorf("TLS certificate %s and key %s: %w", s.files.Cert, s.files.Key, err)
	}
	if len(pair.Certificate) != len(certBlocks) {
		return nil, fmt.Errorf("TLS certificate %s: %d PEM blocks but %d certificates loaded", s.files.Cert,
			len(certBlocks), len(pair.Certificate))
	}
	// X509KeyPair parses only the leaf; an intermediate it would serve must parse too.
	chain := make([]*x509.Certificate, len(pair.Certificate))
	for i, der := range pair.Certificate {
		if chain[i], err = x509.ParseCertificate(der); err != nil {
			return nil, fmt.Errorf("TLS certificate %s: certificate %d: %w", s.files.Cert, i, err)
		}
	}
	leaf := chain[0]
	if s.validating != nil {
		s.validating()
	}
	if now.Before(leaf.NotBefore) {
		return nil, fmt.Errorf("TLS certificate %s: not valid before %s", s.files.Cert, leaf.NotBefore.UTC().Format(time.RFC3339))
	}
	if now.After(leaf.NotAfter) {
		return nil, fmt.Errorf("TLS certificate %s: expired at %s", s.files.Cert, leaf.NotAfter.UTC().Format(time.RFC3339))
	}
	pair.Leaf = leaf
	cfg := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	var pool *x509.CertPool
	if s.files.CA != "" {
		caBlocks, err := pemBlocks(c.ca, "CERTIFICATE")
		if err != nil {
			return nil, fmt.Errorf("TLS CA %s: %w", s.files.CA, err)
		}
		pool = x509.NewCertPool()
		for i, b := range caBlocks {
			ca, err := x509.ParseCertificate(b.Bytes)
			if err != nil {
				return nil, fmt.Errorf("TLS CA %s: certificate %d: %w", s.files.CA, i, err)
			}
			pool.AddCert(ca)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return &material{
		cfg: cfg, pool: pool, src: c, notAfter: leaf.NotAfter,
		subject: leaf.Subject.String(), serial: leaf.SerialNumber.String(),
	}, nil
}

var (
	pemBegin           = []byte("-----BEGIN")
	errOutsideBlock    = errors.New("data outside a PEM block")
	errMalformedBlock  = errors.New("malformed PEM block")
	errNoBlock         = errors.New("no PEM block")
	errHeadersNotTaken = errors.New("PEM block with headers")
)

// pemBlocks decodes data as PEM blocks separated only by whitespace, and
// accounts for every byte. encoding/pem alone does not: Decode skips a
// malformed block and returns the next well-formed one, and tls.X509KeyPair
// stops quietly at the first block it cannot decode, so a truncated
// intermediate would leave a shorter chain that still loads, and a truncated CA
// certificate would vanish from the pool.
func pemBlocks(data []byte, types ...string) ([]*pem.Block, error) {
	var blocks []*pem.Block
	rest := data
	for {
		rest = bytes.TrimLeft(rest, " \t\r\n")
		if len(rest) == 0 {
			break
		}
		if !bytes.HasPrefix(rest, []byte("-----BEGIN ")) {
			return nil, errOutsideBlock
		}
		b, next := pem.Decode(rest)
		if b == nil {
			return nil, errMalformedBlock
		}
		// A block Decode skipped leaves its BEGIN line inside the consumed span.
		// The payload of a well-formed block is base64, which has no '-'.
		if bytes.Count(rest[:len(rest)-len(next)], pemBegin) != 1 {
			return nil, errMalformedBlock
		}
		if len(b.Headers) != 0 {
			return nil, errHeadersNotTaken
		}
		ok := false
		for _, t := range types {
			ok = ok || b.Type == t
		}
		if !ok {
			return nil, fmt.Errorf("unexpected PEM block type %q", b.Type)
		}
		blocks = append(blocks, b)
		rest = next
	}
	if len(blocks) == 0 {
		return nil, errNoBlock
	}
	return blocks, nil
}

// Start begins checking the files every interval on one goroutine. It is a
// no-op once the Source has been started or stopped.
func (s *Source) Start(interval time.Duration) {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.quit != nil || s.closed {
		return
	}
	s.quit, s.done = make(chan struct{}), make(chan struct{})
	go s.watch(interval, s.quit, s.done)
}

// Stop ends the watcher and returns once its goroutine has exited, including a
// Check it was running. It is idempotent, and a no-op without Start.
func (s *Source) Stop() {
	s.lifeMu.Lock()
	quit, done := s.quit, s.done
	if !s.closed && quit != nil {
		close(quit)
	}
	s.closed = true
	s.lifeMu.Unlock()
	if done != nil {
		<-done
	}
}

func (s *Source) watch(interval time.Duration, quit <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-quit:
			return
		case <-t.C:
			s.Check()
		}
	}
}
