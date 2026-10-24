package imagesmoke

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
)

const (
	binaryPath = "usr/local/bin/mqtt-nats-broker"
	bundlePath = "etc/ssl/certs/ca-certificates.crt"
)

// imageFiles applies the image's layers, in manifest order and honoring OCI
// whiteouts, and returns the resulting entries by path with their bodies. It reads
// `docker save` rather than `docker export`: an exported container also carries what
// the daemon mounts into it (/etc/hosts, /etc/resolv.conf, /.dockerenv, /dev, /proc).
func imageFiles(t *testing.T, image string) (map[string]*tar.Header, map[string][]byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	saved, err := docker(ctx, nil, "save", image)
	if err != nil {
		t.Fatal(err)
	}
	blobs := map[string][]byte{}
	tr := tar.NewReader(strings.NewReader(saved))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			b, err := io.ReadAll(tr)
			if err != nil {
				t.Fatal(err)
			}
			blobs[h.Name] = b
		}
	}
	var manifest []struct{ Layers []string }
	if err := json.Unmarshal(blobs["manifest.json"], &manifest); err != nil || len(manifest) != 1 {
		t.Fatalf("docker save manifest.json: %v (%d images)", err, len(manifest))
	}
	headers := map[string]*tar.Header{}
	bodies := map[string][]byte{}
	for _, layer := range manifest[0].Layers {
		var r io.Reader = bytes.NewReader(blobs[layer])
		if b := blobs[layer]; len(b) > 1 && b[0] == 0x1f && b[1] == 0x8b {
			zr, err := gzip.NewReader(r)
			if err != nil {
				t.Fatal(err)
			}
			r = zr
		}
		lr := tar.NewReader(r)
		for {
			h, err := lr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("layer %s: %v", layer, err)
			}
			name := strings.TrimSuffix(strings.TrimPrefix(path.Clean("/"+h.Name), "/"), "/")
			dir, base := path.Split(name)
			switch {
			case base == ".wh..wh..opq":
				for p := range headers {
					if strings.HasPrefix(p, dir) {
						delete(headers, p)
						delete(bodies, p)
					}
				}
				continue
			case strings.HasPrefix(base, ".wh."):
				gone := dir + strings.TrimPrefix(base, ".wh.")
				for p := range headers {
					if p == gone || strings.HasPrefix(p, gone+"/") {
						delete(headers, p)
						delete(bodies, p)
					}
				}
				continue
			}
			headers[name] = h
			if h.Typeflag == tar.TypeReg {
				b, err := io.ReadAll(lr)
				if err != nil {
					t.Fatal(err)
				}
				bodies[name] = b
			}
		}
	}
	return headers, bodies
}

// TestImageRunsAsNumericNonRootFromScratch: the image holds the binary and the CA
// bundle and nothing else, declares a numeric non-root user and group (what
// runAsNonRoot can verify), and that user can execute the one and read the other.
func TestImageRunsAsNumericNonRootFromScratch(t *testing.T) {
	f := newFixture(t)

	user := strings.TrimSpace(mustDocker(t, "image", "inspect", "--format", "{{.Config.User}}", f.image))
	if want := fmt.Sprintf("%d:%d", runtimeUID, runtimeGID); user != want || !regexp.MustCompile(`^[1-9][0-9]*:[1-9][0-9]*$`).MatchString(user) {
		t.Errorf("image user = %q, want %q", user, want)
	}

	headers, bodies := imageFiles(t, f.image)
	var files []string
	for p, h := range headers {
		if h.Typeflag != tar.TypeDir {
			files = append(files, p)
		}
	}
	if len(files) != 2 || headers[binaryPath] == nil || headers[bundlePath] == nil {
		t.Errorf("image files = %v, want exactly %s and %s", files, binaryPath, bundlePath)
	}
	if h := headers[binaryPath]; h != nil && (h.Typeflag != tar.TypeReg || h.Uid != 0 || h.Gid != 0 || h.Mode&0o777 != 0o755) {
		t.Errorf("binary: type %c uid %d gid %d mode %o, want a root-owned 0755 file", h.Typeflag, h.Uid, h.Gid, h.Mode&0o777)
	}
	if h := headers[bundlePath]; h != nil && (h.Typeflag != tar.TypeReg || h.Uid != 0 || h.Gid != 0 || h.Mode&0o777 != 0o644) {
		t.Errorf("CA bundle: type %c uid %d gid %d mode %o, want a root-owned 0644 file", h.Typeflag, h.Uid, h.Gid, h.Mode&0o777)
	}
	// Every directory the layers carry on the way to either file must be traversable
	// by a user that is neither its owner nor in its group.
	for _, p := range []string{"etc", "etc/ssl", "etc/ssl/certs", "usr", "usr/local", "usr/local/bin"} {
		if h := headers[p]; h != nil && h.Mode&0o001 == 0 {
			t.Errorf("directory %s mode %o is not traversable by the runtime user", p, h.Mode&0o777)
		}
	}
	certs := 0
	for rest := bodies[bundlePath]; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			certs++
		}
	}
	if certs < 100 {
		t.Errorf("CA bundle holds %d certificates, want a system bundle (at least 100)", certs)
	}

	f.startNATS(nil, nil)
	b := f.runBroker(nil, brokerArgs())
	b.waitReady()
	// The kernel's record of PID 1, read from inside the broker's PID namespace: the
	// daemon host's ps cannot be relied on for numeric columns.
	status := mustDocker(t, "run", "--rm", "--pid", "container:"+b.name, helperImage, "cat", "/proc/1/status")
	for _, want := range []string{
		fmt.Sprintf("Uid:\t%[1]d\t%[1]d\t%[1]d\t%[1]d", runtimeUID),
		fmt.Sprintf("Gid:\t%[1]d\t%[1]d\t%[1]d\t%[1]d", runtimeGID),
	} {
		if !strings.Contains(status, want+"\n") {
			t.Errorf("broker PID 1 status lacks %q:\n%s", want, status)
		}
	}
}

// natsTLS mints a CA and a server certificate for the fixture's `nats` alias.
func natsTLS(t *testing.T) (ca *testcerts.Authority, certPEM, keyPEM []byte) {
	t.Helper()
	ca, err := testcerts.NewAuthority("imagesmoke NATS CA")
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err = ca.Issue("nats", "nats")
	if err != nil {
		t.Fatal(err)
	}
	return ca, certPEM, keyPEM
}

var tlsNATSArgs = []string{"-nats", "tls://nats:4222", "-auth-passthrough=false"}

// TestImageTrustsNATSThroughTheBakedBundlePath: the file the image ships its bundle at
// is the file its Go runtime reads roots from. The test CA replaces the directory, and
// SSL_CERT_DIR points away from it, so the directory scan cannot be what trusts it.
func TestImageTrustsNATSThroughTheBakedBundlePath(t *testing.T) {
	f := newFixture(t)
	ca, cert, key := natsTLS(t)
	f.startNATS(cert, key)
	vol := f.volume("roots", []tarEntry{{name: "ca-certificates.crt", body: ca.CertPEM}})
	b := f.runBroker([]string{"-v", vol + ":/etc/ssl/certs:ro", "-e", "SSL_CERT_DIR=/nonexistent"}, tlsNATSArgs)
	b.waitReady()
	dialRoundTripper(t, b.addr("1883"), "bundle-path").roundTrip(t, "over TLS to JetStream")
}

// TestImageRefusesAnUntrustedNATSCertificate: with the shipped bundle, a NATS server
// whose CA is not in it is refused, so the TLS route verifies.
func TestImageRefusesAnUntrustedNATSCertificate(t *testing.T) {
	f := newFixture(t)
	_, cert, key := natsTLS(t)
	f.startNATS(cert, key)
	b := f.runBroker(nil, tlsNATSArgs)
	if code := b.waitExit(time.Minute); code == 0 {
		t.Fatalf("broker exited 0 against an untrusted NATS certificate")
	}
	if logs := b.logs(); !strings.Contains(logs, "certificate signed by unknown authority") {
		t.Fatalf("broker did not refuse the certificate as untrusted:\n%s", logs)
	}
}

// TestImageTrustsAPrivateNATSCAThroughSSLCertFile: the operator route for a private
// NATS CA is a mounted file named by SSL_CERT_FILE.
func TestImageTrustsAPrivateNATSCAThroughSSLCertFile(t *testing.T) {
	f := newFixture(t)
	ca, cert, key := natsTLS(t)
	f.startNATS(cert, key)
	vol := f.secretVolume("natsca", map[string][]byte{"ca.crt": ca.CertPEM})
	b := f.runBroker([]string{"-v", vol + ":/etc/nats-ca:ro", "-e", "SSL_CERT_FILE=/etc/nats-ca/ca.crt"}, tlsNATSArgs)
	b.waitReady()
	dialRoundTripper(t, b.addr("1883"), "ssl-cert-file").roundTrip(t, "through a private CA")
}

// TestImageServesMQTTTLSFromAMountedSecret: the runtime user reads a Secret-shaped,
// read-only mount and serves TLS from it.
func TestImageServesMQTTTLSFromAMountedSecret(t *testing.T) {
	f := newFixture(t)
	f.startNATS(nil, nil)
	ca, err := testcerts.NewAuthority("imagesmoke MQTT CA")
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue("broker.smoke", "broker.smoke")
	if err != nil {
		t.Fatal(err)
	}
	vol := f.secretVolume("mqtttls", map[string][]byte{"tls.crt": cert, "tls.key": key})
	b := f.runBroker([]string{"-v", vol + ":/etc/mqtt-tls:ro"},
		brokerArgs("-port", "8883", "-tls-cert", "/etc/mqtt-tls/tls.crt", "-tls-key", "/etc/mqtt-tls/tls.key"))
	b.waitReady()
	cfg := &tls.Config{RootCAs: ca.Pool(), ServerName: "broker.smoke", MinVersion: tls.VersionTLS12}
	if rc := tlsConnack(t, b.addr("8883"), cfg); rc != 0 {
		t.Fatalf("CONNACK return code %d, want 0 (accepted)", rc)
	}
}

// TestImageProbesAndDrainOnSIGTERM: a SIGTERM to PID 1 takes the instance out of
// readiness while it keeps serving its connected clients for the grace, and a second
// signal ends the grace: the drain tells clients the server is shutting down and the
// process exits cleanly.
func TestImageProbesAndDrainOnSIGTERM(t *testing.T) {
	f := newFixture(t)
	f.startNATS(nil, nil)
	b := f.runBroker(nil, brokerArgs("-drain-delay", "120s"))
	b.waitReady()
	if code, body := b.probe("/healthz"); code != 200 {
		t.Fatalf("/healthz = %d %q before the signal", code, body)
	}
	w := dialWatcher(t, b.addr("1883"), "watcher")
	rt := dialRoundTripper(t, b.addr("1883"), "in-grace")
	rt.roundTrip(t, "before the signal")

	tTerm := time.Now()
	b.signal("TERM")
	if body := b.waitProbe("/readyz", 503, 30*time.Second); body != "draining" {
		t.Errorf("/readyz during the grace answered %q, want draining", body)
	}
	if code, body := b.probe("/healthz"); code != 200 {
		t.Errorf("/healthz during the grace = %d %q, want 200", code, body)
	}
	rt.roundTrip(t, "during the grace")

	// The exit deadline below must fall before the grace's own expiry, or an image that
	// ignored the second signal would pass by waiting the grace out.
	if spent := time.Since(tTerm); spent > 60*time.Second {
		t.Fatalf("in-grace checks took %s; the fixture is too slow to tell a second signal from the grace expiring", spent)
	}
	tInt := time.Now()
	if e := w.firstEvent(); e != nil && e.at.Before(tInt) {
		t.Fatalf("the drain began %s after SIGTERM, inside a 120s grace", e.at.Sub(tTerm))
	}
	b.signal("INT")
	if e := w.waitDisconnect(t, 30*time.Second); e.reason != reasonServerShuttingDown {
		t.Errorf("DISCONNECT reason %#x, want %#x (server shutting down)", e.reason, reasonServerShuttingDown)
	}
	if code := b.waitExit(30*time.Second - time.Since(tInt)); code != 0 {
		t.Fatalf("exit code %d after the second signal, want 0", code)
	}
}

// TestImageDrainWaitsOutTheGrace: the drain starts no earlier than the grace after the
// signal. Observations are only ever late, so this lower bound cannot fail a correct
// image; the exact grace is main_test.go's to prove.
func TestImageDrainWaitsOutTheGrace(t *testing.T) {
	const grace = 3 * time.Second
	f := newFixture(t)
	f.startNATS(nil, nil)
	b := f.runBroker(nil, brokerArgs("-drain-delay", grace.String()))
	b.waitReady()
	w := dialWatcher(t, b.addr("1883"), "watcher")

	tTerm := time.Now()
	b.signal("TERM")
	e := w.waitDisconnect(t, 30*time.Second)
	if e.reason != reasonServerShuttingDown {
		t.Errorf("DISCONNECT reason %#x, want %#x", e.reason, reasonServerShuttingDown)
	}
	if after := e.at.Sub(tTerm); after < grace {
		t.Errorf("DISCONNECT observed %s after SIGTERM, before the %s grace", after, grace)
	}
	if code := b.waitExit(30 * time.Second); code != 0 {
		t.Fatalf("exit code %d, want 0", code)
	}
}

// TestImageExitsOnSIGINTAsPID1: SIGINT, the other registered signal, reaches the
// broker as PID 1 and drains it.
func TestImageExitsOnSIGINTAsPID1(t *testing.T) {
	f := newFixture(t)
	f.startNATS(nil, nil)
	b := f.runBroker(nil, brokerArgs("-drain-delay", "0s"))
	b.waitReady()
	w := dialWatcher(t, b.addr("1883"), "watcher")
	b.signal("INT")
	if e := w.waitDisconnect(t, 30*time.Second); e.reason != reasonServerShuttingDown {
		t.Errorf("DISCONNECT reason %#x, want %#x", e.reason, reasonServerShuttingDown)
	}
	if code := b.waitExit(30 * time.Second); code != 0 {
		t.Fatalf("exit code %d, want 0", code)
	}
}
