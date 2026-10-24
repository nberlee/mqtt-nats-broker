// Package imagesmoke runs the shipped container image under the constraints a
// Kubernetes pod imposes - read-only root filesystem, every capability dropped, no
// privilege escalation, the image's own user - and checks what only the artifact can
// show: what the image contains, whom it runs as, whom it trusts, and how it answers
// probes and signals as PID 1. The broker's behavior is the in-process suites' domain.
//
// The tests drive the docker CLI and skip unless MQTTNB_IMAGE_SMOKE_IMAGE names the
// image under test (make image-smoke-external builds and names it). Every file a
// container needs travels through a named volume populated by a helper container,
// never a host bind mount: the daemon need not share this process's filesystem.
package imagesmoke

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// natsImage is the server the conformance jobs pin, at the documented floor.
	natsImage = "synadia/nats-server:2.15.1-scratch@sha256:57e8640c67acb4f40fd453cde5518baa601759ed242a476b705bbfb573cf01f2"
	// helperImage populates volumes; it needs a shell and tar, which neither the
	// image under test nor the NATS image has. Digest-pinned like natsImage.
	helperImage = "busybox@sha256:fd7dc98638c8e305f4dc34e979f1c0fdfdcaeb0fbf8fcff77ae834b6da3d7e6e"

	// runtimeUID and runtimeGID are the numeric identity the image declares.
	runtimeUID = 65532
	runtimeGID = 65532

	readyTimeout = 30 * time.Second
)

// imageUnderTest returns the image reference, skipping when none is configured.
func imageUnderTest(t *testing.T) string {
	t.Helper()
	img := os.Getenv("MQTTNB_IMAGE_SMOKE_IMAGE")
	if img == "" {
		t.Skip("MQTTNB_IMAGE_SMOKE_IMAGE not set; run make image-smoke-external")
	}
	return img
}

// docker runs the CLI and returns its stdout; a nonzero exit is an error carrying stderr.
func docker(ctx context.Context, stdin io.Reader, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func mustDocker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := docker(ctx, nil, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// cleanup removes a Docker object at test end. It is registered BEFORE the object is
// created, so a failed creation still leaves nothing behind; t.Cleanup runs LIFO, so
// containers go before the volumes and network registered ahead of them.
func cleanup(t *testing.T, args ...string) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = docker(ctx, nil, args...)
	})
}

// fixture is one test's private network and the names it owns.
type fixture struct {
	t       *testing.T
	image   string
	suffix  string
	network string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	image := imageUnderTest(t)
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, image: image, suffix: hex.EncodeToString(b)}
	f.network = f.name("net")
	cleanup(t, "network", "rm", f.network)
	mustDocker(t, "network", "create", f.network)
	return f
}

func (f *fixture) name(role string) string { return "imgsmoke-" + role + "-" + f.suffix }

// tarEntry is one volume entry. Files and directories are root-owned with the modes
// a Kubernetes Secret volume presents by default.
type tarEntry struct {
	name string
	body []byte
	dir  bool
	link string // symlink target, relative
}

// volume creates a named volume holding entries and returns its name. It is
// populated before anything else mounts it, so Docker never copies image content in.
func (f *fixture) volume(role string, entries []tarEntry) string {
	f.t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Uid: 0, Gid: 0, ModTime: time.Now()}
		switch {
		case e.dir:
			h.Typeflag, h.Mode = tar.TypeDir, 0o755
		case e.link != "":
			h.Typeflag, h.Linkname, h.Mode = tar.TypeSymlink, e.link, 0o777
		default:
			h.Typeflag, h.Mode, h.Size = tar.TypeReg, 0o644, int64(len(e.body))
		}
		if err := tw.WriteHeader(h); err != nil {
			f.t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := tw.Write(e.body); err != nil {
				f.t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		f.t.Fatal(err)
	}
	vol := f.name(role)
	cleanup(f.t, "volume", "rm", "-f", vol)
	mustDocker(f.t, "volume", "create", vol)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := docker(ctx, &buf, "run", "-i", "--rm", "-v", vol+":/v", helperImage, "tar", "-x", "-C", "/v"); err != nil {
		f.t.Fatal(err)
	}
	return vol
}

// secretVolume lays files out the way the kubelet projects a Secret: the content in a
// timestamped directory, `..data` pointing at it, and each key a symlink through `..data`.
func (f *fixture) secretVolume(role string, files map[string][]byte) string {
	const gen = "..2026_10_24_00_00_00.000000001"
	entries := []tarEntry{{name: gen + "/", dir: true}}
	for name, body := range files {
		entries = append(entries, tarEntry{name: gen + "/" + name, body: body})
	}
	entries = append(entries, tarEntry{name: "..data", link: gen})
	for name := range files {
		entries = append(entries, tarEntry{name: name, link: "..data/" + name})
	}
	return f.volume(role, entries)
}

// startNATS runs a JetStream server reachable as `nats` on the fixture network. With
// certPEM/keyPEM set it requires TLS. Its store is in its own container layer.
func (f *fixture) startNATS(certPEM, keyPEM []byte) {
	f.t.Helper()
	conf := "listen: 0.0.0.0:4222\njetstream {\n  enabled: true\n  store_dir: /data\n  limits { default_max_consumers: -1 }\n}\n"
	files := map[string][]byte{}
	if certPEM != nil {
		conf += "tls {\n  cert_file: /etc/nats/tls.crt\n  key_file: /etc/nats/tls.key\n}\n"
		files["tls.crt"], files["tls.key"] = certPEM, keyPEM
	}
	files["nats.conf"] = []byte(conf)
	var entries []tarEntry
	for name, body := range files {
		entries = append(entries, tarEntry{name: name, body: body})
	}
	vol := f.volume("natsconf", entries)
	name := f.name("nats")
	cleanup(f.t, "rm", "-f", name)
	mustDocker(f.t, "run", "-d", "--name", name, "--network", f.network, "--network-alias", "nats",
		"-v", vol+":/etc/nats:ro", natsImage, "-c", "/etc/nats/nats.conf")
	deadline := time.Now().Add(readyTimeout)
	for {
		// The server logs to stderr, which docker logs replays on its own stderr.
		out, _ := exec.Command("docker", "logs", name).CombinedOutput()
		if strings.Contains(string(out), "Server is ready") {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("NATS not ready within %s:\n%s", readyTimeout, out)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// broker is one running container of the image under test.
type broker struct {
	f    *fixture
	name string
}

// brokerArgs are the flags every broker here starts with: NATS by its network alias,
// and no auth passthrough, since the fixture server has no users.
func brokerArgs(extra ...string) []string {
	return append([]string{"-nats", "nats://nats:4222", "-auth-passthrough=false"}, extra...)
}

// runBroker starts the image under test as a pod would run it. dockerOpts carries
// mounts (-v) and environment (-e); args are the broker's flags. The image's own user
// is under test, so no --user is ever passed.
func (f *fixture) runBroker(dockerOpts []string, args []string) *broker {
	f.t.Helper()
	b := &broker{f: f, name: f.name("broker")}
	cleanup(f.t, "rm", "-f", b.name)
	f.t.Cleanup(func() {
		if f.t.Failed() {
			out, _ := exec.Command("docker", "logs", b.name).CombinedOutput()
			f.t.Logf("broker logs:\n%s", out)
		}
	})
	run := []string{"run", "-d", "--name", b.name, "--network", f.network,
		"--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"-p", "127.0.0.1::1883", "-p", "127.0.0.1::8883", "-p", "127.0.0.1::8080"}
	run = append(run, dockerOpts...)
	run = append(run, f.image)
	run = append(run, args...)
	mustDocker(f.t, run...)
	return b
}

// addr is the host address a container port is published on.
func (b *broker) addr(port string) string {
	b.f.t.Helper()
	out := mustDocker(b.f.t, "port", b.name, port+"/tcp")
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line
		}
	}
	b.f.t.Fatalf("port %s not published on 127.0.0.1: %q", port, out)
	return ""
}

// probe returns a health endpoint's status and body; status 0 means no answer.
func (b *broker) probe(path string) (int, string) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + b.addr("8080") + path)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// waitProbe polls path until it answers want, or fails the test at the deadline.
func (b *broker) waitProbe(path string, want int, within time.Duration) string {
	b.f.t.Helper()
	deadline := time.Now().Add(within)
	for {
		code, body := b.probe(path)
		if code == want {
			return body
		}
		if time.Now().After(deadline) {
			b.f.t.Fatalf("%s: got %d %q, want %d within %s", path, code, body, want, within)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (b *broker) waitReady() { b.f.t.Helper(); b.waitProbe("/readyz", http.StatusOK, readyTimeout) }

func (b *broker) signal(sig string) { b.f.t.Helper(); mustDocker(b.f.t, "kill", "-s", sig, b.name) }

// waitExit returns the container's exit code, failing the test if it is still running
// after within.
func (b *broker) waitExit(within time.Duration) int {
	b.f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	out, err := docker(ctx, nil, "wait", b.name)
	if err != nil {
		b.f.t.Fatalf("container did not exit within %s: %v", within, err)
	}
	code, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		b.f.t.Fatalf("docker wait: %q: %v", out, err)
	}
	return code
}

func (b *broker) logs() string {
	out, _ := exec.Command("docker", "logs", b.name).CombinedOutput()
	return string(out)
}
