// Package helmsmoke installs the Helm chart, and the topology-3 values for the official
// NATS chart, into a kind cluster and checks what only a cluster can show: that the API
// server and Restricted Pod Security admission accept the objects, that each topology
// serves MQTT through JetStream, that Helm upgrades and rolls a release back, that the
// NATS dependency's reloader applies a configuration change, and that in a shared pod
// NATS outlives the broker's shutdown.
//
// The tests skip unless MQTTNB_HELM_SMOKE_IMAGE and MQTTNB_HELM_SMOKE_EMBEDDED_IMAGE name
// the two images built for the host (make helm-smoke). Both are retagged as the chart's
// own default image references and loaded into the cluster, so every install runs the
// shipped values unchanged. One cluster serves every test; each test has its own
// namespace, labelled to enforce the Restricted Pod Security standard.
package helmsmoke

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	chartDir        = "../../charts/mqtt-nats-broker"
	sharedPodValues = "../../docs/deploy/nats-chart-shared-pod"

	installTimeout = 8 * time.Minute
)

// env is what TestMain resolved; empty when the suite skips.
var env struct {
	helm, kind, kubectl string
	kubeconfig          string
	cluster             string
	appVersion          string
}

func TestMain(m *testing.M) {
	external, embedded := os.Getenv("MQTTNB_HELM_SMOKE_IMAGE"), os.Getenv("MQTTNB_HELM_SMOKE_EMBEDDED_IMAGE")
	if external == "" || embedded == "" {
		os.Exit(m.Run()) // every test skips
	}
	code, err := runWithCluster(m, external, embedded)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helmsmoke:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runWithCluster(m *testing.M, external, embedded string) (int, error) {
	env.helm, env.kind, env.kubectl = os.Getenv("MQTTNB_HELM"), os.Getenv("MQTTNB_KIND"), os.Getenv("MQTTNB_KUBECTL")
	if env.helm == "" || env.kind == "" || env.kubectl == "" {
		return 0, fmt.Errorf("MQTTNB_HELM, MQTTNB_KIND and MQTTNB_KUBECTL must name the binaries (make helm-smoke)")
	}
	data, err := os.ReadFile(filepath.Join(chartDir, "Chart.yaml"))
	if err != nil {
		return 0, err
	}
	var chart struct {
		AppVersion string `yaml:"appVersion"`
	}
	if err := yaml.Unmarshal(data, &chart); err != nil || chart.AppVersion == "" {
		return 0, fmt.Errorf("Chart.yaml appVersion: %v", err)
	}
	env.appVersion = chart.AppVersion

	dir, err := os.MkdirTemp("", "helmsmoke-")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)
	env.kubeconfig = filepath.Join(dir, "kubeconfig")
	env.cluster = "mqttnb-helm-smoke-" + randSuffix()

	// Subnets of their own: kind's defaults (10.96.0.0/12 for Services, 10.244.0.0/16 for
	// pods) are every kubeadm cluster's, so a kind cluster run inside one routes its
	// Service traffic into the host cluster's instead.
	kindConfig := filepath.Join(dir, "kind.yaml")
	if err := os.WriteFile(kindConfig, []byte("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\n"+
		"networking:\n  serviceSubnet: 10.200.0.0/16\n  podSubnet: 10.210.0.0/16\n"), 0o600); err != nil {
		return 0, err
	}
	ctx := context.Background()
	if _, err := run(ctx, nil, env.kind, "create", "cluster", "--name", env.cluster, "--config", kindConfig,
		"--kubeconfig", env.kubeconfig, "--wait", "3m"); err != nil {
		return 0, err
	}
	if os.Getenv("MQTTNB_HELM_SMOKE_KEEP") == "" {
		defer func() { _, _ = run(ctx, nil, env.kind, "delete", "cluster", "--name", env.cluster) }()
	}
	// The chart's and the topology-3 values' own image references, so nothing is
	// overridden.
	repo := "ghcr.io/nberlee/mqtt-nats-broker:" + env.appVersion
	for src, dst := range map[string]string{external: repo, embedded: repo + "-embedded"} {
		if _, err := run(ctx, nil, "docker", "tag", src, dst); err != nil {
			return 0, err
		}
		if _, err := run(ctx, nil, env.kind, "load", "docker-image", dst, "--name", env.cluster); err != nil {
			return 0, err
		}
	}
	// The images the NATS chart runs, loaded from the daemon so the node pulls nothing.
	for _, img := range thirdPartyImages {
		if _, err := run(ctx, nil, "docker", "pull", img); err != nil {
			return 0, err
		}
		if _, err := run(ctx, nil, env.kind, "load", "docker-image", img, "--name", env.cluster); err != nil {
			return 0, err
		}
	}
	return m.Run(), nil
}

// thirdPartyImages are the images the NATS chart runs here: the server at the 2.15.2 floor
// (chart 2.15.1's default nats:2.15.1-alpine is below it, so the values override it) and
// chart 2.15.1's config reloader.
var thirdPartyImages = []string{"synadia/nats-server:2.15.2-alpine3.22", "natsio/nats-server-config-reloader:0.24.0"}

func randSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// run executes a command and returns its stdout; a nonzero exit is an error with stderr.
func run(ctx context.Context, stdin io.Reader, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = stdin
	cmd.Env = append(os.Environ(), "KUBECONFIG="+env.kubeconfig)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%s %s: %v\n%s", name, strings.Join(args, " "), err, stderr.String())
	}
	return stdout.String(), nil
}

func skipUnlessCluster(t *testing.T) {
	t.Helper()
	if env.cluster == "" {
		t.Skip("MQTTNB_HELM_SMOKE_IMAGE / MQTTNB_HELM_SMOKE_EMBEDDED_IMAGE not set; run make helm-smoke")
	}
}

func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), installTimeout)
	defer cancel()
	out, err := run(ctx, nil, env.kubectl, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func helm(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), installTimeout+time.Minute)
	defer cancel()
	out, err := run(ctx, nil, env.helm, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// namespace creates the test's namespace, enforcing Restricted Pod Security, with the
// Secrets the chart and the topology-3 values reference, and removes it afterwards.
func namespace(t *testing.T) string {
	t.Helper()
	skipUnlessCluster(t)
	ns := "smoke-" + randSuffix()
	kubectl(t, "create", "namespace", ns)
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := run(context.Background(), nil, env.kubectl, "-n", ns, "get", "pods,events", "-o", "wide")
			t.Logf("namespace %s:\n%s", ns, out)
		}
		_, _ = run(context.Background(), nil, env.kubectl, "delete", "namespace", ns, "--wait=false")
	})
	kubectl(t, "label", "namespace", ns,
		"pod-security.kubernetes.io/enforce=restricted", "pod-security.kubernetes.io/enforce-version=latest")
	kubectl(t, "-n", ns, "create", "secret", "generic", "mqtt-nats-broker-nats",
		"--from-literal=user=svc-"+randSuffix(), "--from-literal=password="+randSuffix()+randSuffix(),
		"--from-literal=default-user=anon-"+randSuffix(), "--from-literal=default-password="+randSuffix()+randSuffix())
	kubectl(t, "-n", ns, "create", "secret", "generic", "mqtt-nats-broker-routes",
		"--from-literal=user=route", "--from-literal=password="+randSuffix()+randSuffix())
	return ns
}

// install installs the chart (or the NATS chart archive) as a fresh release and waits
// for every workload to be ready. The wait is the test's, not Helm's: Helm 4's watcher
// reports a release failed as soon as a pod is in CrashLoopBackOff, and a broker container
// crash-loops until its NATS is up (docs/CONFORMANCE.md, "Shared-pod startup is not
// ordered"), while its legacy waiter also waits for LoadBalancer addresses kind never
// assigns. Readiness of every replica, not the first restart, is what an install reaches.
func install(t *testing.T, ns, release, chart string, values ...string) {
	t.Helper()
	args := []string{"install", release, chart, "-n", ns}
	for _, v := range values {
		args = append(args, "-f", v)
	}
	helm(t, args...)
	waitReady(t, ns)
}

// waitReady waits until every Deployment and StatefulSet in ns has observed its spec and
// has all its replicas updated and ready.
func waitReady(t *testing.T, ns string) {
	t.Helper()
	eventually(t, installTimeout, "every workload ready", func() error {
		out := kubectl(t, "-n", ns, "get", "deployments,statefulsets", "-o",
			`jsonpath={range .items[*]}{.kind}/{.metadata.name} {.metadata.generation} {.status.observedGeneration} {.spec.replicas} {.status.updatedReplicas} {.status.readyReplicas}{"\n"}{end}`)
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			f := strings.Fields(line)
			if len(f) != 6 || f[1] != f[2] || f[3] != f[4] || f[3] != f[5] {
				return fmt.Errorf("not ready (name generation observed replicas updated ready): %s", line)
			}
		}
		return nil
	})
}

func natsChartArchive(t *testing.T) string {
	t.Helper()
	tgz, err := filepath.Glob(filepath.Join(chartDir, "charts", "nats-*.tgz"))
	if err != nil || len(tgz) != 1 {
		t.Fatalf("want the NATS chart archive from helm dependency build, got %v", tgz)
	}
	return tgz[0]
}

func chartValues(name string) string { return filepath.Join(chartDir, "ci", name) }

func writeValues(t *testing.T, body string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "values.yaml")
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

// forward is a kubectl port-forward that reopens when it has ended. kubectl exits when
// the connection to its pod breaks, and a broker container restarts while its NATS
// starts (docs/CONFORMANCE.md, "Shared-pod startup is not ordered"), so a retry through
// a forward opened before such a restart would reach nothing. Each retry asks Addr.
type forward struct {
	t      *testing.T
	ns     string
	target string
	port   int
	addr   string
	ended  chan struct{}
}

// portForward forwards a local port to target's port.
func portForward(t *testing.T, ns, target string, port int) *forward {
	t.Helper()
	f := &forward{t: t, ns: ns, target: target, port: port}
	f.open()
	return f
}

// Addr is the local address of a live forward, reopening it if kubectl has exited.
func (f *forward) Addr() string {
	f.t.Helper()
	select {
	case <-f.ended:
		f.open()
	default:
	}
	return f.addr
}

func (f *forward) open() {
	t := f.t
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, env.kubectl, "-n", f.ns, "port-forward", "--address", "127.0.0.1", f.target, fmt.Sprintf(":%d", f.port))
	cmd.Env = append(os.Environ(), "KUBECONFIG="+env.kubeconfig)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	addr := make(chan string, 1)
	ended := make(chan struct{})
	go func() {
		re := regexp.MustCompile(`Forwarding from (127\.0\.0\.1:\d+)`)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			if m := re.FindStringSubmatch(sc.Text()); m != nil {
				addr <- m[1]
				break
			}
		}
		if sc.Err() == nil {
			_, _ = io.Copy(io.Discard, out)
		}
		_ = cmd.Wait()
		close(ended)
	}()
	t.Cleanup(func() { cancel(); <-ended })
	select {
	case a := <-addr:
		f.addr, f.ended = a, ended
	case <-time.After(30 * time.Second):
		t.Fatalf("port-forward %s %d: %s", f.target, f.port, stderr.String())
	}
}

// podNames returns the names of the pods matching selector.
func podNames(t *testing.T, ns, selector string) []string {
	t.Helper()
	out := kubectl(t, "-n", ns, "get", "pods", "-l", selector, "-o", "jsonpath={.items[*].metadata.name}")
	return strings.Fields(out)
}

// eventually retries f until it returns nil or within passes.
func eventually(t *testing.T, within time.Duration, what string, f func() error) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		err := f()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(2 * time.Second)
	}
}
