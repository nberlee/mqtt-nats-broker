package helmsmoke

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
)

const brokerSelector = "app.kubernetes.io/component=broker"

// TestExternalWithNATSDependency installs the external topology with the official NATS
// chart beside it, one server: MQTT round-trips through JetStream, and a retained message
// outlives the broker pod that stored it.
func TestExternalWithNATSDependency(t *testing.T) {
	ns := namespace(t)
	install(t, ns, "ext", chartDir, chartValues("optional-nats-values.yaml"))
	svc := "svc/ext-mqtt-nats-broker"
	addr := portForward(t, ns, svc, 1883)
	eventually(t, time.Minute, "round trip", func() error { return roundTripOnce(addr, "ext-rt", nil) })

	cl, err := dial(addr, "ext-retain", "smoke/retained", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.roundTrip("kept", true); err != nil {
		t.Fatal(err)
	}
	cl.close()
	for _, pod := range podNames(t, ns, brokerSelector) {
		kubectl(t, "-n", ns, "delete", "pod", pod, "--wait=true")
	}
	kubectl(t, "-n", ns, "rollout", "status", "deployment/ext-mqtt-nats-broker", "--timeout", installTimeout.String())
	addr = portForward(t, ns, svc, 1883)
	eventually(t, time.Minute, "retained after every broker pod was replaced", func() error {
		return awaitRetained(addr, "ext-retain-2", "smoke/retained", "kept")
	})
}

// TestExternalWithNATSDependencyCluster installs the dependency as a three-server
// JetStream cluster with the broker's streams at R3.
func TestExternalWithNATSDependencyCluster(t *testing.T) {
	ns := namespace(t)
	install(t, ns, "extc", chartDir, chartValues("optional-nats-cluster-values.yaml"))
	addr := portForward(t, ns, "svc/extc-mqtt-nats-broker", 1883)
	eventually(t, 2*time.Minute, "round trip", func() error { return roundTripOnce(addr, "extc-rt", nil) })
	if err := routeRefused(portForward(t, ns, "pod/extc-nats-0", 6222)); err != nil {
		t.Errorf("route port: %v", err)
	}
}

// TestExternalTLS serves MQTT over TLS from a kubernetes.io/tls Secret.
func TestExternalTLS(t *testing.T) {
	ns := namespace(t)
	ca, err := testcerts.NewAuthority("helm smoke CA")
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := ca.Issue("mqtt-nats-broker", "mqtt-nats-broker")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cert, _ := testcerts.Write(dir, "tls.crt", certPEM)
	key, _ := testcerts.Write(dir, "tls.key", keyPEM)
	kubectl(t, "-n", ns, "create", "secret", "tls", "mqtt-nats-broker-tls", "--cert", cert, "--key", key)
	install(t, ns, "tls", chartDir, writeValues(t, "topology: external\nnats:\n  enabled: true\nmqtt:\n  tls:\n    enabled: true\n    secretName: mqtt-nats-broker-tls\n"))
	addr := portForward(t, ns, "svc/tls-mqtt-nats-broker", 8883)
	cfg := &tls.Config{RootCAs: ca.Pool(), ServerName: "mqtt-nats-broker", MinVersion: tls.VersionTLS12}
	eventually(t, time.Minute, "TLS round trip", func() error { return roundTripOnce(addr, "tls-rt", cfg) })
}

// TestEmbedded installs the embedded topology, one server and a three-peer cluster. The
// cluster keeps serving after a peer is deleted.
func TestEmbedded(t *testing.T) {
	for _, tc := range []struct {
		name, values string
		clustered    bool
	}{
		{"single", "embedded-values.yaml", false},
		{"cluster", "embedded-cluster-values.yaml", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := namespace(t)
			release := "emb" + tc.name[:1] + randSuffix()[:4]
			install(t, ns, release, chartDir, chartValues(tc.values))
			svc := "svc/" + release + "-mqtt-nats-broker"
			addr := portForward(t, ns, svc, 1883)
			eventually(t, 2*time.Minute, "round trip", func() error { return roundTripOnce(addr, release+"-rt", nil) })
			if !tc.clustered {
				return
			}
			if err := routeRefused(portForward(t, ns, "pod/"+release+"-mqtt-nats-broker-0", 6222)); err != nil {
				t.Errorf("route port: %v", err)
			}
			kubectl(t, "-n", ns, "delete", "pod", release+"-mqtt-nats-broker-0", "--wait=false")
			// Through a peer that stays up.
			addr = portForward(t, ns, "pod/"+release+"-mqtt-nats-broker-1", 1883)
			eventually(t, 2*time.Minute, "round trip with a peer gone", func() error {
				return roundTripOnce(addr, release+"-rt2", nil)
			})
			kubectl(t, "-n", ns, "rollout", "status", "statefulset/"+release+"-mqtt-nats-broker", "--timeout", installTimeout.String())
		})
	}
}

// brokerLogLevel returns the MQTTNB_LOG_LEVEL the live Deployment's pod template sets.
func brokerLogLevel(t *testing.T, ns, deployment string) string {
	t.Helper()
	return strings.TrimSpace(kubectl(t, "-n", ns, "get", "deployment", deployment, "-o",
		`jsonpath={.spec.template.spec.containers[?(@.name=="broker")].env[?(@.name=="MQTTNB_LOG_LEVEL")].value}`))
}

// podsLogLevels returns the MQTTNB_LOG_LEVEL of every running broker pod.
func podsLogLevels(t *testing.T, ns string) []string {
	t.Helper()
	out := kubectl(t, "-n", ns, "get", "pods", "-l", brokerSelector, "--field-selector", "status.phase=Running", "-o",
		`jsonpath={range .items[*]}{.metadata.deletionTimestamp}{"|"}{.spec.containers[?(@.name=="broker")].env[?(@.name=="MQTTNB_LOG_LEVEL")].value}{"\n"}{end}`)
	var levels []string
	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		deleting, level, _ := strings.Cut(line, "|")
		if deleting == "" && level != "" {
			levels = append(levels, level)
		}
	}
	return levels
}

// TestUpgradeAndRollback changes a value with helm upgrade, then rolls the release back:
// each time the live spec, the running pods and the release history must agree, and the
// broker must serve. A round trip alone could be served by the old pods.
func TestUpgradeAndRollback(t *testing.T) {
	ns := namespace(t)
	install(t, ns, "upg", chartDir, chartValues("optional-nats-values.yaml"))
	dep := "upg-mqtt-nats-broker"
	if got := brokerLogLevel(t, ns, dep); got != "info" {
		t.Fatalf("installed log level %q, want info", got)
	}

	helm(t, "upgrade", "upg", chartDir, "-n", ns, "--wait", "--timeout", installTimeout.String(),
		"-f", chartValues("optional-nats-values.yaml"), "--set", "broker.logLevel=debug")
	if got := brokerLogLevel(t, ns, dep); got != "debug" {
		t.Fatalf("upgraded log level %q, want debug", got)
	}
	eventually(t, 2*time.Minute, "every pod running the upgrade", func() error {
		if l := podsLogLevels(t, ns); len(l) != 3 || strings.Count(strings.Join(l, " "), "debug") != 3 {
			return fmt.Errorf("pod log levels %v", l)
		}
		return nil
	})

	helm(t, "rollback", "upg", "1", "-n", ns, "--wait", "--timeout", installTimeout.String())
	history := helm(t, "history", "upg", "-n", ns, "-o", "json")
	if !strings.Contains(history, `"revision":3`) || !strings.Contains(history, "Rollback to 1") {
		t.Fatalf("history after the rollback: %s", history)
	}
	if v1, v3 := helm(t, "get", "values", "upg", "-n", ns, "--revision", "1", "-o", "json"),
		helm(t, "get", "values", "upg", "-n", ns, "--revision", "3", "-o", "json"); v1 != v3 {
		t.Errorf("revision 3 values %s differ from revision 1's %s", v3, v1)
	}
	if got := brokerLogLevel(t, ns, dep); got != "info" {
		t.Fatalf("rolled-back log level %q, want info", got)
	}
	eventually(t, 2*time.Minute, "every pod running the rollback", func() error {
		if l := podsLogLevels(t, ns); len(l) != 3 || strings.Count(strings.Join(l, " "), "info") != 3 {
			return fmt.Errorf("pod log levels %v", l)
		}
		return nil
	})
	addr := portForward(t, ns, "svc/"+dep, 1883)
	eventually(t, time.Minute, "round trip after the rollback", func() error { return roundTripOnce(addr, "upg-rt", nil) })
}

// TestNATSDependencyReload changes the dependency's configuration with helm upgrade: its
// reloader, running as the server's non-root user on a read-only root, applies it without
// restarting the server, and the added user can connect.
func TestNATSDependencyReload(t *testing.T) {
	ns := namespace(t)
	install(t, ns, "rel", chartDir, chartValues("optional-nats-values.yaml"))
	pod := "rel-nats-0"
	restarts := func() string {
		return strings.TrimSpace(kubectl(t, "-n", ns, "get", "pod", pod, "-o",
			`jsonpath={.status.containerStatuses[?(@.name=="nats")].restartCount}`))
	}
	before := restarts()
	helm(t, "upgrade", "rel", chartDir, "-n", ns, "--wait", "--timeout", installTimeout.String(),
		"-f", chartValues("optional-nats-values.yaml"), "-f", writeValues(t, `
nats:
  config:
    merge:
      accounts:
        EXTRA:
          users:
            - user: extra
              password: extra-password
`))
	eventually(t, 2*time.Minute, "the reload", func() error {
		if !strings.Contains(kubectl(t, "-n", ns, "logs", pod, "-c", "nats"), "Reloaded server configuration") {
			return errNotYet
		}
		return nil
	})
	if after := restarts(); after != before {
		t.Errorf("the nats container restarted (%s -> %s); the reloader should have applied the change", before, after)
	}
	addr := portForward(t, ns, "pod/"+pod, 4222)
	eventually(t, time.Minute, "the added user", func() error {
		nc, err := nats.Connect("nats://"+addr, nats.UserInfo("extra", "extra-password"), nats.Timeout(5*time.Second))
		if err != nil {
			return err
		}
		nc.Close()
		return nil
	})
}

// logStream follows a container's log from now until the container ends.
type logStream struct {
	mu    sync.Mutex
	lines []string
	done  chan struct{}
}

func followLog(t *testing.T, ns, pod, container string) *logStream {
	t.Helper()
	ls := &logStream{done: make(chan struct{})}
	cmd := exec.Command(env.kubectl, "-n", ns, "logs", "-f", pod, "-c", container)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+env.kubeconfig)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-ls.done })
	go func() {
		defer close(ls.done)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			ls.mu.Lock()
			ls.lines = append(ls.lines, sc.Text())
			ls.mu.Unlock()
		}
		_ = cmd.Wait()
	}()
	return ls
}

func (ls *logStream) snapshot() []string {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return append([]string(nil), ls.lines...)
}

// TestSharedPodShutdownOrder installs the official NATS chart with the topology-3 values,
// one server and a three-server cluster, and deletes a pod while an MQTT client is
// connected. NATS must keep serving the broker through the broker's drain and close:
// within the nats container's own log, one stream with one clock, the broker's service
// connection closes as "Client Closed" before lame duck mode is entered, and a QoS 1
// round trip still completes during the drain delay.
func TestSharedPodShutdownOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files []string
	}{
		{"single", []string{"values.yaml"}},
		{"cluster", []string{"values.yaml", "values-cluster.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := namespace(t)
			release := "shr" + tc.name[:1]
			var values []string
			for _, f := range tc.files {
				values = append(values, sharedPodValues+"/"+f)
			}
			// Test-only: nats-server logs client closes at debug level. The 2.15.1 chart
			// reads its server settings from config.merge, not config.debug.
			values = append(values, writeValues(t, "config:\n  merge:\n    debug: true\n"))
			install(t, ns, release, natsChartArchive(t), values...)

			// The NATS chart's fullname: the release name, suffixed -nats unless it contains it.
			pod := release + "-nats-0"
			addr := portForward(t, ns, "pod/"+pod, 1883)
			var cl *client
			eventually(t, 2*time.Minute, "connect", func() error {
				var err error
				cl, err = dial(addr, "shared-drain", "smoke/shared-drain", nil)
				if err != nil {
					return err
				}
				return cl.roundTrip("before", false)
			})
			defer cl.close()
			if len(tc.files) > 1 {
				if err := routeRefused(portForward(t, ns, "pod/"+pod, 6222)); err != nil {
					t.Errorf("route port: %v", err)
				}
			}

			logs := followLog(t, ns, pod, "nats")
			time.Sleep(2 * time.Second) // the follow is attached
			kubectl(t, "-n", ns, "delete", "pod", pod, "--wait=false")
			// Inside the drain delay (10 s): /readyz is 503, but the connected client is
			// still served end to end, through NATS.
			time.Sleep(3 * time.Second)
			if err := cl.roundTrip("during the drain delay", false); err != nil {
				t.Errorf("round trip during the drain delay: %v", err)
			}
			select {
			case <-logs.done:
			case <-time.After(3 * time.Minute):
				t.Fatal("the nats container did not exit")
			}
			checkSharedPodOrder(t, logs.snapshot())
		})
	}
}

// checkSharedPodOrder finds the broker's service connection close (the connection named
// exactly mqtt-nats-broker, not the per-client mqtt-nats-broker:mqtt:... ones) and lame
// duck entry in one log, and requires the close, by the client, first.
func checkSharedPodOrder(t *testing.T, lines []string) {
	t.Helper()
	closeAt, ldmAt := -1, -1
	var closeLine string
	for i, l := range lines {
		if closeAt < 0 && strings.Contains(l, `:mqtt-nats-broker" - `) && strings.Contains(l, "Client connection closed") {
			closeAt, closeLine = i, l
		}
		if ldmAt < 0 && strings.Contains(l, "Entering lame duck mode") {
			ldmAt = i
		}
	}
	if closeAt < 0 || ldmAt < 0 {
		t.Fatalf("service connection close at line %d, lame duck at %d; log:\n%s", closeAt, ldmAt, strings.Join(lines, "\n"))
	}
	if closeAt > ldmAt {
		t.Errorf("lame duck (line %d) before the broker's service connection closed (line %d): NATS left the broker mid-shutdown", ldmAt, closeAt)
	}
	if !strings.HasSuffix(closeLine, "Client Closed") {
		t.Errorf("the broker's service connection closed as %q, want Client Closed (the broker's own close)", closeLine)
	}
}

// The chart's own appVersion is what the images were retagged to; a mismatch would run
// an image the cluster does not have.
func TestImagesLoaded(t *testing.T) {
	skipUnlessCluster(t)
	nodes := strings.Fields(kubectl(t, "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}"))
	if len(nodes) == 0 {
		t.Fatal("no nodes")
	}
	out, err := run(context.Background(), nil, "docker", "exec", nodes[0], "crictl", "images", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	for _, tag := range []string{env.appVersion, env.appVersion + "-embedded"} {
		if !strings.Contains(out, "ghcr.io/nberlee/mqtt-nats-broker:"+tag) {
			t.Errorf("image tag %s not loaded", tag)
		}
	}
}
