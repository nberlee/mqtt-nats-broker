package main

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"gopkg.in/yaml.v3"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
)

// The chart is judged the way the example deployment is: against the binary that will run
// it. Its contract has two halves.
//
// What the chart ships, which is every variant under charts/mqtt-nats-broker/ci and the
// topology-3 values for the official NATS chart, is judged against a closed inventory:
//   - the objects: exactly the variant's kinds, nothing else from the broker's side;
//   - the fields of every broker object: none outside the set the chart renders;
//   - the environment: exactly the variant's variables, each one internal/config or the
//     embedded cluster configuration reads, accepted by config.Load, credentials from
//     their Secret keys, and kubelet $(VAR) references only where the route credentials
//     are expanded;
//   - the listeners, probes, grace, Service, TLS mount and security, as for the example;
//   - the embedded topology: the StatefulSet, headless Service, store claim, route list
//     and nats-server configuration, the last loaded and enforced as the binary does;
//   - the optional NATS dependency: the broker's URL from that chart's rendered Service,
//     JetStream with the consumer limit the broker needs, the deny ACL for anonymous MQTT
//     clients, and Restricted Pod Security on every container it renders.
//
// What a user adds through merge, patch or extraResources is outside that inventory: the
// chart's purpose includes letting them change any field. TestHelmChartExtensionPoints
// shows that each hook lands where the values document it and keeps what the chart
// renders. Neither half shows that an API server admits the objects; the kind install in
// test/helmsmoke does.

const (
	helmChartDir    = "../../charts/mqtt-nats-broker"
	sharedPodValues = "../../docs/deploy/nats-chart-shared-pod"

	// The embedded topology's grace keeps this beyond the external one for the in-process
	// server's Shutdown and WaitForShutdown, which have no named budget.
	embeddedShutdownHeadroom = 15 * time.Second

	// natsRuntimeUser is the identity the chart gives the official NATS chart's containers.
	natsRuntimeUser = 1000

	routeSecretName = "mqtt-nats-broker-routes"
	helmRelease     = "t"
	helmNamespace   = "default"
)

// chartVariant names a shipped values file by what it must render.
type chartVariant struct {
	file      string
	embedded  bool
	clustered bool
	replicas  int
	listen    chartListeners
	nats      bool // the optional dependency is enabled
	hpa       bool
	// streamReplicas is the MQTTNB_STREAM_REPLICAS the variant sets; 0 leaves it unset.
	streamReplicas int
}

type chartListeners struct{ tls, plain, ws bool }

var chartVariants = []chartVariant{
	{file: "default-values.yaml", embedded: true, replicas: 1},
	{file: "external-values.yaml", replicas: 3},
	{file: "tls-values.yaml", replicas: 3, listen: chartListeners{tls: true}},
	{file: "tls-plaintext-values.yaml", replicas: 3, listen: chartListeners{tls: true, plain: true}},
	{file: "websocket-values.yaml", replicas: 3, listen: chartListeners{ws: true}},
	{file: "autoscaling-values.yaml", replicas: 3, hpa: true},
	{file: "optional-nats-values.yaml", replicas: 3, nats: true},
	{file: "optional-nats-cluster-values.yaml", replicas: 3, nats: true, streamReplicas: 3},
	{file: "optional-nats-fullname-values.yaml", replicas: 3, nats: true},
	{file: "optional-nats-names-values.yaml", replicas: 3, nats: true},
	{file: "embedded-values.yaml", embedded: true, replicas: 1},
	{file: "embedded-cluster-values.yaml", embedded: true, clustered: true, replicas: 3, streamReplicas: 3},
	{file: "embedded-cluster-tls-values.yaml", embedded: true, clustered: true, replicas: 3, streamReplicas: 3,
		listen: chartListeners{tls: true}},
}

// TestHelmChartStatic checks what needs no helm: the shipped values files, the helper
// names, and the chart's copy of the cluster configuration.
func TestHelmChartStatic(t *testing.T) {
	t.Run("Variants", func(t *testing.T) {
		files, err := filepath.Glob(filepath.Join(helmChartDir, "ci", "*.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var got, want []string
		for _, f := range files {
			got = append(got, filepath.Base(f))
		}
		for _, v := range chartVariants {
			want = append(want, v.file)
		}
		sort.Strings(want)
		if !slices.Equal(got, want) {
			t.Errorf("ci values %v, but the test judges %v: every shipped variant is judged", got, want)
		}
	})

	t.Run("BlockStyle", func(t *testing.T) {
		values := []string{filepath.Join(helmChartDir, "values.yaml"), filepath.Join(helmChartDir, "Chart.yaml")}
		for _, dir := range []string{filepath.Join(helmChartDir, "ci"), sharedPodValues} {
			fs, _ := filepath.Glob(filepath.Join(dir, "*.yaml"))
			values = append(values, fs...)
		}
		for _, file := range values {
			for _, doc := range decodeYAMLNodes(t, file) {
				if line, ok := nonEmptyFlowNode(doc); ok {
					t.Errorf("%s:%d: flow-style YAML; the chart's values are block style", file, line)
				}
			}
		}
	})

	// Helm's named templates are one namespace across the chart and its subcharts, and
	// the NATS chart defines jsonpatch, tplYaml and their helpers under the same names the
	// copies here came with. An unprefixed name would resolve to the subchart's.
	t.Run("TemplateNames", func(t *testing.T) {
		re := regexp.MustCompile(`\b(define|include|template)\s+"([^"]+)"`)
		var files []string
		for _, dir := range []string{"templates", "files"} {
			fs, _ := filepath.Glob(filepath.Join(helmChartDir, dir, "*"))
			files = append(files, fs...)
		}
		seen := 0
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range re.FindAllStringSubmatch(string(data), -1) {
				seen++
				if !strings.HasPrefix(m[2], "mqtt-nats-broker.") {
					t.Errorf("%s: %s %q is not prefixed mqtt-nats-broker.", filepath.Base(f), m[1], m[2])
				}
			}
		}
		if seen == 0 {
			t.Fatal("no template names found; the check proves nothing")
		}
	})

	t.Run("ClusterConfigCopy", func(t *testing.T) {
		chartCopy, err := os.ReadFile(filepath.Join(helmChartDir, "files", "nats-cluster.conf"))
		if err != nil {
			t.Fatal(err)
		}
		source, err := os.ReadFile("../../embedded/nats-cluster.conf")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(chartCopy, source) {
			t.Error("charts/mqtt-nats-broker/files/nats-cluster.conf differs from embedded/nats-cluster.conf; Helm cannot read outside the chart, so the copy must stay identical")
		}
	})
}

// nonEmptyFlowNode is flowNode allowing the empty {} and [] the values use for an
// unset merge or patch.
func nonEmptyFlowNode(n *yaml.Node) (int, bool) {
	if n.Style&yaml.FlowStyle != 0 && (n.Kind == yaml.MappingNode || n.Kind == yaml.SequenceNode) && len(n.Content) > 0 {
		return n.Line, true
	}
	for _, c := range n.Content {
		if line, ok := nonEmptyFlowNode(c); ok {
			return line, true
		}
	}
	return 0, false
}

func helmBin(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("MQTTNB_HELM")
	if bin == "" {
		t.Skip("MQTTNB_HELM not set; run make helm-check")
	}
	return bin
}

// helmTemplate renders chart with the values files and returns its objects; a failure to
// render fails the test.
func helmTemplate(t *testing.T, bin, chart string, values ...string) []map[string]any {
	t.Helper()
	out, err := helmRender(bin, chart, values...)
	if err != nil {
		t.Fatalf("helm template %s %v: %v", chart, values, err)
	}
	var objs []map[string]any
	for _, doc := range decodeYAMLStream(t, chart, out) {
		var obj map[string]any
		if err := doc.Decode(&obj); err != nil {
			t.Fatal(err)
		}
		if obj != nil {
			objs = append(objs, obj)
		}
	}
	return objs
}

func helmRender(bin, chart string, values ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"template", helmRelease, chart, "--namespace", helmNamespace}
	for _, v := range values {
		args = append(args, "-f", v)
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%v: %s", err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// TestHelmChart renders every shipped variant and judges it.
func TestHelmChart(t *testing.T) {
	bin := helmBin(t)
	for _, v := range chartVariants {
		t.Run(strings.TrimSuffix(v.file, "-values.yaml"), func(t *testing.T) {
			objs := helmTemplate(t, bin, helmChartDir, filepath.Join(helmChartDir, "ci", v.file))
			checkChartRender(t, v, objs)
		})
	}
}

// chartObjects separates the broker's objects (by kind and name) from the optional
// dependency's.
type chartObjects struct {
	broker map[string]map[string]any // "Kind/name"
	nats   []map[string]any
}

func splitChartObjects(t *testing.T, objs []map[string]any) chartObjects {
	t.Helper()
	co := chartObjects{broker: map[string]map[string]any{}}
	for _, obj := range objs {
		chart, _ := field(obj, "metadata", "labels", "helm.sh/chart")
		switch {
		case strings.HasPrefix(str(chart), "nats-"):
			co.nats = append(co.nats, obj)
		case strings.HasPrefix(str(chart), "mqtt-nats-broker-"):
			key := str(obj["kind"]) + "/" + str(mustField(t, obj, "metadata", "name"))
			if _, dup := co.broker[key]; dup {
				t.Fatalf("%s rendered twice", key)
			}
			co.broker[key] = obj
		default:
			t.Fatalf("an object of no known chart: %s %v", obj["kind"], obj["metadata"])
		}
	}
	return co
}

func checkChartRender(t *testing.T, v chartVariant, objs []map[string]any) {
	t.Helper()
	co := splitChartObjects(t, objs)
	full := helmRelease + "-mqtt-nats-broker"

	// The broker's objects, exactly.
	want := []string{"Service/" + full, "PodDisruptionBudget/" + full}
	if v.embedded {
		want = append(want, "StatefulSet/"+full, "Service/"+full+"-headless")
		if v.clustered {
			want = append(want, "ConfigMap/"+full+"-config", "NetworkPolicy/"+full)
		}
	} else {
		want = append(want, "Deployment/"+full)
		if v.hpa {
			want = append(want, "HorizontalPodAutoscaler/"+full)
		}
	}
	got := slices.Sorted(maps.Keys(co.broker))
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("broker objects %v, want exactly %v", got, want)
	}
	if v.nats != (len(co.nats) > 0) {
		t.Fatalf("the NATS dependency rendered %d objects; enabled=%v", len(co.nats), v.nats)
	}

	kind := "Deployment"
	if v.embedded {
		kind = "StatefulSet"
	}
	wl := co.broker[kind+"/"+full]
	svc := co.broker["Service/"+full]
	checkKeys(t, kind, wl, chartWorkloadKeys(v))
	checkKeys(t, "Service", svc, chartServiceKeys)

	pod := mustMap(t, wl, "spec", "template", "spec")
	containers := mustList(t, pod, "containers")
	if len(containers) != 1 || str(mustField(t, containers[0], "name")) != "broker" {
		t.Fatalf("want the single container broker, got %d containers", len(containers))
	}
	c := mustMap(t, containers, 0)
	checkSecurity(t, pod, c)
	if pod["enableServiceLinks"] != false {
		t.Errorf("enableServiceLinks = %v, want false: a Service link named like an MQTTNB_ setting would configure the broker", pod["enableServiceLinks"])
	}
	if fs, has := field(pod, "securityContext", "fsGroup"); v.embedded != has || (has && fs != runtimeUser) {
		t.Errorf("pod fsGroup %v (set %v): want %d exactly when the pod has a JetStream store", fs, has, runtimeUser)
	}

	image := str(c["image"])
	wantImage := "ghcr.io/nberlee/mqtt-nats-broker:0.1.0-alpha.1"
	if v.embedded {
		wantImage += "-embedded"
	}
	if image != wantImage {
		t.Errorf("image %q, want %q: the %s topology's image at the chart's appVersion", image, wantImage, kind)
	}

	env := checkChartEnvironment(t, v, c, co, full)
	cfg := env.cfg
	ports := chartContainerPorts(t, c, v)
	for name, want := range map[string]int{"mqtt": cfg.MQTTPort, "mqtt-plain": cfg.PlainPort, "mqtt-ws": cfg.WSPort} {
		if ports[name] != want {
			t.Errorf("container port %s %d, but the broker listens on %d", name, ports[name], want)
		}
	}
	httpHost, httpPort, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		t.Fatalf("HTTPAddr %q: %v", cfg.HTTPAddr, err)
	}
	for what, host := range map[string]string{"MQTT": cfg.MQTTHost, "health": httpHost, "WebSocket": cfg.WSHost} {
		if ip := net.ParseIP(host); host != "" && (ip == nil || !ip.IsUnspecified()) {
			t.Errorf("the %s listener binds %q; the chart needs every address", what, host)
		}
	}
	if strconv.Itoa(ports["health"]) != httpPort {
		t.Errorf("container port health %d, but the health server listens on %s", ports["health"], httpPort)
	}

	checkProbes(t, c)
	checkStartupProbe(t, c, v.embedded)
	need := cfg.DrainDelay + chartShutdownBudget()
	if v.embedded {
		need += embeddedShutdownHeadroom
	}
	if secs, _ := mustField(t, pod, "terminationGracePeriodSeconds").(int); time.Duration(secs)*time.Second < need {
		t.Errorf("terminationGracePeriodSeconds %d < %s: the drain delay, the named shutdown budgets and the headroom", secs, need)
	}

	checkChartService(t, wl, svc, cfg, v.listen)
	checkChartTLS(t, pod, c, cfg, v.listen.tls)

	pdb := co.broker["PodDisruptionBudget/"+full]
	checkKeys(t, "PodDisruptionBudget", pdb, chartPDBKeys)
	if mustField(t, pdb, "spec", "maxUnavailable") != 1 {
		t.Errorf("PodDisruptionBudget maxUnavailable %v, want 1", mustField(t, pdb, "spec", "maxUnavailable"))
	}
	checkSelects(t, "PodDisruptionBudget", mustMap(t, pdb, "spec", "selector", "matchLabels"), wl)

	if v.hpa {
		hpa := co.broker["HorizontalPodAutoscaler/"+full]
		checkKeys(t, "HorizontalPodAutoscaler", hpa, chartHPAKeys)
		if ref := mustMap(t, hpa, "spec", "scaleTargetRef"); ref["kind"] != "Deployment" || ref["name"] != full || ref["apiVersion"] != "apps/v1" {
			t.Errorf("HPA targets %v, want the Deployment %s", ref, full)
		}
		if _, ok := field(wl, "spec", "replicas"); ok {
			t.Error("the Deployment sets replicas beside an autoscaler, which would reset its count on every upgrade")
		}
	} else if r, _ := field(wl, "spec", "replicas"); r != v.replicas {
		t.Errorf("replicas %v, want %d", r, v.replicas)
	}

	if v.embedded {
		checkEmbeddedTopology(t, v, co, wl, pod, c, env, full)
	}
	if v.nats {
		checkNATSDependency(t, co, cfg)
	}
}

// chartShutdownBudget is what the binary runs after the drain delay: the client drain,
// the health server and the NATS service connection's close, plus the headroom.
func chartShutdownBudget() time.Duration {
	return mqtt.ShutdownDrainTimeout + httpShutdownTimeout + backend.ServiceCloseBudget + graceHeadroom
}

// checkStartupProbe requires the embedded topology's startupProbe on a path the health
// handler serves, bounding the JetStream wait, and no startupProbe otherwise.
func checkStartupProbe(t *testing.T, c map[string]any, embedded bool) {
	t.Helper()
	sp, has := c["startupProbe"]
	if has != embedded {
		t.Fatalf("startupProbe present=%v, want %v: only the embedded topology waits for JetStream before serving HTTP", has, embedded)
	}
	if !has {
		return
	}
	rec := httptest.NewRecorder()
	healthMux(nil, new(atomic.Bool)).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, str(mustField(t, sp, "httpGet", "path")), nil))
	if rec.Code != http.StatusOK || mustField(t, sp, "httpGet", "port") != "health" {
		t.Errorf("startupProbe %v: the health handler answers %d; want 200 on the health port", sp, rec.Code)
	}
	period, _ := mustField(t, sp, "periodSeconds").(int)
	failures, _ := mustField(t, sp, "failureThreshold").(int)
	if period*failures < 60 {
		t.Errorf("startupProbe allows %d s; a clustered cold start needs its quorum to form", period*failures)
	}
}

// The fields the chart renders on the broker's objects, and nothing else (see
// checkKeys).
var (
	chartLabelKeys = map[string]any{
		"helm.sh/chart": leaf, "app.kubernetes.io/name": leaf, "app.kubernetes.io/instance": leaf,
		"app.kubernetes.io/component": leaf, "app.kubernetes.io/version": leaf, "app.kubernetes.io/managed-by": leaf,
	}
	chartSelectorKeys = map[string]any{
		"app.kubernetes.io/name": leaf, "app.kubernetes.io/instance": leaf, "app.kubernetes.io/component": leaf,
	}
	chartMetadataKeys = map[string]any{"name": leaf, "labels": chartLabelKeys}
	chartProbeKeys    = map[string]any{
		"httpGet":       map[string]any{"path": leaf, "port": leaf},
		"periodSeconds": leaf, "failureThreshold": leaf,
	}
	chartContainerKeys = map[string]any{
		"name": leaf, "image": leaf,
		"env": []any{map[string]any{
			"name": leaf, "value": leaf,
			"valueFrom": map[string]any{
				"secretKeyRef": map[string]any{"name": leaf, "key": leaf, "optional": leaf},
				"fieldRef":     map[string]any{"fieldPath": leaf},
			},
		}},
		"ports":          []any{map[string]any{"name": leaf, "containerPort": leaf}},
		"startupProbe":   chartProbeKeys,
		"readinessProbe": chartProbeKeys,
		"livenessProbe":  chartProbeKeys,
		"resources": map[string]any{
			"requests": map[string]any{"cpu": leaf, "memory": leaf},
			"limits":   map[string]any{"cpu": leaf, "memory": leaf},
		},
		"securityContext": map[string]any{
			"allowPrivilegeEscalation": leaf, "readOnlyRootFilesystem": leaf,
			"capabilities": map[string]any{"drop": leaf},
		},
		"volumeMounts": []any{map[string]any{"name": leaf, "mountPath": leaf, "readOnly": leaf}},
	}
	chartPodKeys = map[string]any{
		"automountServiceAccountToken": leaf, "enableServiceLinks": leaf, "terminationGracePeriodSeconds": leaf,
		"securityContext": map[string]any{
			"runAsNonRoot": leaf, "runAsUser": leaf, "runAsGroup": leaf, "fsGroup": leaf,
			"seccompProfile": map[string]any{"type": leaf},
		},
		"containers": []any{chartContainerKeys},
		"volumes": []any{map[string]any{
			"name": leaf, "secret": map[string]any{"secretName": leaf}, "configMap": map[string]any{"name": leaf},
		}},
	}
	chartServiceKeys = map[string]any{
		"apiVersion": leaf, "kind": leaf, "metadata": chartMetadataKeys,
		"spec": map[string]any{
			"type": leaf, "selector": chartSelectorKeys, "clusterIP": leaf, "publishNotReadyAddresses": leaf,
			"ports": []any{map[string]any{"name": leaf, "port": leaf, "targetPort": leaf}},
		},
	}
	chartPDBKeys = map[string]any{
		"apiVersion": leaf, "kind": leaf, "metadata": chartMetadataKeys,
		"spec": map[string]any{"maxUnavailable": leaf, "selector": map[string]any{"matchLabels": chartSelectorKeys}},
	}
	chartHPAKeys = map[string]any{
		"apiVersion": leaf, "kind": leaf, "metadata": chartMetadataKeys,
		"spec": map[string]any{
			"scaleTargetRef": map[string]any{"apiVersion": leaf, "kind": leaf, "name": leaf},
			"minReplicas":    leaf, "maxReplicas": leaf,
			"metrics": []any{map[string]any{"type": leaf, "resource": map[string]any{
				"name": leaf, "target": map[string]any{"type": leaf, "averageUtilization": leaf},
			}}},
		},
	}
	chartNetworkPolicyKeys = map[string]any{
		"apiVersion": leaf, "kind": leaf, "metadata": chartMetadataKeys,
		"spec": map[string]any{
			"podSelector": map[string]any{"matchLabels": chartSelectorKeys}, "policyTypes": leaf,
			"ingress": []any{map[string]any{
				"from":  []any{map[string]any{"podSelector": map[string]any{"matchLabels": chartSelectorKeys}}},
				"ports": []any{map[string]any{"port": leaf}},
			}},
		},
	}
	chartConfigMapKeys = map[string]any{
		"apiVersion": leaf, "kind": leaf, "metadata": chartMetadataKeys, "data": map[string]any{"nats.conf": leaf},
	}
)

func chartWorkloadKeys(v chartVariant) map[string]any {
	template := map[string]any{"metadata": map[string]any{"labels": chartLabelKeys}, "spec": chartPodKeys}
	spec := map[string]any{
		"replicas": leaf, "selector": map[string]any{"matchLabels": chartSelectorKeys}, "template": template,
	}
	if v.embedded {
		spec["serviceName"] = leaf
		spec["podManagementPolicy"] = leaf
		spec["volumeClaimTemplates"] = []any{map[string]any{
			"metadata": map[string]any{"name": leaf},
			"spec": map[string]any{
				"accessModes": leaf, "resources": map[string]any{"requests": map[string]any{"storage": leaf}},
			},
		}}
		if v.clustered {
			template["metadata"] = map[string]any{"labels": chartLabelKeys, "annotations": map[string]any{"checksum/config": leaf}}
		}
	}
	return map[string]any{"apiVersion": leaf, "kind": leaf, "metadata": chartMetadataKeys, "spec": spec}
}

// checkSelects requires selector to be the workload's own selector, matching its pods.
func checkSelects(t *testing.T, what string, selector map[string]any, wl map[string]any) {
	t.Helper()
	if fmt.Sprint(selector) != fmt.Sprint(mustMap(t, wl, "spec", "selector", "matchLabels")) {
		t.Errorf("%s selector %v differs from the workload selector", what, selector)
	}
	labels := mustMap(t, wl, "spec", "template", "metadata", "labels")
	for k, v := range selector {
		if labels[k] != v {
			t.Errorf("%s selector %s=%v does not match the pod labels %v", what, k, v, labels)
		}
	}
}

// chartEnv is what checkChartEnvironment derives: the configuration config.Load builds,
// and the container's literal values and reference names.
type chartEnv struct {
	cfg      *config.Config
	values   map[string]string // literal values, and placeholders for references
	fieldRef map[string]string
}

// clusterConfigEnvNames returns the MQTTNB_ variables the embedded cluster configuration
// references: the ones nats-server reads from the environment, beside internal/config's.
func clusterConfigEnvNames(t *testing.T) map[string]bool {
	t.Helper()
	data, err := os.ReadFile("../../embedded/nats-cluster.conf")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		for _, m := range regexp.MustCompile(`\$(MQTTNB_[A-Z_]+)`).FindAllStringSubmatch(line, -1) {
			names[m[1]] = true
		}
	}
	if len(names) == 0 {
		t.Fatal("no MQTTNB_ references in embedded/nats-cluster.conf")
	}
	return names
}

// routeRef is the userinfo every route URL must carry: the two Secret-sourced route
// credentials, expanded by the kubelet, never a literal.
const routeRef = "$(MQTTNB_EMBEDDED_ROUTE_USER):$(MQTTNB_EMBEDDED_ROUTE_PASS)@"

func checkChartEnvironment(t *testing.T, v chartVariant, c map[string]any, co chartObjects, full string) chartEnv {
	t.Helper()
	known := configEnvNames(t)
	for name := range clusterConfigEnvNames(t) {
		known[name] = true
	}
	allowed := []string{
		"MQTTNB_NATS_USER", "MQTTNB_NATS_PASS", "MQTTNB_DEFAULT_USER", "MQTTNB_DEFAULT_PASS",
		"MQTTNB_LOG_LEVEL", "MQTTNB_DRAIN_DELAY", "MQTTNB_PORT",
	}
	if !v.embedded {
		allowed = append(allowed, "MQTTNB_NATS_URLS")
	}
	if v.listen.tls {
		allowed = append(allowed, "MQTTNB_TLS_CERT", "MQTTNB_TLS_KEY")
	}
	if v.listen.plain {
		allowed = append(allowed, "MQTTNB_PLAIN_PORT")
	}
	if v.listen.ws {
		allowed = append(allowed, "MQTTNB_WS_PORT", "MQTTNB_WS_PATH")
	}
	if v.streamReplicas != 0 {
		allowed = append(allowed, "MQTTNB_STREAM_REPLICAS")
	}
	if v.clustered {
		allowed = append(allowed, "MQTTNB_NATS_EMBEDDED_CONFIG", "MQTTNB_EMBEDDED_SERVER_NAME",
			"MQTTNB_EMBEDDED_ROUTE_USER", "MQTTNB_EMBEDDED_ROUTE_PASS", "MQTTNB_EMBEDDED_ROUTES")
	}
	// The default identity is optional only against a NATS the operator runs: the
	// embedded configurations and the dependency's account block reference it.
	defaultOptional := !v.embedded && !v.nats
	type ref struct {
		secret, key string
		optional    bool
	}
	wantRefs := map[string]ref{
		"MQTTNB_NATS_USER":    {natsSecretName, "user", false},
		"MQTTNB_NATS_PASS":    {natsSecretName, "password", false},
		"MQTTNB_DEFAULT_USER": {natsSecretName, "default-user", defaultOptional},
		"MQTTNB_DEFAULT_PASS": {natsSecretName, "default-password", defaultOptional},
	}
	if v.clustered {
		wantRefs["MQTTNB_EMBEDDED_ROUTE_USER"] = ref{routeSecretName, "user", false}
		wantRefs["MQTTNB_EMBEDDED_ROUTE_PASS"] = ref{routeSecretName, "password", false}
	}

	out := chartEnv{values: map[string]string{}, fieldRef: map[string]string{}}
	declared := map[string]bool{}
	for _, e := range mustList(t, c, "env") {
		name := str(mustField(t, e, "name"))
		if !known[name] {
			t.Errorf("env %s is read by neither internal/config nor the embedded cluster configuration", name)
		}
		if !slices.Contains(allowed, name) {
			t.Errorf("env %s is not one this variant sets, so the checks cannot judge it", name)
		}
		if declared[name] {
			t.Errorf("env %s is set twice", name)
		}
		if r, ok := field(e, "valueFrom", "secretKeyRef"); ok {
			w, expected := wantRefs[name]
			delete(wantRefs, name)
			opt, _ := field(r, "optional")
			if !expected || str(mustField(t, r, "name")) != w.secret || str(mustField(t, r, "key")) != w.key || (opt == true) != w.optional {
				t.Errorf("env %s takes %v; want Secret %s key %q optional=%v", name, r, w.secret, w.key, w.optional)
			}
			out.values[name] = "placeholder-" + strings.ToLower(strings.TrimPrefix(name, "MQTTNB_"))
			declared[name] = true
			continue
		}
		if fr, ok := field(e, "valueFrom", "fieldRef", "fieldPath"); ok {
			if name != "MQTTNB_EMBEDDED_SERVER_NAME" || fr != "metadata.name" {
				t.Errorf("env %s from field %v: only the server name comes from the pod name", name, fr)
			}
			out.fieldRef[name] = str(fr)
			declared[name] = true
			continue
		}
		if _, isRef := wantRefs[name]; isRef {
			t.Errorf("env %s is a credential and must come from a Secret", name)
		}
		val, ok := field(e, "value")
		if !ok {
			t.Errorf("env %s has neither a value nor a reference", name)
		}
		s := str(val)
		if name == "MQTTNB_EMBEDDED_ROUTES" {
			checkRouteList(t, s, declared, v, full)
		} else {
			// The kubelet rewrites a value wherever a $ appears, and @ delimits URL
			// userinfo: no literal but the route list needs either.
			if strings.Contains(s, "$") {
				t.Errorf("env %s = %q: the kubelet would rewrite it", name, s)
			}
			if strings.Contains(s, "@") {
				t.Errorf("env %s = %q: a literal may not carry userinfo; credentials come from Secrets", name, s)
			}
		}
		out.values[name] = s
		declared[name] = true
	}
	for name := range wantRefs {
		t.Errorf("env %s is not set from its Secret", name)
	}
	for _, name := range allowed {
		if !declared[name] {
			t.Errorf("env %s, which this variant sets, is missing", name)
		}
	}
	if v.streamReplicas != 0 && out.values["MQTTNB_STREAM_REPLICAS"] != strconv.Itoa(v.streamReplicas) {
		t.Errorf("MQTTNB_STREAM_REPLICAS %q, want %d", out.values["MQTTNB_STREAM_REPLICAS"], v.streamReplicas)
	}

	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "MQTTNB_") {
			t.Setenv(name, "")
			_ = os.Unsetenv(name)
		}
	}
	// The embedded image's own ENV, which a container that does not set it inherits.
	if v.embedded {
		if _, set := out.values["MQTTNB_NATS_EMBEDDED_CONFIG"]; !set {
			t.Setenv("MQTTNB_NATS_EMBEDDED_CONFIG", dockerfileEnv(t, "embedded", "MQTTNB_NATS_EMBEDDED_CONFIG"))
		}
	}
	configNames := configEnvNames(t)
	for name, val := range out.values {
		if configNames[name] {
			t.Setenv(name, val)
		}
	}
	cfg, err := config.Load(nil)
	if err != nil {
		t.Fatalf("the binary refuses the chart's environment: %v", err)
	}
	if v.embedded != (cfg.NATSEmbeddedConfig != "") {
		t.Fatalf("embedded configuration %q in the %v topology", cfg.NATSEmbeddedConfig, map[bool]string{true: "embedded", false: "external"}[v.embedded])
	}
	if !v.embedded {
		for _, raw := range strings.Split(cfg.NATSURLs, ",") {
			u, err := url.Parse(strings.TrimSpace(raw))
			if err != nil || (u.Scheme != "nats" && u.Scheme != "tls") || !strings.Contains(raw, "://") || u.User != nil {
				t.Errorf("NATS URL %q: want an explicit nats:// or tls:// URL without userinfo", raw)
			}
		}
	}
	out.cfg = cfg
	return out
}

// checkRouteList requires the route list to name every ordinal of the StatefulSet exactly
// once on the headless Service's DNS name and the route port, each URL with exactly the
// two route credential references, both declared earlier in the list (the kubelet expands
// only those).
func checkRouteList(t *testing.T, s string, declared map[string]bool, v chartVariant, full string) {
	t.Helper()
	for _, dep := range []string{"MQTTNB_EMBEDDED_ROUTE_USER", "MQTTNB_EMBEDDED_ROUTE_PASS"} {
		if !declared[dep] {
			t.Errorf("MQTTNB_EMBEDDED_ROUTES precedes %s: the kubelet expands only variables declared before it", dep)
		}
	}
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		t.Fatalf("MQTTNB_EMBEDDED_ROUTES %q: want a NATS list", s)
	}
	var hosts []string
	for _, u := range strings.Split(strings.Trim(s, "[]"), ",") {
		u = strings.TrimSpace(u)
		rest, ok := strings.CutPrefix(u, "nats-route://"+routeRef)
		if !ok || strings.Contains(rest, "@") || strings.Contains(rest, "$") {
			t.Errorf("route %q: want nats-route://%s<host>:6222, the credentials only as the two references", u, routeRef)
			continue
		}
		hosts = append(hosts, rest)
	}
	var want []string
	for i := range v.replicas {
		want = append(want, fmt.Sprintf("%s-%d.%s-headless:6222", full, i, full))
	}
	if !slices.Equal(hosts, want) {
		t.Errorf("route hosts %v, want every peer once: %v", hosts, want)
	}
}

// dockerfileEnv returns an ENV value of a Dockerfile stage.
func dockerfileEnv(t *testing.T, stage, name string) string {
	t.Helper()
	data, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	in := false
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == "FROM" {
			in = len(f) >= 4 && f[len(f)-2] == "AS" && f[len(f)-1] == stage
		}
		if in && len(f) == 2 && f[0] == "ENV" {
			if k, val, ok := strings.Cut(f[1], "="); ok && k == name {
				return val
			}
		}
	}
	t.Fatalf("Dockerfile stage %s sets no ENV %s", stage, name)
	return ""
}

// dockerfileCopySource returns the repository file a Dockerfile COPY puts at dest.
func dockerfileCopySource(t *testing.T, dest string) string {
	t.Helper()
	data, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if f := strings.Fields(line); len(f) == 3 && f[0] == "COPY" && f[2] == dest {
			return filepath.Join("../..", f[1])
		}
	}
	t.Fatalf("no Dockerfile COPY puts a file at %s", dest)
	return ""
}

// chartContainerPorts requires exactly the variant's ports on distinct numbers.
func chartContainerPorts(t *testing.T, c map[string]any, v chartVariant) map[string]int {
	t.Helper()
	names := []string{"mqtt", "health"}
	if v.listen.plain {
		names = append(names, "mqtt-plain")
	}
	if v.listen.ws {
		names = append(names, "mqtt-ws")
	}
	if v.clustered {
		names = append(names, "route")
	}
	ports := map[string]int{}
	numbers := map[int]string{}
	list := mustList(t, c, "ports")
	for _, p := range list {
		n, _ := mustField(t, p, "containerPort").(int)
		name := str(mustField(t, p, "name"))
		if other, dup := numbers[n]; dup {
			t.Errorf("%s and %s share port %d", other, name, n)
		}
		ports[name], numbers[n] = n, name
	}
	if len(list) != len(names) || !slices.Equal(slices.Sorted(maps.Keys(ports)), slices.Sorted(slices.Values(names))) {
		t.Errorf("container ports %v: want exactly %v", ports, names)
	}
	return ports
}

// checkChartService requires the Service to select the workload's pods and to expose
// exactly the MQTT listeners.
func checkChartService(t *testing.T, wl, svc map[string]any, cfg *config.Config, l chartListeners) {
	t.Helper()
	checkSelects(t, "Service", mustMap(t, svc, "spec", "selector"), wl)
	if _, ok := field(svc, "spec", "clusterIP"); ok {
		t.Error("the client Service sets clusterIP")
	}
	want := map[string]int{"mqtt": cfg.MQTTPort}
	if l.plain {
		want["mqtt-plain"] = cfg.PlainPort
	}
	if l.ws {
		want["mqtt-ws"] = cfg.WSPort
	}
	got := map[string]int{}
	list := mustList(t, svc, "spec", "ports")
	for _, p := range list {
		port, _ := mustField(t, p, "port").(int)
		if str(mustField(t, p, "name")) != str(mustField(t, p, "targetPort")) {
			t.Errorf("Service port %v: the name and the target differ", p)
		}
		got[str(mustField(t, p, "targetPort"))] = port
	}
	if len(list) != len(want) || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("Service ports %v: want exactly %v", got, want)
	}
}

// checkChartTLS is checkTLS for a pod that may carry other volumes: with TLS, one
// read-only directory mount of the TLS Secret whose two keys the broker loads; without,
// no TLS volume and no listener TLS.
func checkChartTLS(t *testing.T, pod, c map[string]any, cfg *config.Config, wantTLS bool) {
	t.Helper()
	var tlsVols []map[string]any
	vols, _ := pod["volumes"].([]any)
	for _, v := range vols {
		if _, ok := field(v, "secret"); ok {
			tlsVols = append(tlsVols, v.(map[string]any))
		}
	}
	if !wantTLS {
		if cfg.TLSEnabled() || len(tlsVols) > 0 {
			t.Errorf("the plaintext variant configures TLS (%v) or mounts a Secret (%d)", cfg.TLSEnabled(), len(tlsVols))
		}
		return
	}
	if len(tlsVols) != 1 {
		t.Fatalf("want one Secret volume, got %d", len(tlsVols))
	}
	vol := tlsVols[0]
	var mount map[string]any
	mounts, _ := c["volumeMounts"].([]any)
	for _, m := range mounts {
		if str(mustField(t, m, "name")) == str(vol["name"]) {
			if mount != nil {
				t.Fatal("the TLS volume is mounted twice")
			}
			mount = m.(map[string]any)
		}
	}
	if mount == nil {
		t.Fatal("the TLS volume is not mounted")
	}
	checkTLS(t, map[string]any{"volumes": []any{vol}}, map[string]any{"volumeMounts": []any{mount}}, cfg, true)
}

// checkEmbeddedTopology judges what only the embedded topology renders: the StatefulSet's
// ordering and store, the headless Service, the network policy and the nats-server
// configuration, which is loaded and enforced as the binary would.
func checkEmbeddedTopology(t *testing.T, v chartVariant, co chartObjects, sts, pod, c map[string]any, env chartEnv, full string) {
	t.Helper()
	headless := co.broker["Service/"+full+"-headless"]
	checkKeys(t, "headless Service", headless, chartServiceKeys)
	if mustField(t, sts, "spec", "podManagementPolicy") != "Parallel" {
		t.Error("podManagementPolicy must be Parallel: every peer is part of the quorum the others wait for")
	}
	if mustField(t, sts, "spec", "serviceName") != full+"-headless" {
		t.Errorf("serviceName %v, want the headless Service", mustField(t, sts, "spec", "serviceName"))
	}
	if mustField(t, headless, "spec", "clusterIP") != "None" || mustField(t, headless, "spec", "publishNotReadyAddresses") != true {
		t.Error("the headless Service must be clusterIP None with publishNotReadyAddresses: route names resolve before any peer is ready")
	}
	checkSelects(t, "headless Service", mustMap(t, headless, "spec", "selector"), sts)
	hports, _ := field(headless, "spec", "ports")
	if v.clustered {
		l, _ := hports.([]any)
		if len(l) != 1 || mustField(t, l[0], "port") != 6222 || mustField(t, l[0], "targetPort") != "route" {
			t.Errorf("headless ports %v, want the route port 6222 only", hports)
		}
	} else if hports != nil {
		t.Errorf("headless ports %v: an unclustered server has no routes", hports)
	}

	claims := mustList(t, sts, "spec", "volumeClaimTemplates")
	if len(claims) != 1 || mustField(t, claims[0], "metadata", "name") != "data" ||
		!slices.Equal(anyStrings(mustField(t, claims[0], "spec", "accessModes")), []string{"ReadWriteOnce"}) {
		t.Fatalf("volumeClaimTemplates %v: want the one ReadWriteOnce claim data", claims)
	}
	dataMount := ""
	for _, m := range mustList(t, c, "volumeMounts") {
		if mustField(t, m, "name") == "data" {
			dataMount = str(mustField(t, m, "mountPath"))
			if ro, _ := field(m, "readOnly"); ro == true {
				t.Error("the store is mounted read-only")
			}
		}
	}

	// The configuration the server will run: the image's baked file unless the
	// container names the ConfigMap's.
	confPath := env.cfg.NATSEmbeddedConfig
	var conf []byte
	var err error
	if v.clustered {
		cm := co.broker["ConfigMap/"+full+"-config"]
		checkKeys(t, "ConfigMap", cm, chartConfigMapKeys)
		conf = []byte(str(mustField(t, cm, "data", "nats.conf")))
		source, err := os.ReadFile("../../embedded/nats-cluster.conf")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(conf, source) {
			t.Error("the ConfigMap's nats.conf differs from embedded/nats-cluster.conf")
		}
		var cmMount string
		for _, m := range mustList(t, c, "volumeMounts") {
			if mustField(t, m, "name") == "config" {
				cmMount = str(mustField(t, m, "mountPath"))
			}
		}
		if confPath != path.Join(cmMount, "nats.conf") {
			t.Errorf("MQTTNB_NATS_EMBEDDED_CONFIG %s is not the ConfigMap's nats.conf under %s", confPath, cmMount)
		}
		foundVol := false
		for _, vol := range mustList(t, pod, "volumes") {
			if mustField(t, vol, "name") == "config" {
				foundVol = str(mustField(t, vol, "configMap", "name")) == full+"-config"
			}
		}
		if !foundVol {
			t.Error("the config volume is not the chart's ConfigMap")
		}
		if _, ok := field(sts, "spec", "template", "metadata", "annotations", "checksum/config"); !ok {
			t.Error("no checksum/config annotation: a configuration change would not roll the pods, and there is no reload")
		}

		np := co.broker["NetworkPolicy/"+full]
		checkKeys(t, "NetworkPolicy", np, chartNetworkPolicyKeys)
		checkNetworkPolicy(t, np, sts, v)
	} else {
		conf, err = os.ReadFile(dockerfileCopySource(t, confPath))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Load it with the environment the kubelet gives the first peer.
	ordinal0 := full + "-0"
	vars := map[string]string{}
	for name, val := range env.values {
		vars[name] = val
	}
	if v.clustered {
		vars["MQTTNB_EMBEDDED_SERVER_NAME"] = ordinal0
		vars["MQTTNB_EMBEDDED_ROUTES"] = kubeletExpand(env.values["MQTTNB_EMBEDDED_ROUTES"], vars)
	}
	for name, val := range vars {
		t.Setenv(name, val)
	}
	file := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(file, conf, 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(file)
	if err != nil {
		t.Fatalf("nats-server refuses the configuration with the chart's environment: %v", err)
	}
	if err := enforceEmbeddedOptions(opts, env.cfg.JSDomain); err != nil {
		t.Fatalf("the broker refuses the configuration with the chart's environment: %v", err)
	}
	if opts.StoreDir != dataMount {
		t.Errorf("store_dir %q is not the claim's mount %q", opts.StoreDir, dataMount)
	}
	if v.clustered {
		if opts.ServerName != ordinal0 {
			t.Errorf("server_name %q, want the pod name %s", opts.ServerName, ordinal0)
		}
		if opts.Cluster.Port != 6222 || len(opts.Routes) != v.replicas {
			t.Errorf("cluster port %d with %d routes, want 6222 and %d", opts.Cluster.Port, len(opts.Routes), v.replicas)
		}
		for _, r := range opts.Routes {
			if r.User.Username() != vars["MQTTNB_EMBEDDED_ROUTE_USER"] {
				t.Errorf("route %s presents user %q, not the route user", r.Host, r.User.Username())
			}
		}
	}
}

// kubeletExpand resolves $(NAME) the way the kubelet does for variables declared earlier.
func kubeletExpand(s string, vars map[string]string) string {
	return regexp.MustCompile(`\$\(([A-Z_]+)\)`).ReplaceAllStringFunc(s, func(m string) string {
		if v, ok := vars[m[2:len(m)-1]]; ok {
			return v
		}
		return m
	})
}

// checkNetworkPolicy requires the route port to be reachable only from the broker's own
// pods, and the MQTT and health ports from anywhere.
func checkNetworkPolicy(t *testing.T, np, sts map[string]any, v chartVariant) {
	t.Helper()
	checkSelects(t, "NetworkPolicy", mustMap(t, np, "spec", "podSelector", "matchLabels"), sts)
	if !slices.Equal(anyStrings(mustField(t, np, "spec", "policyTypes")), []string{"Ingress"}) {
		t.Errorf("policyTypes %v, want [Ingress]", mustField(t, np, "spec", "policyTypes"))
	}
	open := []string{"health", "mqtt"}
	if v.listen.plain {
		open = append(open, "mqtt-plain")
	}
	if v.listen.ws {
		open = append(open, "mqtt-ws")
	}
	sort.Strings(open)
	var sawRoute, sawOpen bool
	for _, rule := range mustList(t, np, "spec", "ingress") {
		var ports []string
		for _, p := range mustList(t, rule, "ports") {
			ports = append(ports, str(mustField(t, p, "port")))
		}
		sort.Strings(ports)
		from, hasFrom := field(rule, "from")
		switch {
		case slices.Equal(ports, []string{"route"}) && hasFrom:
			l, _ := from.([]any)
			if len(l) != 1 {
				t.Errorf("route ingress from %v, want the broker's pods only", from)
			} else {
				checkSelects(t, "route ingress", mustMap(t, l[0], "podSelector", "matchLabels"), sts)
			}
			sawRoute = true
		case slices.Equal(ports, open) && !hasFrom:
			sawOpen = true
		default:
			t.Errorf("ingress rule %v: neither the route rule nor the open listeners %v", rule, open)
		}
	}
	if !sawRoute || !sawOpen {
		t.Errorf("ingress: route rule %v, open rule %v; want both", sawRoute, sawOpen)
	}
}

// checkNATSDependency judges the optional NATS chart's output as this chart configures
// it: the broker's URL is its rendered client Service, its configuration runs JetStream
// with the broker's consumer limit and denies anonymous MQTT clients the broker's
// internals, and every container it renders is Restricted.
func checkNATSDependency(t *testing.T, co chartObjects, cfg *config.Config) {
	t.Helper()
	var client, sts, cm map[string]any
	for _, obj := range co.nats {
		switch str(obj["kind"]) {
		case "Service":
			if cip, _ := field(obj, "spec", "clusterIP"); cip != "None" {
				client = obj
			}
		case "StatefulSet":
			sts = obj
		case "ConfigMap":
			cm = obj
		case "PodDisruptionBudget":
		default:
			t.Errorf("the NATS dependency renders a %s; the chart's defaults leave only the server", obj["kind"])
		}
	}
	if client == nil || sts == nil || cm == nil {
		t.Fatal("the NATS dependency rendered no client Service, StatefulSet or ConfigMap")
	}

	host := str(mustField(t, client, "metadata", "name"))
	if ns, ok := field(client, "metadata", "namespace"); ok && ns != helmNamespace {
		host += "." + str(ns)
	}
	port := 0
	for _, p := range mustList(t, client, "spec", "ports") {
		if mustField(t, p, "name") == "nats" {
			port, _ = mustField(t, p, "port").(int)
		}
	}
	if wantURL := fmt.Sprintf("nats://%s:%d", host, port); cfg.NATSURLs != wantURL {
		t.Errorf("MQTTNB_NATS_URLS %q, but the dependency's client Service is %s", cfg.NATSURLs, wantURL)
	}

	pod := mustMap(t, sts, "spec", "template", "spec")
	var natsC map[string]any
	for _, c := range mustList(t, pod, "containers") {
		cm := c.(map[string]any)
		checkRestrictedContainer(t, "nats pod "+str(cm["name"]), pod, cm, natsRuntimeUser)
		if cm["name"] == "nats" {
			natsC = cm
		}
	}
	replicas, _ := mustField(t, sts, "spec", "replicas").(int)
	vars := checkNATSContainerCredentials(t, natsC, replicas)

	conf := str(mustField(t, cm, "data", "nats.conf"))
	opts := loadNATSChartConfig(t, conf, vars)
	checkBrokerNATSOptions(t, opts, replicas)
}

// checkRestrictedContainer requires what the Restricted Pod Security standard asks of a
// container, with the pod or the container naming the user.
func checkRestrictedContainer(t *testing.T, where string, pod, c map[string]any, uid int) {
	t.Helper()
	sc, _ := field(pod, "securityContext")
	csc, _ := field(c, "securityContext")
	get := func(key ...any) any {
		if v, ok := field(csc, key...); ok {
			return v
		}
		v, _ := field(sc, key...)
		return v
	}
	if get("runAsNonRoot") != true || get("runAsUser") != uid || get("seccompProfile", "type") != "RuntimeDefault" {
		t.Errorf("%s: runAsNonRoot %v runAsUser %v seccomp %v, want true, %d, RuntimeDefault",
			where, get("runAsNonRoot"), get("runAsUser"), get("seccompProfile", "type"), uid)
	}
	if v, _ := field(csc, "allowPrivilegeEscalation"); v != false {
		t.Errorf("%s: allowPrivilegeEscalation %v, want false", where, v)
	}
	if v, _ := field(csc, "readOnlyRootFilesystem"); v != true {
		t.Errorf("%s: readOnlyRootFilesystem %v, want true", where, v)
	}
	if drop, _ := field(csc, "capabilities", "drop"); !slices.Equal(anyStrings(drop), []string{"ALL"}) {
		t.Errorf("%s: capabilities.drop %v, want [ALL]", where, drop)
	}
}

// natsRouteRef is the userinfo every route of a clustered NATS dependency must carry: the
// route credentials from their Secret, expanded by the kubelet, never a literal.
const natsRouteRef = "$(NATS_CLUSTER_USER):$(NATS_CLUSTER_PASS)@"

// checkNATSContainerCredentials requires the four variables the account block references
// on the nats container, from the broker's Secret, and with more than one server the route
// credentials from theirs and a route list of exactly the two references per peer; it
// returns the environment the kubelet would give the server, with fixtures for Secrets.
func checkNATSContainerCredentials(t *testing.T, c map[string]any, replicas int) map[string]string {
	t.Helper()
	if c == nil {
		t.Fatal("no nats container")
	}
	want := map[string]string{
		"MQTTNB_NATS_USER": "user", "MQTTNB_NATS_PASS": "password",
		"MQTTNB_DEFAULT_USER": "default-user", "MQTTNB_DEFAULT_PASS": "default-password",
	}
	secretOf := map[string]string{}
	for name := range want {
		secretOf[name] = natsSecretName
	}
	if replicas > 1 {
		want["NATS_CLUSTER_USER"], want["NATS_CLUSTER_PASS"] = "user", "password"
		secretOf["NATS_CLUSTER_USER"], secretOf["NATS_CLUSTER_PASS"] = routeSecretName, routeSecretName
	}
	vars := map[string]string{}
	sawRoutes := false
	for _, e := range mustList(t, c, "env") {
		name := str(mustField(t, e, "name"))
		if name == "NATS_ROUTES" {
			sawRoutes = true
			routes := str(mustField(t, e, "value"))
			if _, cu := vars["NATS_CLUSTER_USER"]; !cu {
				t.Error("NATS_ROUTES precedes NATS_CLUSTER_USER: the kubelet expands only variables declared before it")
			}
			if _, cp := vars["NATS_CLUSTER_PASS"]; !cp {
				t.Error("NATS_ROUTES precedes NATS_CLUSTER_PASS: the kubelet expands only variables declared before it")
			}
			n := 0
			for _, u := range strings.Split(strings.Trim(routes, "[]"), ",") {
				rest, ok := strings.CutPrefix(strings.TrimSpace(u), "nats://"+natsRouteRef)
				if !ok || strings.ContainsAny(rest, "@$") {
					t.Errorf("route %q: want nats://%s<host>:<port>, the credentials only as the two references", u, natsRouteRef)
				}
				n++
			}
			if n != replicas {
				t.Errorf("NATS_ROUTES names %d routes for %d servers", n, replicas)
			}
			vars[name] = kubeletExpand(routes, vars)
			continue
		}
		key, ok := want[name]
		if !ok {
			continue
		}
		r, _ := field(e, "valueFrom", "secretKeyRef")
		if str(mustField(t, r, "name")) != secretOf[name] || str(mustField(t, r, "key")) != key {
			t.Errorf("nats container %s takes %v, want Secret %s key %s", name, r, secretOf[name], key)
		}
		if opt, _ := field(r, "optional"); opt == true {
			t.Errorf("nats container %s is optional; the account block needs it", name)
		}
		vars[name] = "fixture-" + strings.ToLower(strings.TrimPrefix(name, "MQTTNB_"))
		delete(want, name)
	}
	for name := range want {
		t.Errorf("the nats container does not set %s, which its configuration references", name)
	}
	if sawRoutes != (replicas > 1) {
		t.Errorf("NATS_ROUTES set %v with %d servers", sawRoutes, replicas)
	}
	return vars
}

// loadNATSChartConfig parses the NATS chart's rendered configuration the way its server
// will: with its pod's environment and the files it includes absent.
func loadNATSChartConfig(t *testing.T, conf string, vars map[string]string) *server.Options {
	t.Helper()
	for name, val := range vars {
		t.Setenv(name, val)
	}
	t.Setenv("SERVER_NAME", "nats-0")
	t.Setenv("POD_NAME", "nats-0")
	file := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(file, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(file)
	if err != nil {
		t.Fatalf("the NATS chart's rendered configuration does not parse: %v\n%s", err, conf)
	}
	return opts
}

// checkBrokerNATSOptions requires a NATS configuration the broker can run on: JetStream,
// no default consumer cap, and the default user (anonymous MQTT) denied the broker's
// internals both ways.
func checkBrokerNATSOptions(t *testing.T, opts *server.Options, replicas int) {
	t.Helper()
	// More than one server: the route listener is authenticated, and every route presents
	// the route user (an unauthenticated listener admits anyone who reaches it as a peer).
	if replicas > 1 {
		if opts.Cluster.Username != "fixture-nats_cluster_user" || opts.Cluster.Password != "fixture-nats_cluster_pass" {
			t.Errorf("cluster authorization user %q: want the route credentials from their Secret", opts.Cluster.Username)
		}
		if len(opts.Routes) != replicas {
			t.Errorf("%d routes for %d servers", len(opts.Routes), replicas)
		}
		for _, r := range opts.Routes {
			if r.User.Username() != "fixture-nats_cluster_user" {
				t.Errorf("route %s presents user %q, not the route user", r.Host, r.User.Username())
			}
		}
	} else if opts.Cluster.Port != 0 {
		t.Error("a single server renders a cluster listener")
	}
	if !opts.JetStream {
		t.Error("the NATS configuration does not enable JetStream")
	}
	if opts.JetStreamLimits.DefaultMaxConsumers != -1 {
		t.Errorf("default_max_consumers %d, want -1: the broker's streams carry one consumer per subscription", opts.JetStreamLimits.DefaultMaxConsumers)
	}
	internals := []string{"$MQTT.>", "$JS.>", "$KV.>", "$O.>"}
	var found bool
	for _, u := range opts.Users {
		if u.Username != "fixture-default_user" {
			continue
		}
		found = true
		if u.Account == nil || u.Account.Name != "MQTT" {
			t.Errorf("the default user is in account %v, want MQTT", u.Account)
		}
		for dir, p := range map[string]*server.SubjectPermission{"publish": permDir(u.Permissions, true), "subscribe": permDir(u.Permissions, false)} {
			var deny []string
			if p != nil {
				deny = p.Deny
			}
			for _, s := range internals {
				if !slices.Contains(deny, s) {
					t.Errorf("the default user may %s %s", dir, s)
				}
			}
		}
	}
	if !found {
		t.Error("no default user (MQTTNB_DEFAULT_USER) in the NATS configuration")
	}
	var service bool
	for _, u := range opts.Users {
		service = service || (u.Username == "fixture-nats_user" && u.Account != nil && u.Account.Name == "MQTT")
	}
	if !service {
		t.Error("no service user (MQTTNB_NATS_USER) in the account MQTT")
	}
}

func permDir(p *server.Permissions, publish bool) *server.SubjectPermission {
	if p == nil {
		return nil
	}
	if publish {
		return p.Publish
	}
	return p.Subscribe
}

// writeValues writes a values file for one test case.
func writeValues(t *testing.T, name, body string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), name+".yaml")
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

// TestHelmChartRefusals requires the chart to refuse at render time what the broker or
// the topology cannot run, naming the setting.
func TestHelmChartRefusals(t *testing.T) {
	bin := helmBin(t)
	for _, tc := range []struct{ name, values, want string }{
		{"UnknownTopology", "topology: sidecar\nbroker:\n  natsURLs: nats://nats:4222\n", "topology must be external or embedded"},
		{"NoNATS", "topology: external\n", "broker.natsURLs"},
		{"PlaintextWithoutTLS", "topology: external\nbroker:\n  natsURLs: nats://nats:4222\nmqtt:\n  tls:\n    plaintext: true\n", "mqtt.tls.plaintext"},
		{"TLSWithoutSecret", "topology: external\nbroker:\n  natsURLs: nats://nats:4222\nmqtt:\n  tls:\n    enabled: true\n", "mqtt.tls.secretName"},
		{"NoCredentials", "topology: external\nbroker:\n  natsURLs: nats://nats:4222\n  credentials:\n    secretName: \"\"\n", "broker.credentials.secretName"},
		{"EmbeddedAutoscaling", "topology: embedded\nreplicas: 1\nautoscaling:\n  enabled: true\n", "autoscaling"},
		{"EmbeddedWithDependency", "topology: embedded\nreplicas: 1\nnats:\n  enabled: true\n", "nats.enabled"},
		{"EmbeddedUnclusteredReplicas", "topology: embedded\nreplicas: 3\n", "replicas: an unclustered embedded server is one pod"},
		{"EmbeddedClusterEven", "topology: embedded\nreplicas: 4\nembedded:\n  cluster:\n    enabled: true\n", "odd count of at least 3"},
		{"EmbeddedClusterOne", "topology: embedded\nreplicas: 1\nembedded:\n  cluster:\n    enabled: true\n", "odd count of at least 3"},
		{"EmbeddedClusterNoRouteSecret", "topology: embedded\nreplicas: 3\nembedded:\n  cluster:\n    enabled: true\n    routeSecretName: \"\"\n", "embedded.cluster.routeSecretName"},
		{"DependencyServiceDisabled", "topology: external\nnats:\n  enabled: true\n  service:\n    enabled: false\n", "nats.service.enabled is false"},
		{"DependencyPortDisabled", "topology: external\nnats:\n  enabled: true\n  service:\n    ports:\n      nats:\n        enabled: false\n", "nats.service.ports.nats.enabled is false"},
		{"DependencyClusterUnauthenticated", "topology: external\nnats:\n  enabled: true\n  config:\n    cluster:\n      enabled: true\n", "routes are unauthenticated"},
		{"DependencySecretMismatch", "topology: external\nbroker:\n  credentials:\n    secretName: other\nnats:\n  enabled: true\n", "must come from broker.credentials.secretName"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := helmRender(bin, helmChartDir, writeValues(t, tc.name, tc.values))
			if err == nil {
				t.Fatal("the chart renders; want a refusal")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("refused with %v; want the message to name %q", err, tc.want)
			}
		})
	}
}

// TestHelmChartExtensionPoints shows that each merge, patch and extraResources hook lands
// where values.yaml documents it, that $tplYaml resolves before the merge and the patch,
// and that an append keeps every entry the chart renders.
func TestHelmChartExtensionPoints(t *testing.T) {
	bin := helmBin(t)
	tlsBase := "topology: external\nbroker:\n  natsURLs: nats://nats:4222\nmqtt:\n  tls:\n    enabled: true\n    secretName: mqtt-nats-broker-tls\n"
	base := helmTemplate(t, bin, helmChartDir, writeValues(t, "base", tlsBase))
	baseDep := findObject(t, base, "Deployment")
	baseC := mustMap(t, baseDep, "spec", "template", "spec", "containers", 0)

	objs := helmTemplate(t, bin, helmChartDir, writeValues(t, "hooks", tlsBase+`
podTemplate:
  merge:
    metadata:
      annotations:
        example.com/merged: "yes"
    spec:
      nodeSelector:
        kubernetes.io/os: linux
      initContainers:
        - name: init
          image: busybox
  patch:
    - op: add
      path: /spec/volumes/-
      value:
        name: extra
        emptyDir: {}
container:
  merge:
    stdin: false
  patch:
    - op: add
      path: /env/-
      value:
        name: GOMEMLIMIT
        value: 100MiB
    - op: add
      path: /volumeMounts/-
      value:
        name: extra
        mountPath: /extra
deployment:
  merge:
    spec:
      minReadySeconds: 5
service:
  merge:
    metadata:
      annotations:
        example.com/lb:
          $tplYaml: >
            {{ printf "%s-lb" .Release.Name | quote }}
extraResources:
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name:
        $tplYaml: >
          {{ include "mqtt-nats-broker.fullname" $ | quote }}
    data:
      k: v
`))
	dep := findObject(t, objs, "Deployment")
	pod := mustMap(t, dep, "spec", "template", "spec")
	c := mustMap(t, pod, "containers", 0)

	for _, tc := range []struct {
		what string
		keys []any
		want any
	}{
		{"podTemplate.merge annotation", []any{"spec", "template", "metadata", "annotations", "example.com/merged"}, "yes"},
		{"podTemplate.merge nodeSelector", []any{"spec", "template", "spec", "nodeSelector", "kubernetes.io/os"}, "linux"},
		{"podTemplate.merge initContainer", []any{"spec", "template", "spec", "initContainers", 0, "name"}, "init"},
		{"deployment.merge", []any{"spec", "minReadySeconds"}, 5},
		{"container.merge", []any{"spec", "template", "spec", "containers", 0, "stdin"}, false},
	} {
		if got, _ := field(dep, tc.keys...); got != tc.want {
			t.Errorf("%s: %v = %v, want %v", tc.what, tc.keys, got, tc.want)
		}
	}
	svc := findObject(t, objs, "Service")
	if got, _ := field(svc, "metadata", "annotations", "example.com/lb"); got != helmRelease+"-lb" {
		t.Errorf("service.merge with $tplYaml: annotation %v, want %s-lb", got, helmRelease)
	}
	var extra map[string]any
	for _, obj := range objs {
		if obj["kind"] == "ConfigMap" {
			extra = obj
		}
	}
	if extra == nil || mustField(t, extra, "metadata", "name") != helmRelease+"-mqtt-nats-broker" {
		t.Errorf("extraResources with $tplYaml: %v, want a ConfigMap named by the fullname", extra)
	}

	// An append keeps every entry the chart renders, in order, and adds the user's last.
	for _, list := range []struct {
		what      string
		before    []any
		after     []any
		appended  string
		nameField string
	}{
		{"container env", mustList(t, baseC, "env"), mustList(t, c, "env"), "GOMEMLIMIT", "name"},
		{"container volumeMounts", mustList(t, baseC, "volumeMounts"), mustList(t, c, "volumeMounts"), "extra", "name"},
		{"pod volumes", mustList(t, baseDep, "spec", "template", "spec", "volumes"), mustList(t, pod, "volumes"), "extra", "name"},
	} {
		if len(list.before) == 0 {
			t.Fatalf("%s: the chart renders none, so the append proves nothing", list.what)
		}
		if len(list.after) != len(list.before)+1 {
			t.Errorf("%s: %d entries after the append, want %d", list.what, len(list.after), len(list.before)+1)
			continue
		}
		for i, e := range list.before {
			if fmt.Sprint(e) != fmt.Sprint(list.after[i]) {
				t.Errorf("%s[%d]: the chart's %v became %v", list.what, i, e, list.after[i])
			}
		}
		if mustField(t, list.after[len(list.before)], list.nameField) != list.appended {
			t.Errorf("%s: the last entry is %v, want the appended %s", list.what, list.after[len(list.before)], list.appended)
		}
	}

	// The embedded topology's hooks: the StatefulSet and its claim.
	objs = helmTemplate(t, bin, helmChartDir, writeValues(t, "embedded-hooks", `
topology: embedded
replicas: 1
statefulSet:
  patch:
    - op: add
      path: /spec/minReadySeconds
      value: 7
embedded:
  storage:
    merge:
      spec:
        volumeMode: Filesystem
`))
	sts := findObject(t, objs, "StatefulSet")
	if got, _ := field(sts, "spec", "minReadySeconds"); got != 7 {
		t.Errorf("statefulSet.patch: minReadySeconds %v, want 7", got)
	}
	if got, _ := field(sts, "spec", "volumeClaimTemplates", 0, "spec", "volumeMode"); got != "Filesystem" {
		t.Errorf("embedded.storage.merge: volumeMode %v, want Filesystem", got)
	}
}

func findObject(t *testing.T, objs []map[string]any, kind string) map[string]any {
	t.Helper()
	var found map[string]any
	for _, obj := range objs {
		if obj["kind"] == kind {
			if found != nil {
				t.Fatalf("two %s objects", kind)
			}
			found = obj
		}
	}
	if found == nil {
		t.Fatalf("no %s", kind)
	}
	return found
}

// The topology-3 shutdown hook: wait for the broker process to leave the shared PID
// namespace, then lame duck.
const sharedPodPreStop = "while pgrep -f '^/usr/local/bin/mqtt-nats-broker( |$)' >/dev/null; do sleep 1; done; exec nats-server -sl=ldm=/var/run/nats/nats.pid"

// TestNATSChartSharedPod renders the official NATS chart with the topology-3 values,
// single and clustered, and judges what they ship: the broker container as the chart's
// own is judged, the shutdown order, the grace, and the NATS configuration and pod.
func TestNATSChartSharedPod(t *testing.T) {
	bin := helmBin(t)
	tgz, err := filepath.Glob(filepath.Join(helmChartDir, "charts", "nats-*.tgz"))
	if err != nil || len(tgz) != 1 {
		t.Fatalf("want the one NATS chart archive from helm dependency build, got %v", tgz)
	}
	for _, tc := range []struct {
		name     string
		files    []string
		replicas int
	}{
		{"single", []string{"values.yaml"}, 1},
		{"cluster", []string{"values.yaml", "values-cluster.yaml"}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var values []string
			for _, f := range tc.files {
				values = append(values, filepath.Join(sharedPodValues, f))
			}
			objs := helmTemplate(t, bin, tgz[0], values...)
			var sts, cm, client map[string]any
			for _, obj := range objs {
				switch str(obj["kind"]) {
				case "StatefulSet":
					sts = obj
				case "ConfigMap":
					cm = obj
				case "Service":
					if cip, _ := field(obj, "spec", "clusterIP"); cip != "None" {
						client = obj
					}
				case "PodDisruptionBudget":
				default:
					// Any other workload would need its own Restricted settings: NATS Box
					// is a separate Deployment that inherits none of them.
					t.Errorf("the NATS chart renders a %s %v", obj["kind"], mustField(t, obj, "metadata", "name"))
				}
			}
			if sts == nil || cm == nil || client == nil {
				t.Fatal("no StatefulSet, ConfigMap or client Service")
			}
			if r := mustField(t, sts, "spec", "replicas"); r != tc.replicas {
				t.Errorf("replicas %v, want %d", r, tc.replicas)
			}
			pod := mustMap(t, sts, "spec", "template", "spec")
			if pod["shareProcessNamespace"] != true {
				t.Error("shareProcessNamespace is not set: the nats container cannot see the broker exit")
			}
			var natsC, brokerC map[string]any
			for _, c := range mustList(t, pod, "containers") {
				cm := c.(map[string]any)
				uid := natsRuntimeUser
				if cm["name"] == "broker" {
					brokerC, uid = cm, runtimeUser
				}
				if cm["name"] == "nats" {
					natsC = cm
				}
				checkRestrictedContainer(t, "container "+str(cm["name"]), pod, cm, uid)
			}
			if brokerC == nil || natsC == nil {
				t.Fatal("want the nats and broker containers")
			}
			if cmd := anyStrings(mustField(t, natsC, "lifecycle", "preStop", "exec", "command")); !slices.Equal(cmd, []string{"/bin/sh", "-c", sharedPodPreStop}) {
				t.Errorf("nats preStop %q: want the wait for the broker's exit, then lame duck", cmd)
			}

			cfg := checkSharedPodBroker(t, brokerC, tc.replicas)
			var clientPort int
			for _, p := range mustList(t, client, "spec", "ports") {
				if mustField(t, p, "name") == "nats" {
					clientPort, _ = mustField(t, p, "port").(int)
				}
				if mustField(t, p, "name") == "mqtt" && (mustField(t, p, "targetPort") != "mqtt" || mustField(t, p, "port") != cfg.MQTTPort) {
					t.Errorf("Service mqtt port %v, want %d to the container's mqtt", p, cfg.MQTTPort)
				}
			}
			if cfg.NATSURLs != fmt.Sprintf("nats://127.0.0.1:%d", clientPort) {
				t.Errorf("MQTTNB_NATS_URLS %q, want the pod's own server on loopback, port %d", cfg.NATSURLs, clientPort)
			}

			// The grace covers the broker's whole shutdown, then lame duck, then the NATS
			// chart's stated overhead.
			conf := str(mustField(t, cm, "data", "nats.conf"))
			opts := loadNATSChartConfig(t, conf, checkNATSContainerCredentials(t, natsC, tc.replicas))
			checkBrokerNATSOptions(t, opts, tc.replicas)
			need := cfg.DrainDelay + chartShutdownBudget() + opts.LameDuckGracePeriod + opts.LameDuckDuration + 20*time.Second
			if secs, _ := mustField(t, pod, "terminationGracePeriodSeconds").(int); time.Duration(secs)*time.Second < need {
				t.Errorf("terminationGracePeriodSeconds %d < %s: the broker's shutdown, lame duck and the chart's overhead", secs, need)
			}
		})
	}
}

// checkSharedPodBroker judges the broker container the topology-3 values add with the
// chart's container checks: exactly the example's environment, the probes, ports and
// resources, and returns the configuration it runs with.
func checkSharedPodBroker(t *testing.T, c map[string]any, replicas int) *config.Config {
	t.Helper()
	keys := maps.Clone(chartContainerKeys)
	keys["securityContext"] = map[string]any{
		"runAsNonRoot": leaf, "runAsUser": leaf, "runAsGroup": leaf, "allowPrivilegeEscalation": leaf,
		"readOnlyRootFilesystem": leaf, "capabilities": map[string]any{"drop": leaf},
		"seccompProfile": map[string]any{"type": leaf},
	}
	delete(keys, "startupProbe")
	delete(keys, "volumeMounts")
	checkKeys(t, "broker container", c, keys)
	if g, _ := field(c, "securityContext", "runAsGroup"); g != runtimeUser {
		t.Errorf("broker runAsGroup %v, want %d", g, runtimeUser)
	}
	v := chartVariant{nats: true, replicas: replicas}
	if replicas > 1 {
		v.streamReplicas = replicas
	} else {
		v.streamReplicas = 1
	}
	env := checkChartEnvironment(t, v, c, chartObjects{}, "")
	ports := chartContainerPorts(t, c, v)
	if ports["mqtt"] != env.cfg.MQTTPort {
		t.Errorf("container port mqtt %d, broker listens on %d", ports["mqtt"], env.cfg.MQTTPort)
	}
	if _, p, _ := net.SplitHostPort(env.cfg.HTTPAddr); strconv.Itoa(ports["health"]) != p {
		t.Errorf("container port health %d, health server on %s", ports["health"], p)
	}
	checkProbes(t, c)
	for _, kind := range []string{"requests", "limits"} {
		for _, res := range []string{"cpu", "memory"} {
			if _, ok := field(c, "resources", kind, res); !ok {
				t.Errorf("resources.%s.%s is not set", kind, res)
			}
		}
	}
	return env.cfg
}
