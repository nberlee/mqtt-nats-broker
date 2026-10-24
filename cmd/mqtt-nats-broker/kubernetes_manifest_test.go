package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/config"
	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
	"github.com/nberlee/mqtt-nats-broker/internal/testcerts"
)

// The example deployment is judged against the binary that will run it: its settings are
// evaluated by config.Load, its probes are served by the health handler, its TLS files
// are loaded by resolveTLS, and its termination grace is summed from the shutdown
// budgets in the source. A flag rename, a route rename or a raised budget fails here
// rather than in somebody's cluster.
//
// The contract is this inventory, and nothing wider:
//   - the fields: none outside the set the example sets (checkKeys);
//   - the environment: exactly the example's variable names, each one internal/config
//     reads, accepted by config.Load, credentials from their Secret keys;
//   - the listeners: MQTT and health bound to every address, on exactly those two
//     distinct container ports, and only MQTT behind the Service;
//   - the probes: served by healthMux, on the health port;
//   - the grace: the drain delay plus the shutdown budgets plus headroom;
//   - the stated security defaults: the Restricted fields, no token, a tagged image;
//   - TLS: the Secret's two keys under a read-only mount, loadable by resolveTLS;
//   - resources: requests and limits PRESENT, values not judged;
//   - the kustomize structure, and the same checks on the real render.
//
// It does not show that the API server admits the manifest (a request above its limit, a
// malformed quantity): that is admission, which only a cluster install verifies.

const (
	kubernetesManifestDir = "../../docs/deploy/kubernetes"

	// runtimeUser is the numeric identity the image declares (Dockerfile, USER).
	runtimeUser = 65532

	// graceHeadroom is what the grace keeps beyond the named shutdown budgets.
	graceHeadroom = 15 * time.Second

	// The Secrets the example references and the TLS keys a kubernetes.io/tls Secret
	// guarantees.
	natsSecretName = "mqtt-nats-broker-nats"
	tlsSecretName  = "mqtt-nats-broker-tls"
	tlsCertKey     = "tls.crt"
	tlsKeyKey      = "tls.key"
)

// TestKubernetesManifest checks the example tree statically: every file, the base, and
// the TLS component applied to the base. The composed output of the real kustomize is
// TestKubernetesManifestRender's.
func TestKubernetesManifest(t *testing.T) {
	root := manifestRoot(t)

	t.Run("BlockStyle", func(t *testing.T) {
		for _, file := range manifestFiles(t, root) {
			for _, doc := range decodeYAMLNodes(t, file) {
				if line, ok := flowNode(doc); ok {
					t.Errorf("%s:%d: flow-style YAML; the example is block style", rel(root, file), line)
				}
			}
		}
	})

	t.Run("Kustomizations", func(t *testing.T) {
		checkKustomizations(t, root)
	})

	dep, svc := baseWorkload(t, root)

	t.Run("Base", func(t *testing.T) {
		checkBrokerWorkload(t, dep, svc, false)
	})

	t.Run("TLSComponent", func(t *testing.T) {
		tlsDep := applyPatchFile(t, dep, filepath.Join(root, "components/tls/deployment-patch.yaml"))
		tlsSvc := applyPatchFile(t, svc, filepath.Join(root, "components/tls/service-patch.yaml"))
		checkBrokerWorkload(t, tlsDep, tlsSvc, true)
	})
}

// TestKubernetesManifestRender renders the base and the TLS overlay with the kustomize
// binary MQTTNB_KUSTOMIZE names (make kubernetes-render installs the pinned version)
// and checks what comes out. Unset, it skips; set, every failure to render fails.
func TestKubernetesManifestRender(t *testing.T) {
	bin := os.Getenv("MQTTNB_KUSTOMIZE")
	if bin == "" {
		t.Skip("MQTTNB_KUSTOMIZE not set; run make kubernetes-render")
	}
	root := manifestRoot(t)
	for _, tc := range []struct {
		dir     string
		wantTLS bool
	}{
		{"base", false},
		{"overlays/tls", true},
	} {
		t.Run(tc.dir, func(t *testing.T) {
			dep, svc := renderWorkload(t, bin, filepath.Join(root, tc.dir))
			checkBrokerWorkload(t, dep, svc, tc.wantTLS)
		})
	}
}

func manifestRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(kubernetesManifestDir)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func rel(root, file string) string {
	if r, err := filepath.Rel(root, file); err == nil {
		return r
	}
	return file
}

// manifestFiles lists every YAML file under root.
func manifestFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && (strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml")) {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no YAML under %s", root)
	}
	return files
}

// decodeYAMLNodes decodes every document of a YAML stream.
func decodeYAMLNodes(t *testing.T, file string) []*yaml.Node {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return decodeYAMLStream(t, file, data)
}

func decodeYAMLStream(t *testing.T, name string, data []byte) []*yaml.Node {
	t.Helper()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var docs []*yaml.Node
	for {
		var n yaml.Node
		err := dec.Decode(&n)
		if errors.Is(err, io.EOF) {
			return docs
		}
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		docs = append(docs, &n)
	}
}

// flowNode reports the line of the first flow-style mapping or sequence under n.
func flowNode(n *yaml.Node) (int, bool) {
	if n.Style&yaml.FlowStyle != 0 && (n.Kind == yaml.MappingNode || n.Kind == yaml.SequenceNode) {
		return n.Line, true
	}
	for _, c := range n.Content {
		if line, ok := flowNode(c); ok {
			return line, true
		}
	}
	return 0, false
}

// decodeObjects decodes every document of a file into generic values.
func decodeObjects(t *testing.T, file string) []map[string]any {
	t.Helper()
	var objs []map[string]any
	for _, doc := range decodeYAMLNodes(t, file) {
		var obj map[string]any
		if err := doc.Decode(&obj); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		objs = append(objs, obj)
	}
	return objs
}

// field walks a path of map keys and list indexes; ok is false where it leaves the tree.
func field(v any, keys ...any) (any, bool) {
	for _, k := range keys {
		switch k := k.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil, false
			}
			if v, ok = m[k]; !ok {
				return nil, false
			}
		case int:
			l, ok := v.([]any)
			if !ok || k < 0 || k >= len(l) {
				return nil, false
			}
			v = l[k]
		}
	}
	return v, true
}

func mustField(t *testing.T, v any, keys ...any) any {
	t.Helper()
	f, ok := field(v, keys...)
	if !ok {
		t.Fatalf("missing %v", keys)
	}
	return f
}

func mustMap(t *testing.T, v any, keys ...any) map[string]any {
	t.Helper()
	m, ok := mustField(t, v, keys...).(map[string]any)
	if !ok {
		t.Fatalf("%v is not a mapping", keys)
	}
	return m
}

func mustList(t *testing.T, v any, keys ...any) []any {
	t.Helper()
	l, ok := mustField(t, v, keys...).([]any)
	if !ok {
		t.Fatalf("%v is not a sequence", keys)
	}
	return l
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// kustomization is the part of a kustomization file the example uses.
type kustomization struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Resources  []string `yaml:"resources"`
	Components []string `yaml:"components"`
	Patches    []struct {
		Path   string `yaml:"path"`
		Target struct {
			Group   string `yaml:"group"`
			Version string `yaml:"version"`
			Kind    string `yaml:"kind"`
			Name    string `yaml:"name"`
		} `yaml:"target"`
	} `yaml:"patches"`
}

func loadKustomization(t *testing.T, dir string) kustomization {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var k kustomization
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&k); err != nil {
		t.Fatalf("%s: %v", dir, err)
	}
	return k
}

// checkKustomizations requires the three entry points to be well formed, every reference
// to resolve, every YAML file to be referenced, and every patch target to name a base
// resource by group, version, kind and name.
func checkKustomizations(t *testing.T, root string) {
	referenced := map[string]bool{}
	want := map[string]struct{ apiVersion, kind string }{
		"base":           {"kustomize.config.k8s.io/v1beta1", "Kustomization"},
		"components/tls": {"kustomize.config.k8s.io/v1alpha1", "Component"},
		"overlays/tls":   {"kustomize.config.k8s.io/v1beta1", "Kustomization"},
	}
	for _, file := range manifestFiles(t, root) {
		if filepath.Base(file) != "kustomization.yaml" {
			continue
		}
		dir := filepath.Dir(file)
		name := filepath.ToSlash(rel(root, dir))
		w, ok := want[name]
		if !ok {
			t.Errorf("%s: a kustomization the example does not describe", name)
			continue
		}
		delete(want, name)
		referenced[file] = true
		k := loadKustomization(t, dir)
		if k.APIVersion != w.apiVersion || k.Kind != w.kind {
			t.Errorf("%s: %s %s, want %s %s", name, k.APIVersion, k.Kind, w.apiVersion, w.kind)
		}
		for _, r := range k.Resources {
			p := filepath.Join(dir, r)
			if st, err := os.Stat(p); err != nil {
				t.Errorf("%s: resource %s: %v", name, r, err)
			} else if !st.IsDir() {
				referenced[p] = true
			} else if _, err := os.Stat(filepath.Join(p, "kustomization.yaml")); err != nil {
				t.Errorf("%s: resource directory %s has no kustomization", name, r)
			}
		}
		for _, c := range k.Components {
			if _, err := os.Stat(filepath.Join(dir, c, "kustomization.yaml")); err != nil {
				t.Errorf("%s: component %s: %v", name, c, err)
			}
		}
		for _, p := range k.Patches {
			file := filepath.Join(dir, p.Path)
			if _, err := os.Stat(file); err != nil {
				t.Errorf("%s: patch %s: %v", name, p.Path, err)
			}
			referenced[file] = true
			if !baseHasResource(t, root, p.Target.Group, p.Target.Version, p.Target.Kind, p.Target.Name) {
				t.Errorf("%s: patch %s targets %s/%s %s %q, which is no base resource",
					name, p.Path, p.Target.Group, p.Target.Version, p.Target.Kind, p.Target.Name)
			}
		}
		if name == "overlays/tls" {
			if !slices.Equal(k.Resources, []string{"../../base"}) || !slices.Equal(k.Components, []string{"../../components/tls"}) {
				t.Errorf("overlays/tls: resources %v components %v, want the base and the tls component", k.Resources, k.Components)
			}
		}
	}
	for name := range want {
		t.Errorf("%s/kustomization.yaml is missing", name)
	}
	for _, file := range manifestFiles(t, root) {
		if !referenced[file] {
			t.Errorf("%s is referenced by no kustomization", rel(root, file))
		}
	}
}

// baseHasResource reports whether a base resource has exactly this group, version, kind
// and name; an empty group is the core group.
func baseHasResource(t *testing.T, root, group, version, kind, name string) bool {
	t.Helper()
	gv := version
	if group != "" {
		gv = group + "/" + version
	}
	for _, r := range loadKustomization(t, filepath.Join(root, "base")).Resources {
		for _, obj := range decodeObjects(t, filepath.Join(root, "base", r)) {
			if version != "" && str(obj["apiVersion"]) == gv && str(obj["kind"]) == kind &&
				str(mustField(t, obj, "metadata", "name")) == name {
				return true
			}
		}
	}
	return false
}

// baseWorkload returns the base's Deployment and Service.
func baseWorkload(t *testing.T, root string) (dep, svc map[string]any) {
	t.Helper()
	var objs []map[string]any
	for _, r := range loadKustomization(t, filepath.Join(root, "base")).Resources {
		objs = append(objs, decodeObjects(t, filepath.Join(root, "base", r))...)
	}
	return workload(t, objs)
}

// workload requires exactly one Deployment and one Service and nothing else.
func workload(t *testing.T, objs []map[string]any) (dep, svc map[string]any) {
	t.Helper()
	for _, obj := range objs {
		switch kind := str(obj["kind"]); {
		case kind == "Deployment" && dep == nil:
			dep = obj
		case kind == "Service" && svc == nil:
			svc = obj
		default:
			t.Fatalf("unexpected %s %v", kind, obj["metadata"])
		}
	}
	if dep == nil || svc == nil {
		t.Fatalf("want one Deployment and one Service, got %d objects", len(objs))
	}
	return dep, svc
}

func renderWorkload(t *testing.T, bin, dir string) (dep, svc map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "build", dir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s build %s: %v\n%s", bin, dir, err, stderr.String())
	}
	var objs []map[string]any
	for _, doc := range decodeYAMLStream(t, dir, stdout.Bytes()) {
		var obj map[string]any
		if err := doc.Decode(&obj); err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		objs = append(objs, obj)
	}
	return workload(t, objs)
}

// applyPatchFile applies a JSON6902 patch of add and replace operations to a copy of
// obj. It is stricter than RFC 6902 where that protects the base: a replace must name an
// existing value, and an add may not overwrite an existing mapping key.
func applyPatchFile(t *testing.T, obj map[string]any, file string) map[string]any {
	t.Helper()
	var ops []struct {
		Op    string `yaml:"op"`
		Path  string `yaml:"path"`
		Value any    `yaml:"value"`
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(data, &ops); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	out := deepCopy(obj).(map[string]any)
	for _, op := range ops {
		tokens := strings.Split(strings.TrimPrefix(op.Path, "/"), "/")
		parent, ok := field(out, pointerKeys(tokens[:len(tokens)-1])...)
		if !ok || !strings.HasPrefix(op.Path, "/") {
			t.Fatalf("%s: %s %s: the parent does not exist in the base", file, op.Op, op.Path)
		}
		last := tokens[len(tokens)-1]
		switch p := parent.(type) {
		case map[string]any:
			_, exists := p[last]
			if op.Op == "replace" && !exists || op.Op == "add" && exists || op.Op != "add" && op.Op != "replace" {
				t.Fatalf("%s: %s %s: key present=%v", file, op.Op, op.Path, exists)
			}
			p[last] = deepCopy(op.Value)
		case []any:
			idx, err := strconv.Atoi(last)
			switch {
			case op.Op == "add" && last == "-":
				if !setField(out, pointerKeys(tokens[:len(tokens)-1]), append(p, deepCopy(op.Value))) {
					t.Fatalf("%s: %s %s", file, op.Op, op.Path)
				}
			case op.Op == "replace" && err == nil && idx >= 0 && idx < len(p):
				p[idx] = deepCopy(op.Value)
			default:
				t.Fatalf("%s: %s %s: unsupported on a sequence", file, op.Op, op.Path)
			}
		default:
			t.Fatalf("%s: %s %s: the parent is a scalar", file, op.Op, op.Path)
		}
	}
	return out
}

// pointerKeys turns JSON pointer tokens into field keys: numeric tokens index sequences.
func pointerKeys(tokens []string) []any {
	keys := make([]any, len(tokens))
	for i, tok := range tokens {
		if n, err := strconv.Atoi(tok); err == nil {
			keys[i] = n
		} else {
			keys[i] = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		}
	}
	return keys
}

// setField replaces the value at keys, which must exist.
func setField(root any, keys []any, v any) bool {
	parent, ok := field(root, keys[:len(keys)-1]...)
	if !ok {
		return false
	}
	switch k := keys[len(keys)-1].(type) {
	case string:
		m, ok := parent.(map[string]any)
		if ok {
			m[k] = v
		}
		return ok
	case int:
		l, ok := parent.([]any)
		if ok && k >= 0 && k < len(l) {
			l[k] = v
			return true
		}
	}
	return false
}

func deepCopy(v any) any {
	switch v := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(v))
		for k, e := range v {
			m[k] = deepCopy(e)
		}
		return m
	case []any:
		l := make([]any, len(v))
		for i, e := range v {
			l[i] = deepCopy(e)
		}
		return l
	}
	return v
}

// checkBrokerWorkload judges a Deployment and Service against the binary. wantTLS says
// whether the MQTT listener must be TLS from the mounted Secret.
func checkBrokerWorkload(t *testing.T, dep, svc map[string]any, wantTLS bool) {
	t.Helper()
	pod := mustMap(t, dep, "spec", "template", "spec")
	containers := mustList(t, pod, "containers")
	if len(containers) != 1 || str(mustField(t, containers[0], "name")) != "broker" {
		t.Fatalf("want the single container broker, got %d containers", len(containers))
	}
	c := mustMap(t, containers, 0)

	checkKeys(t, "Deployment", dep, deploymentKeys)
	checkKeys(t, "Service", svc, serviceKeys)
	checkSecurity(t, pod, c)

	image := str(c["image"])
	if i := strings.LastIndex(image, ":"); i < 0 || strings.Contains(image[i:], "/") || image[i+1:] == "latest" {
		t.Errorf("image %q: name a release tag or digest, not latest or nothing", image)
	}

	cfg := checkEnvironment(t, c, wantTLS)
	ports := containerPorts(t, c)
	if ports["mqtt"] != cfg.MQTTPort {
		t.Errorf("container port mqtt %d, but the broker listens on %d", ports["mqtt"], cfg.MQTTPort)
	}
	httpHost, httpPort, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		t.Fatalf("HTTPAddr %q: %v", cfg.HTTPAddr, err)
	}
	// The kubelet probes the pod IP and the Service forwards to it, so a listener bound to
	// one address (loopback, say) is unreachable however its port matches.
	for what, host := range map[string]string{"MQTT": cfg.MQTTHost, "health": httpHost} {
		if ip := net.ParseIP(host); host != "" && (ip == nil || !ip.IsUnspecified()) {
			t.Errorf("the %s listener binds %q; the example needs every address", what, host)
		}
	}
	if strconv.Itoa(ports["health"]) != httpPort {
		t.Errorf("container port health %d, but the health server listens on %s", ports["health"], httpPort)
	}

	checkProbes(t, c)
	checkGrace(t, pod, cfg)
	checkService(t, dep, svc, cfg)
	checkTLS(t, pod, c, cfg, wantTLS)
}

// leaf marks a field whose value the checks below judge, or which carries no setting
// that could contradict them.
var leaf = struct{}{}

// The fields the example may set, and nothing else. Each check below judges a value the
// binary or the cluster will act on, and a field it does not know can override that value
// without changing it: container arguments win over the environment the test evaluates, a
// container securityContext overrides the pod's, a probe scheme decides the transport, a
// Secret volume mode decides who can read the files. So the class is closed by refusing
// every unlisted field rather than by listing the dangerous ones.
var (
	securityKeys = map[string]any{
		"runAsNonRoot": leaf, "runAsUser": leaf, "runAsGroup": leaf,
		"seccompProfile": map[string]any{"type": leaf},
	}
	probeKeys = map[string]any{
		"httpGet":       map[string]any{"path": leaf, "port": leaf},
		"periodSeconds": leaf, "failureThreshold": leaf,
	}
	containerKeys = map[string]any{
		"name": leaf, "image": leaf,
		"env": []any{map[string]any{
			"name": leaf, "value": leaf,
			"valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": leaf, "key": leaf, "optional": leaf}},
		}},
		"ports":          []any{map[string]any{"name": leaf, "containerPort": leaf}},
		"readinessProbe": probeKeys,
		"livenessProbe":  probeKeys,
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
	labelKeys      = map[string]any{"app.kubernetes.io/name": leaf}
	deploymentKeys = map[string]any{
		"apiVersion": leaf, "kind": leaf,
		"metadata": map[string]any{"name": leaf, "labels": labelKeys},
		"spec": map[string]any{
			"replicas": leaf,
			"selector": map[string]any{"matchLabels": labelKeys},
			"template": map[string]any{
				"metadata": map[string]any{"labels": labelKeys},
				"spec": map[string]any{
					"automountServiceAccountToken": leaf, "terminationGracePeriodSeconds": leaf,
					"securityContext": securityKeys,
					"containers":      []any{containerKeys},
					"volumes":         []any{map[string]any{"name": leaf, "secret": map[string]any{"secretName": leaf}}},
				},
			},
		},
	}
	serviceKeys = map[string]any{
		"apiVersion": leaf, "kind": leaf,
		"metadata": map[string]any{"name": leaf, "labels": labelKeys},
		"spec": map[string]any{
			"type": leaf, "selector": labelKeys,
			"ports": []any{map[string]any{"name": leaf, "port": leaf, "targetPort": leaf}},
		},
	}
)

// checkKeys refuses any field of v the schema does not list. A schema is leaf, a mapping
// of allowed keys to their schemas, or a one-element sequence holding every item's schema.
func checkKeys(t *testing.T, where string, v any, schema any) {
	t.Helper()
	switch sc := schema.(type) {
	case map[string]any:
		m, ok := v.(map[string]any)
		if !ok {
			t.Errorf("%s: want a mapping, got %T", where, v)
			return
		}
		for k, e := range m {
			sub, allowed := sc[k]
			if !allowed {
				t.Errorf("%s.%s: a field the example does not set, which the checks cannot judge", where, k)
				continue
			}
			checkKeys(t, where+"."+k, e, sub)
		}
	case []any:
		l, ok := v.([]any)
		if !ok {
			t.Errorf("%s: want a sequence, got %T", where, v)
			return
		}
		for i, e := range l {
			checkKeys(t, fmt.Sprintf("%s[%d]", where, i), e, sc[0])
		}
	}
}

// checkSecurity requires what the Restricted Pod Security standard asks of this pod, the
// image's numeric identity, no service-account token, and requests and limits.
func checkSecurity(t *testing.T, pod, c map[string]any) {
	t.Helper()
	sc := mustMap(t, pod, "securityContext")
	for key, want := range map[string]any{"runAsNonRoot": true, "runAsUser": runtimeUser, "runAsGroup": runtimeUser} {
		if sc[key] != want {
			t.Errorf("pod securityContext.%s = %v, want %v", key, sc[key], want)
		}
	}
	if v, _ := field(sc, "seccompProfile", "type"); v != "RuntimeDefault" {
		t.Errorf("pod seccompProfile.type = %v, want RuntimeDefault", v)
	}
	if pod["automountServiceAccountToken"] != false {
		t.Errorf("automountServiceAccountToken = %v, want false", pod["automountServiceAccountToken"])
	}
	csc := mustMap(t, c, "securityContext")
	if csc["allowPrivilegeEscalation"] != false || csc["readOnlyRootFilesystem"] != true {
		t.Errorf("container securityContext %v: want allowPrivilegeEscalation false, readOnlyRootFilesystem true", csc)
	}
	if drop, _ := field(csc, "capabilities", "drop"); !slices.Equal(anyStrings(drop), []string{"ALL"}) {
		t.Errorf("capabilities.drop = %v, want [ALL]", drop)
	}
	for _, kind := range []string{"requests", "limits"} {
		for _, res := range []string{"cpu", "memory"} {
			if _, ok := field(c, "resources", kind, res); !ok {
				t.Errorf("resources.%s.%s is not set", kind, res)
			}
		}
	}
}

func anyStrings(v any) []string {
	l, _ := v.([]any)
	out := make([]string, 0, len(l))
	for _, e := range l {
		out = append(out, str(e))
	}
	return out
}

// The variables the example sets, and with TLS the ones the component adds. Like the
// fields, the set is closed: another variable is a setting the checks may not judge.
var (
	exampleEnv = []string{
		"MQTTNB_NATS_URLS", "MQTTNB_NATS_USER", "MQTTNB_NATS_PASS", "MQTTNB_DEFAULT_USER",
		"MQTTNB_DEFAULT_PASS", "MQTTNB_LOG_LEVEL", "MQTTNB_DRAIN_DELAY",
	}
	exampleTLSEnv = []string{"MQTTNB_PORT", "MQTTNB_TLS_CERT", "MQTTNB_TLS_KEY"}
)

// checkEnvironment requires exactly the example's variables, each one the binary reads,
// the credentials to come from the expected Secret keys, and returns the configuration
// the binary derives from the environment.
func checkEnvironment(t *testing.T, c map[string]any, wantTLS bool) *config.Config {
	t.Helper()
	known := configEnvNames(t)
	allowed := slices.Clone(exampleEnv)
	if wantTLS {
		allowed = append(allowed, exampleTLSEnv...)
	}
	wantRefs := map[string]struct {
		key      string
		optional bool
	}{
		"MQTTNB_NATS_USER":    {"user", false},
		"MQTTNB_NATS_PASS":    {"password", false},
		"MQTTNB_DEFAULT_USER": {"default-user", true},
		"MQTTNB_DEFAULT_PASS": {"default-password", true},
	}
	values := map[string]string{}
	for _, e := range mustList(t, c, "env") {
		name := str(mustField(t, e, "name"))
		if !known[name] {
			t.Errorf("env %s is not a variable internal/config reads", name)
		}
		if !slices.Contains(allowed, name) {
			t.Errorf("env %s is not one the example sets, so the checks cannot judge it", name)
		}
		if _, dup := values[name]; dup {
			t.Errorf("env %s is set twice", name)
		}
		if ref, ok := field(e, "valueFrom", "secretKeyRef"); ok {
			want, expected := wantRefs[name]
			delete(wantRefs, name)
			opt, _ := field(ref, "optional")
			if !expected || str(mustField(t, ref, "name")) != natsSecretName ||
				str(mustField(t, ref, "key")) != want.key || (opt == true) != want.optional {
				t.Errorf("env %s takes %v; want Secret %s key %q optional=%v", name, ref, natsSecretName, want.key, want.optional)
			}
			values[name] = "placeholder"
			continue
		}
		if _, isRef := wantRefs[name]; isRef {
			t.Errorf("env %s is a credential and must come from a Secret", name)
		}
		v, ok := field(e, "value")
		if !ok {
			t.Errorf("env %s has neither a value nor a Secret reference", name)
		}
		// The kubelet rewrites a value before the binary sees it wherever a $ appears
		// ($(VAR) is replaced, $$ becomes $), so the string evaluated here would not be the
		// one the broker runs with. The example's values carry no $ at all.
		if strings.Contains(str(v), "$") {
			t.Errorf("env %s = %q: the kubelet would rewrite it; the example sets literal values", name, str(v))
		}
		// @ delimits userinfo in every URL form, so a literal holding one can hold a
		// credential; none of the example's values needs it.
		if strings.Contains(str(v), "@") {
			t.Errorf("env %s = %q: a literal value may not carry userinfo; credentials come from the Secret", name, str(v))
		}
		values[name] = str(v)
	}
	for name := range wantRefs {
		t.Errorf("env %s is not set from its Secret", name)
	}
	for _, name := range allowed {
		if _, ok := values[name]; !ok {
			t.Errorf("env %s, which the example sets, is missing", name)
		}
	}

	for _, kv := range os.Environ() {
		if name, _, _ := strings.Cut(kv, "="); strings.HasPrefix(name, "MQTTNB_") {
			t.Setenv(name, "")
			_ = os.Unsetenv(name)
		}
	}
	for name, v := range values {
		t.Setenv(name, v)
	}
	cfg, err := config.Load(nil)
	if err != nil {
		t.Fatalf("the binary refuses the manifest's environment: %v", err)
	}
	// A credential in a URL is refused above as an @ in a literal. The scheme is required
	// too: nats.go reads a URL without "://" as nats://, so the example names it.
	for _, raw := range strings.Split(cfg.NATSURLs, ",") {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || (u.Scheme != "nats" && u.Scheme != "tls") || !strings.Contains(raw, "://") {
			t.Errorf("NATS URL %q: want an explicit nats:// or tls:// URL", raw)
		}
	}
	return cfg
}

// configEnvNames returns the MQTTNB_ string literals in internal/config/config.go: the
// variables the binary reads, from the source rather than from a second list.
func configEnvNames(t *testing.T) map[string]bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "../../internal/config/config.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if s, err := strconv.Unquote(lit.Value); err == nil && strings.HasPrefix(s, "MQTTNB_") {
				names[s] = true
			}
		}
		return true
	})
	if len(names) == 0 {
		t.Fatal("no MQTTNB_ literals in internal/config/config.go")
	}
	return names
}

// containerPorts requires exactly the two listeners, mqtt and health, and returns their
// numbers. Like the fields, the set is closed: a third name could alias a listener (the
// health port under another name) past the Service check.
func containerPorts(t *testing.T, c map[string]any) map[string]int {
	t.Helper()
	ports := map[string]int{}
	for _, p := range mustList(t, c, "ports") {
		n, _ := mustField(t, p, "containerPort").(int)
		ports[str(mustField(t, p, "name"))] = n
	}
	if len(ports) != 2 || ports["mqtt"] == 0 || ports["health"] == 0 || len(mustList(t, c, "ports")) != 2 {
		t.Errorf("container ports %v: want exactly mqtt and health", ports)
	}
	if ports["mqtt"] == ports["health"] {
		t.Errorf("mqtt and health share port %d: both listeners bind every address, so one cannot start", ports["mqtt"])
	}
	return ports
}

// checkProbes serves the manifest's probe paths through the broker's own health handler,
// not ready yet (the state before the listener is up and while draining): liveness must
// answer 200, readiness 503, and a path the handler does not serve 404.
func checkProbes(t *testing.T, c map[string]any) {
	t.Helper()
	mux := healthMux(nil, new(atomic.Bool))
	get := func(p string) (int, string) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		return rec.Code, rec.Body.String()
	}
	if code, _ := get("/not-a-probe"); code != http.StatusNotFound {
		t.Fatalf("the health handler answers %d for an unregistered path; the probe checks below would prove nothing", code)
	}
	for _, probe := range []struct {
		key  string
		code int
	}{
		{"livenessProbe", http.StatusOK},
		{"readinessProbe", http.StatusServiceUnavailable},
	} {
		p := str(mustField(t, c, probe.key, "httpGet", "path"))
		if port := mustField(t, c, probe.key, "httpGet", "port"); port != "health" {
			t.Errorf("%s port %v, want health", probe.key, port)
		}
		if code, body := get(p); code != probe.code {
			t.Errorf("%s %s: the health handler answers %d %q, want %d", probe.key, p, code, body, probe.code)
		}
	}
	if v := mustField(t, c, "readinessProbe", "failureThreshold"); v != 1 {
		t.Errorf("readinessProbe.failureThreshold = %v, want 1, so one failed probe withdraws the endpoint", v)
	}
}

// checkGrace requires the termination grace to cover the drain delay and every named
// shutdown budget the binary runs after it, read from the source.
func checkGrace(t *testing.T, pod map[string]any, cfg *config.Config) {
	t.Helper()
	secs, ok := mustField(t, pod, "terminationGracePeriodSeconds").(int)
	if !ok {
		t.Fatal("terminationGracePeriodSeconds is not an integer")
	}
	need := cfg.DrainDelay + mqtt.ShutdownDrainTimeout + httpShutdownTimeout + backend.ServiceCloseBudget + graceHeadroom
	if got := time.Duration(secs) * time.Second; got < need {
		t.Errorf("terminationGracePeriodSeconds %s < %s: drain delay %s + client drain %s + health server %s + NATS close %s + headroom %s",
			got, need, cfg.DrainDelay, mqtt.ShutdownDrainTimeout, httpShutdownTimeout, backend.ServiceCloseBudget, graceHeadroom)
	}
}

// checkService requires the Service to select exactly this Deployment's pods and to
// expose the MQTT listener and nothing else.
func checkService(t *testing.T, dep, svc map[string]any, cfg *config.Config) {
	t.Helper()
	labels := mustMap(t, dep, "spec", "template", "metadata", "labels")
	selector := mustMap(t, svc, "spec", "selector")
	if fmt.Sprint(selector) != fmt.Sprint(mustMap(t, dep, "spec", "selector", "matchLabels")) {
		t.Errorf("Service selector %v differs from the Deployment selector", selector)
	}
	for k, v := range selector {
		if labels[k] != v {
			t.Errorf("Service selector %s=%v does not match the pod labels %v", k, v, labels)
		}
	}
	// Exactly one port, the MQTT listener: the health listener is never exposed.
	svcPorts := mustList(t, svc, "spec", "ports")
	if len(svcPorts) != 1 || str(mustField(t, svcPorts[0], "targetPort")) != "mqtt" {
		t.Fatalf("Service ports %v: want exactly one, targeting mqtt", svcPorts)
	}
	if port, _ := mustField(t, svcPorts[0], "port").(int); port != cfg.MQTTPort {
		t.Errorf("Service port %d for the MQTT listener on %d", port, cfg.MQTTPort)
	}
}

// checkTLS requires, with TLS, the certificate and key to be the two keys of the mounted
// kubernetes.io/tls Secret, the mount to be a read-only directory (so the kubelet's
// updates reach the reload), the Secret to be required, and the files to load; without
// TLS, no listener TLS and no volume.
func checkTLS(t *testing.T, pod, c map[string]any, cfg *config.Config, wantTLS bool) {
	t.Helper()
	if !wantTLS {
		if cfg.TLSEnabled() {
			t.Error("the plaintext workload configures listener TLS")
		}
		if _, ok := pod["volumes"]; ok {
			t.Error("the plaintext workload mounts volumes")
		}
		return
	}
	if !cfg.TLSEnabled() || cfg.TLSCA != "" {
		t.Fatalf("listener TLS: cert %q key %q ca %q, want a certificate and key and no client CA", cfg.TLSCert, cfg.TLSKey, cfg.TLSCA)
	}
	mounts := mustList(t, c, "volumeMounts")
	volumes := mustList(t, pod, "volumes")
	if len(mounts) != 1 || len(volumes) != 1 {
		t.Fatalf("want one TLS mount and volume, got %d and %d", len(mounts), len(volumes))
	}
	mount, vol := mustMap(t, mounts, 0), mustMap(t, volumes, 0)
	dir := str(mount["mountPath"])
	// checkKeys has refused subPath, optional, items and defaultMode already.
	if mount["readOnly"] != true {
		t.Errorf("TLS mount %v: want read-only", mount)
	}
	if str(mount["name"]) != str(vol["name"]) {
		t.Errorf("the mount names volume %v, the volume is %v", mount["name"], vol["name"])
	}
	secret := mustMap(t, vol, "secret")
	if str(secret["secretName"]) != tlsSecretName {
		t.Errorf("TLS volume %v: want the Secret %s", secret, tlsSecretName)
	}
	if cfg.TLSCert != path.Join(dir, tlsCertKey) || cfg.TLSKey != path.Join(dir, tlsKeyKey) {
		t.Fatalf("listener TLS files %s, %s: want %s and %s under the mount %s", cfg.TLSCert, cfg.TLSKey, tlsCertKey, tlsKeyKey, dir)
	}

	ca, err := testcerts.NewAuthority("kubernetes manifest test CA")
	if err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := ca.Issue("mqtt-nats-broker", "mqtt-nats-broker")
	if err != nil {
		t.Fatal(err)
	}
	projected := t.TempDir()
	for key, data := range map[string][]byte{tlsCertKey: certPEM, tlsKeyKey: keyPEM} {
		if _, err := testcerts.Write(projected, key, data); err != nil {
			t.Fatal(err)
		}
	}
	local := *cfg
	local.TLSCert = filepath.Join(projected, strings.TrimPrefix(cfg.TLSCert, dir))
	local.TLSKey = filepath.Join(projected, strings.TrimPrefix(cfg.TLSKey, dir))
	if _, err := resolveTLS(&local, slog.New(slog.DiscardHandler)); err != nil {
		t.Errorf("the broker cannot load a kubernetes.io/tls Secret from the configured paths: %v", err)
	}
}
