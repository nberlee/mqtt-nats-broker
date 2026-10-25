package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// What a release publishes is judged here, not the chart directory: the archive helm
// package builds (make helm-package) and the one a registry serves back after the push
// (make helm-publish). The release's version is the chart's version AND its appVersion,
// and the appVersion is the image tag a default install pulls, so the archive is only
// correct if it names images the same release pushed.

const helmChartName = "mqtt-nats-broker"

// TestHelmChartPackage judges a packaged chart: MQTTNB_HELM_PACKAGE is the archive, or an
// oci:// reference pulled at exactly MQTTNB_HELM_PACKAGE_VERSION (pulling without the
// version would fetch whatever is newest and prove nothing about what was pushed).
func TestHelmChartPackage(t *testing.T) {
	bin := helmBin(t)
	ref, version := os.Getenv("MQTTNB_HELM_PACKAGE"), os.Getenv("MQTTNB_HELM_PACKAGE_VERSION")
	if ref == "" || version == "" {
		t.Skip("MQTTNB_HELM_PACKAGE and MQTTNB_HELM_PACKAGE_VERSION not set; run make helm-package")
	}
	archive := ref
	if strings.HasPrefix(ref, "oci://") {
		dir := t.TempDir()
		args := append([]string{"pull", ref, "--version", version, "-d", dir},
			strings.Fields(os.Getenv("MQTTNB_HELM_REGISTRY_FLAGS"))...)
		if out, err := runHelm(bin, nil, args...); err != nil {
			t.Fatalf("helm %v: %v", args, err)
		} else {
			t.Logf("%s", out)
		}
		archive = filepath.Join(dir, helmChartName+"-"+version+".tgz")
	}
	files := readChartArchive(t, archive)

	var values struct {
		Image struct{ Repository string } `yaml:"image"`
	}
	if err := yaml.Unmarshal(archiveFile(t, files, "values.yaml"), &values); err != nil {
		t.Fatal(err)
	}
	repository := values.Image.Repository
	external := imageList(t, "MQTTNB_HELM_EXTERNAL_IMAGES")
	embedded := imageList(t, "MQTTNB_HELM_EMBEDDED_IMAGES")

	t.Run("chart-metadata", func(t *testing.T) {
		var c struct {
			Name       string `yaml:"name"`
			Version    string `yaml:"version"`
			AppVersion string `yaml:"appVersion"`
		}
		if err := yaml.Unmarshal(archiveFile(t, files, "Chart.yaml"), &c); err != nil {
			t.Fatal(err)
		}
		if c.Name != helmChartName || c.Version != version || c.AppVersion != version {
			t.Errorf("Chart.yaml: name %q version %q appVersion %q; want %q, %q, %q",
				c.Name, c.Version, c.AppVersion, helmChartName, version, version)
		}
	})

	t.Run("contents", func(t *testing.T) {
		// helm serializes a dependency expanded, not as the nested .tgz charts/ holds.
		var dep struct{ Version string }
		if err := yaml.Unmarshal(archiveFile(t, files, "charts/nats/Chart.yaml"), &dep); err != nil {
			t.Fatal(err)
		}
		if want := lockedDependencyVersion(t, "nats"); dep.Version != want {
			t.Errorf("bundled nats chart %q; Chart.lock pins %q", dep.Version, want)
		}
		for name := range files {
			segs := strings.Split(name, "/")
			for i, s := range segs {
				if strings.HasPrefix(s, ".") && (i != len(segs)-1 || s != ".helmignore") {
					t.Errorf("hidden entry %q in the archive", name)
					break
				}
			}
		}
	})

	t.Run("default-images", func(t *testing.T) {
		embObjs := renderIsolated(t, bin, archive)
		extObjs := renderIsolated(t, bin, archive, filepath.Join(helmChartDir, "ci", "external-values.yaml"))
		// The appVersion is a label value too, so the version must be one Kubernetes takes.
		checkVersionLabels(t, embObjs, version)
		checkVersionLabels(t, extObjs, version)
		emb := brokerImage(t, embObjs, "StatefulSet")
		if want := repository + ":" + version + "-embedded"; emb != want {
			t.Errorf("embedded image %q, want %q", emb, want)
		}
		ext := brokerImage(t, extObjs, "Deployment")
		if want := repository + ":" + version; ext != want {
			t.Errorf("external image %q, want %q", ext, want)
		}
		// The release passes the tags its image jobs pushed: the chart must name one of them.
		if embedded != nil && !slices.Contains(embedded, emb) {
			t.Errorf("embedded image %q is not one the release pushed: %q", emb, embedded)
		}
		if external != nil && !slices.Contains(external, ext) {
			t.Errorf("external image %q is not one the release pushed: %q", ext, external)
		}
	})

	t.Run("dependency-offline", func(t *testing.T) {
		// helm's homes are empty, so nothing but the archive can supply the dependency.
		objs := renderIsolated(t, bin, archive, filepath.Join(helmChartDir, "ci", "optional-nats-values.yaml"))
		var nats bool
		for _, obj := range objs {
			if obj["kind"] == "StatefulSet" && slices.Contains(containerNames(obj), "nats") {
				nats = true
			}
		}
		if !nats {
			t.Error("the bundled NATS chart rendered no nats StatefulSet")
		}
	})

	t.Run("registry", func(t *testing.T) {
		registry := os.Getenv("MQTTNB_HELM_CHART_REGISTRY")
		if registry == "" {
			t.Skip("MQTTNB_HELM_CHART_REGISTRY not set")
		}
		// Landing in an image repository would make the chart and the image overwrite
		// each other's tags.
		chartRepo := chartRepository(registry)
		repos := []string{repository}
		for _, img := range append(slices.Clone(external), embedded...) {
			repos = append(repos, imageRepository(img))
		}
		for _, r := range repos {
			if path.Clean(strings.ToLower(r)) == chartRepo {
				t.Errorf("chart repository %q is the image repository %q", chartRepo, r)
			}
		}
	})
}

// imageList reads a newline-separated list of image references from env, as
// metadata-action outputs its tags: nil when the variable is unset, and a failure when it
// is set but lists nothing, since an image job that reports no tags pushed none the chart
// could name.
func imageList(t *testing.T, env string) []string {
	t.Helper()
	s, ok := os.LookupEnv(env)
	if !ok {
		return nil
	}
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		t.Fatalf("%s is set but lists no image", env)
	}
	return out
}

// labelValue is a Kubernetes label value: at most 63 characters, alphanumeric at both ends.
var labelValue = regexp.MustCompile(`^([A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?)?$`)

// checkVersionLabels requires every label on the rendered objects and their pod templates
// to be a valid label value, and the broker's two version labels to carry the release
// version whole: app.kubernetes.io/version is the version, and helm.sh/chart is the chart
// name and version, which the chart truncates at 63 characters.
func checkVersionLabels(t *testing.T, objs []map[string]any, version string) {
	t.Helper()
	var sawVersion bool
	check := func(where string, meta any) {
		m, _ := meta.(map[string]any)
		labels, _ := m["labels"].(map[string]any)
		for k, v := range labels {
			s := fmt.Sprint(v)
			if len(s) > 63 || !labelValue.MatchString(s) {
				t.Errorf("%s: label %s=%q is not a Kubernetes label value", where, k, s)
			}
			if k == "helm.sh/chart" && labels["app.kubernetes.io/name"] == helmChartName && s != helmChartName+"-"+version {
				t.Errorf("%s: helm.sh/chart %q, want %q untruncated", where, s, helmChartName+"-"+version)
			}
			if k == "app.kubernetes.io/version" && labels["app.kubernetes.io/name"] == helmChartName {
				sawVersion = true
				if s != version {
					t.Errorf("%s: app.kubernetes.io/version %q, want %q", where, s, version)
				}
			}
		}
	}
	for _, obj := range objs {
		where := fmt.Sprint(obj["kind"], "/", obj["metadata"].(map[string]any)["name"])
		check(where, obj["metadata"])
		if spec, ok := obj["spec"].(map[string]any); ok {
			if tmpl, ok := spec["template"].(map[string]any); ok {
				check(where+" pod template", tmpl["metadata"])
			}
		}
	}
	if !sawVersion {
		t.Error("no broker object carries app.kubernetes.io/version")
	}
}

// chartRepository is the repository helm push writes a chart to: the registry path
// without its scheme, joined with the chart name by path.Join, as helm's OCI pusher does,
// so a trailing slash, a doubled separator or a dot segment resolves as it will.
func chartRepository(registry string) string {
	return strings.ToLower(path.Join(strings.TrimPrefix(registry, "oci://"), helmChartName))
}

// TestChartRepositoryResolvesAsHelmPush pins chartRepository to the destinations helm
// push resolves, including the spellings that land a chart in the images' repository.
func TestChartRepositoryResolvesAsHelmPush(t *testing.T) {
	for registry, want := range map[string]string{
		"oci://ghcr.io/nberlee/charts":    "ghcr.io/nberlee/charts/mqtt-nats-broker",
		"oci://ghcr.io/nberlee":           "ghcr.io/nberlee/mqtt-nats-broker",
		"oci://ghcr.io/nberlee/":          "ghcr.io/nberlee/mqtt-nats-broker",
		"oci://ghcr.io//nberlee":          "ghcr.io/nberlee/mqtt-nats-broker",
		"oci://ghcr.io/nberlee/charts/..": "ghcr.io/nberlee/mqtt-nats-broker",
		"oci://ghcr.io/nberlee/./":        "ghcr.io/nberlee/mqtt-nats-broker",
		"oci://GHCR.io/NBerlee/charts":    "ghcr.io/nberlee/charts/mqtt-nats-broker",
	} {
		if got := chartRepository(registry); got != want {
			t.Errorf("chartRepository(%q) = %q, want %q", registry, got, want)
		}
	}
}

// imageRepository strips the tag or digest from an image reference.
func imageRepository(ref string) string {
	if i := strings.Index(ref, "@"); i >= 0 {
		return ref[:i]
	}
	if i := strings.LastIndex(ref, ":"); i > strings.LastIndex(ref, "/") {
		return ref[:i]
	}
	return ref
}

// readChartArchive returns every regular file in a chart archive by its path.
func readChartArchive(t *testing.T, path string) map[string][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		files[h.Name] = b
	}
}

func archiveFile(t *testing.T, files map[string][]byte, name string) []byte {
	t.Helper()
	b, ok := files[helmChartName+"/"+name]
	if !ok {
		t.Fatalf("no %s/%s in the archive", helmChartName, name)
	}
	return b
}

// lockedDependencyVersion is the version Chart.lock pins for a dependency.
func lockedDependencyVersion(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(helmChartDir, "Chart.lock"))
	if err != nil {
		t.Fatal(err)
	}
	var lock struct {
		Dependencies []struct{ Name, Version string }
	}
	if err := yaml.Unmarshal(b, &lock); err != nil {
		t.Fatal(err)
	}
	for _, d := range lock.Dependencies {
		if d.Name == name {
			return d.Version
		}
	}
	t.Fatalf("Chart.lock pins no %s", name)
	return ""
}

func runHelm(bin string, env []string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%v: %s", err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// renderIsolated renders a chart with helm's cache, configuration and data homes empty,
// so no repository or cached chart can stand in for what the archive lacks.
func renderIsolated(t *testing.T, bin, chart string, values ...string) []map[string]any {
	t.Helper()
	home := t.TempDir()
	env := []string{
		"HELM_CACHE_HOME=" + filepath.Join(home, "cache"),
		"HELM_CONFIG_HOME=" + filepath.Join(home, "config"),
		"HELM_DATA_HOME=" + filepath.Join(home, "data"),
	}
	args := []string{"template", helmRelease, chart, "--namespace", helmNamespace}
	for _, v := range values {
		args = append(args, "-f", v)
	}
	out, err := runHelm(bin, env, args...)
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

func podContainers(obj map[string]any) []map[string]any {
	spec, _ := obj["spec"].(map[string]any)
	tmpl, _ := spec["template"].(map[string]any)
	pod, _ := tmpl["spec"].(map[string]any)
	list, _ := pod["containers"].([]any)
	var out []map[string]any
	for _, c := range list {
		if m, ok := c.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func containerNames(obj map[string]any) []string {
	var names []string
	for _, c := range podContainers(obj) {
		names = append(names, fmt.Sprint(c["name"]))
	}
	return names
}

// brokerImage returns the broker container's image in the one workload of kind.
func brokerImage(t *testing.T, objs []map[string]any, kind string) string {
	t.Helper()
	for _, c := range podContainers(findObject(t, objs, kind)) {
		if c["name"] == "broker" {
			return fmt.Sprint(c["image"])
		}
	}
	t.Fatalf("no broker container in the %s", kind)
	return ""
}

// TestReleaseWorkflow checks the order and destinations release.yml publishes with. A tag
// that is not a release tag must publish nothing, so every publishing job waits on the
// version check; the chart names its images by tag, so it waits on both image jobs and
// is checked against the tags they report; and its OCI repository is under /charts,
// never the images' repository (helm push appends the chart name to the registry path).
func TestReleaseWorkflow(t *testing.T) {
	b, err := os.ReadFile("../../.github/workflows/release.yml")
	if err != nil {
		t.Fatal(err)
	}
	type step struct {
		ID  string            `yaml:"id"`
		Run string            `yaml:"run"`
		Env map[string]string `yaml:"env"`
	}
	type job struct {
		Needs   yaml.Node         `yaml:"needs"`
		Outputs map[string]string `yaml:"outputs"`
		Steps   []step            `yaml:"steps"`
	}
	var wf struct {
		Jobs map[string]job `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(b, &wf); err != nil {
		t.Fatal(err)
	}
	needs := func(name string) []string {
		j, ok := wf.Jobs[name]
		if !ok {
			t.Fatalf("release.yml has no job %q", name)
		}
		var list []string
		switch j.Needs.Kind {
		case yaml.ScalarNode:
			list = []string{j.Needs.Value}
		case yaml.SequenceNode:
			if err := j.Needs.Decode(&list); err != nil {
				t.Fatal(err)
			}
		}
		return list
	}
	runs := func(name, what string) *step {
		for i, s := range wf.Jobs[name].Steps {
			if strings.Contains(s.Run, what) {
				return &wf.Jobs[name].Steps[i]
			}
		}
		t.Errorf("job %s runs no %q", name, what)
		return nil
	}

	runs("version", "| make -s release-version")
	for name := range wf.Jobs {
		if name != "version" && !slices.Contains(needs(name), "version") {
			t.Errorf("job %s publishes without waiting on the version check", name)
		}
	}
	for _, img := range []string{"image", "image-embedded"} {
		if !slices.Contains(needs("chart"), img) {
			t.Errorf("chart does not wait on %s", img)
		}
		if got := wf.Jobs[img].Outputs["tags"]; got != "${{ steps.meta.outputs.tags }}" {
			t.Errorf("%s outputs tags %q, want its metadata step's tags", img, got)
		}
	}
	if s := runs("chart", "make helm-publish"); s != nil {
		if got := s.Env["MQTTNB_HELM_EXTERNAL_IMAGES"]; got != "${{ needs.image.outputs.tags }}" {
			t.Errorf("chart's external image list %q", got)
		}
		if got := s.Env["MQTTNB_HELM_EMBEDDED_IMAGES"]; got != "${{ needs.image-embedded.outputs.tags }}" {
			t.Errorf("chart's embedded image list %q", got)
		}
		m := regexp.MustCompile(`CHART_REGISTRY="?(oci://[^"\s]+)`).FindStringSubmatch(s.Run)
		if m == nil || !strings.HasSuffix(m[1], "/charts") {
			t.Errorf("chart's CHART_REGISTRY %q does not end in /charts", m)
		}
	}
}
