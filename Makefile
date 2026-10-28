VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE   ?= ghcr.io/nberlee/mqtt-nats-broker

.PHONY: build test vet lint lint-go lint-md core-build core-test full-suite test-shard experimental-shared-qos2-tests cluster-failover-tests image-smoke-external image-smoke-embedded kubernetes-render helm-check release-version helm-package chart-registry-check helm-publish helm-smoke bench-inbound bench-jspaths bench-outbound depproof soak resource-soak run docker docker-multiarch conformance wiki tidy clean

# Paired soak knobs (see the `soak` target).
SOAK_RUNS ?= 20
SOAK_RUN  ?= TestQoS2Move
SOAK_BASE ?= HEAD
SOAK_DIR  ?= /tmp/mqtt-nats-broker-soak

# The kustomize the example deployment is rendered with (see `kubernetes-render`).
KUSTOMIZE_VERSION := v5.8.1

# The helm the chart is checked with, and the kind and kubectl its cluster install uses
# (see `helm-check` and `helm-smoke`). helm and kind are go-installed, version-pinned
# builds authenticated by the Go checksum database; kubectl is the release binary of the
# kind node's Kubernetes version, checked against the pinned digests. Helm keeps its
# cache, configuration and data under bin/, never in the user's home.
HELM_VERSION    := v4.3.1
KIND_VERSION    := v0.33.0
KUBECTL_VERSION := v1.37.0
KUBECTL_SHA256_amd64 := 6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f
KUBECTL_SHA256_arm64 := 922df28df248cc00a9e025f947704f1d1482de64ece54cfe57e61f19eaf1eef3
HELM_ENV := HELM_CACHE_HOME=$(CURDIR)/bin/helm-home/cache HELM_CONFIG_HOME=$(CURDIR)/bin/helm-home/config HELM_DATA_HOME=$(CURDIR)/bin/helm-home/data
HELM_CHART := charts/mqtt-nats-broker

build: ## Build the binary for the host platform
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o mqtt-nats-broker ./cmd/mqtt-nats-broker

test: ## Run unit tests (default tree)
	go test -race -count=1 -timeout 30m ./...

vet: ## go vet the default tree
	go vet ./...

lint: lint-go lint-md ## Run all linters (Go and Markdown)

# Four passes, one configuration, each must be clean (F-055):
#   1. production only: the one pass that sees a declaration only tests still reach. Every
#      pass that compiles tests counts a test caller as a use.
#   2. the default build with its tests.
#   3. the mqtt_fulltests build, where most of the package's tests live.
#   4. the experimental shared-QoS 2 build, which selects different files than pass 3 (the
#      experimental suite and the disabled twin it replaces).
lint-go: ## Run golangci-lint over production, the default tests and both tagged builds
	golangci-lint run --tests=false ./...
	golangci-lint run ./...
	golangci-lint run --build-tags=mqtt_fulltests ./...
	golangci-lint run --build-tags=mqtt_fulltests,mqtt_experimental_shared_qos2 ./...

lint-md: ## Run markdownlint over the project's own Markdown
	@command -v markdownlint >/dev/null || { \
		echo "markdownlint not found: install with 'npm i -g markdownlint-cli'"; exit 1; }
	markdownlint '**/*.md'

full-suite: ## Run the mqtt_fulltests build: the upstream MQTT suite and the harness-based broker tests.
	go test -tags mqtt_fulltests -timeout 60m ./internal/mqtt/

# One shard of a tagged ./internal/mqtt/ suite under -race, as CI runs it: the tests
# `go test -list` names, every SHARDS-th one starting at SHARD (0-based), so the shards
# together run each test exactly once and a new test joins a shard with no list to edit.
# The whole tagged suite in one -race process outlasts any reasonable timeout.
TEST_TAGS ?= mqtt_fulltests
SHARD     ?= 0
SHARDS    ?= 1
SHARD_TIMEOUT ?= 45m

test-shard: ## Run shard SHARD of SHARDS of the tagged suite (TEST_TAGS) under -race
	@tests=$$(go test -tags '$(TEST_TAGS)' -list '^Test' ./internal/mqtt/ | grep '^Test' \
		| awk -v s=$(SHARD) -v n=$(SHARDS) 'NR % n == s' | paste -sd'|'); \
	test -n "$$tests" || { echo "shard $(SHARD) of $(SHARDS) holds no test"; exit 1; }; \
	echo "shard $(SHARD) of $(SHARDS): $$(echo "$$tests" | tr '|' '\n' | wc -l) tests"; \
	go test -tags '$(TEST_TAGS)' -race -count=1 -timeout $(SHARD_TIMEOUT) -run "^($$tests)\$$" ./internal/mqtt/

experimental-shared-qos2-tests: ## Run the parked, non-gating shared-QoS2 suite.
	go test -tags 'mqtt_fulltests mqtt_experimental_shared_qos2' ./internal/mqtt/

cluster-failover-tests: ## Run the 3-node JetStream tests (nodes run js_ack_fc_v2): Will-outbox failover, takeover arbitration, shared group ownership, the cross-broker orphan sweep, the v2 acknowledgement-layout witness, and an embedded-NATS cluster forming from a staggered start.
	MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -tags mqtt_fulltests -race -run '^(TestMQTTWillOutboxReplicated|TestMQTTTakeover.*Replicated|TestMQTTSharedGroup.*Replicated|TestMQTTQoS0Downgrade.*Replicated|TestMQTTAckReplyLayoutV2Replicated)' ./internal/mqtt/
	MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -race -run '^TestEmbeddedClusterFormsAfterFirstProbeFails$$' ./cmd/mqtt-nats-broker/

depproof: ## Dependency canary: JetStream write-contract regressions plus every mutation
	./test/depproof/run.sh --matrix

bench-inbound: ## Inbound QoS 1/2 acknowledgement throughput, standalone and R3 (MQTTNB_INBOUND_ACK_BENCH_OUT=<csv> to record rows).
	MQTTNB_INBOUND_ACK_BENCH=1 GOMEMLIMIT=3000MiB go test -tags mqtt_fulltests -p 1 -count 1 -v -timeout 90m -run '^TestMQTTInboundAckThroughput$$' ./internal/mqtt/

bench-jspaths: ## JetStream request cost per MQTT operation, standalone and R3 (MQTTNB_JS_PATH_BENCH_OUT=<csv> to record rows, MQTTNB_JS_PATH_BENCH_LOADS=<a,b> to select loads).
	MQTTNB_JS_PATH_BENCH=1 GOMEMLIMIT=3000MiB go test -tags mqtt_fulltests -p 1 -count 1 -v -timeout 90m -run '^TestMQTTJetStreamPathCosts$$' ./internal/mqtt/

bench-outbound: ## Outbound QoS 1/2 delivery throughput to backlogged subscribers, standalone and R3 (MQTTNB_OUTBOUND_DELIVERY_BENCH_OUT=<csv> to record rows, MQTTNB_OUTBOUND_DELIVERY_BENCH_CASES=<a,b> to select cases).
	MQTTNB_OUTBOUND_DELIVERY_BENCH=1 GOMEMLIMIT=3000MiB go test -tags mqtt_fulltests -p 1 -count 1 -v -timeout 120m -run '^TestMQTTOutboundDeliveryThroughput$$' ./internal/mqtt/

resource-soak: ## Resource-lifetime soak, run alone: churn, failover, TLS reloads, WebSocket churn and shutdown leave every broker-owned resource at its expected count (MQTTNB_RESOURCE_SOAK_DIR=<dir> keeps the per-block heap profiles).
	MQTTNB_RESOURCE_SOAK=1 MQTTNB_CLUSTER_FAILOVER_TESTS=1 GOMEMLIMIT=3000MiB go test -tags mqtt_fulltests -p 1 -count 1 -v -timeout 90m -run '^(TestResourceChurnSingleBroker|TestResourceChurnFailover|TestResourceDisplacedDisposalCollected|TestResourceShutdownComplete|TestResourceTLSReloadChurn|TestResourceWebSocketChurn)$$' ./internal/mqtt/

soak: ## Paired flakiness soak: same gated tests on this tree vs a clean baseline
	SOAK_RUNS=$(SOAK_RUNS) SOAK_RUN='$(SOAK_RUN)' SOAK_BASE=$(SOAK_BASE) SOAK_DIR=$(SOAK_DIR) \
		./test/soak.sh

run: build ## Build and run against a local NATS on :4222 (serves MQTT on :1883)
	./mqtt-nats-broker

docker: ## Build a single-arch image for the host
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

image-smoke-external: ## Build the external-NATS image for the host platform and run the container smoke test against it (needs Docker)
	docker build --build-arg VERSION=$(VERSION) -t mqtt-nats-broker:smoke-external .
	MQTTNB_IMAGE_SMOKE_IMAGE=mqtt-nats-broker:smoke-external go test -count=1 -v -timeout 15m ./test/imagesmoke/

image-smoke-embedded: ## Build the embedded-NATS image for the host platform and run its container smoke test, single node and a three-node cluster (needs Docker)
	docker build --target embedded --build-arg VERSION=$(VERSION) -t mqtt-nats-broker:smoke-embedded .
	MQTTNB_IMAGE_SMOKE_EMBEDDED_IMAGE=mqtt-nats-broker:smoke-embedded go test -count=1 -v -timeout 20m ./test/imagesmoke/

kubernetes-render: ## Render docs/deploy/kubernetes (base and TLS overlay) with the pinned kustomize and check the output against the binary
	GOBIN=$(CURDIR)/bin go install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)
	MQTTNB_KUSTOMIZE=$(CURDIR)/bin/kustomize go test -count=1 -v -run '^TestKubernetesManifestRender$$' ./cmd/mqtt-nats-broker/

helm-check: ## Lint and render the Helm chart and the topology-3 NATS chart values with the pinned helm, and check the output against the binary
	GOBIN=$(CURDIR)/bin go install helm.sh/helm/v4/cmd/helm@$(HELM_VERSION)
	$(HELM_ENV) bin/helm repo add --force-update nats https://nats-io.github.io/k8s/helm/charts/
	$(HELM_ENV) bin/helm dependency build $(HELM_CHART)
	for f in $(HELM_CHART)/ci/*.yaml; do $(HELM_ENV) bin/helm lint --strict $(HELM_CHART) -f $$f || exit 1; done
	$(HELM_ENV) bin/helm lint --strict $(HELM_CHART)/charts/nats-*.tgz -f docs/deploy/nats-chart-shared-pod/values.yaml
	$(HELM_ENV) bin/helm lint --strict $(HELM_CHART)/charts/nats-*.tgz -f docs/deploy/nats-chart-shared-pod/values.yaml -f docs/deploy/nats-chart-shared-pod/values-cluster.yaml
	$(HELM_ENV) MQTTNB_HELM=$(CURDIR)/bin/helm go test -count=1 -v -run '^(TestHelmChartStatic|TestHelmChart|TestHelmChartRefusals|TestHelmChartExtensionPoints|TestNATSChartSharedPod)$$' ./cmd/mqtt-nats-broker/

# A release tag is v + a version every publisher reads the same way, checked before any of
# them runs (release.yml's version job):
#   - strict semver (no leading zeros, in the core or in a numeric prerelease identifier),
#     which is what metadata-action accepts, where helm would take loose versions;
#   - no build metadata, since an OCI tag cannot carry "+";
#   - numbers of at most nine digits, inside every parser's integer range;
#   - at most 46 characters, ending in a letter or digit: the version is in two label
#     values, which Kubernetes holds to 63 characters with alphanumeric ends. The chart's
#     helm.sh/chart is "mqtt-nats-broker-<version>" (17 + 46), which the chart would
#     otherwise truncate, and app.kubernetes.io/version is the version itself. Its other
#     consumers are looser: the image tags (the embedded one 9 longer, Docker allows 128),
#     the binaries' file names and main.version;
#   - not ending in "-embedded": the embedded image's tags are the external image's with
#     that suffix in the same repository, so release X-embedded would publish its
#     external image at the tag release X gave its embedded one.
RELEASE_NUM    := (0|[1-9][0-9]{0,8})
RELEASE_PREID  := ($(RELEASE_NUM)|[0-9]*[A-Za-z-][0-9A-Za-z-]*)
RELEASE_TAG_RE := ^v$(RELEASE_NUM)\.$(RELEASE_NUM)\.$(RELEASE_NUM)(-$(RELEASE_PREID)(\.$(RELEASE_PREID))*)?$$

# The tag is read from stdin, never from a make argument: a git tag may contain dollar signs and
# parentheses, which make would expand, and nothing reaches make or the shell as code
# before this check has passed.
release-version: ## Print the release version of the tag on stdin, or refuse a tag that is not a release tag
	@IFS= read -r t; rest=$$(wc -c); printf '%s' "$$t" | grep -Eqx '$(RELEASE_TAG_RE)' && [ "$$rest" -eq 0 ] \
		&& [ $${#t} -le 47 ] && printf '%s' "$$t" | grep -q '[0-9A-Za-z]$$' && ! printf '%s' "$$t" | grep -q -- '-embedded$$' \
		|| { printf 'not a release tag: "%s" (want one line vMAJOR.MINOR.PATCH[-PRERELEASE]: strict semver, numbers of at most 9 digits, no build metadata, at most 46 characters after the v, ending in a letter or digit, not ending in -embedded)\n' "$$t" >&2; exit 1; }; \
	printf '%s\n' "$${t#v}"

# The chart is packaged with the release's version as both its version and its appVersion,
# which is the image tag a default install pulls.
helm-package: ## Package the Helm chart for the release tag TAG into bin/helm-package and check the archive
	v=$$(printf '%s\n' '$(TAG)' | $(MAKE) -s release-version) || exit 1; \
	GOBIN=$(CURDIR)/bin go install helm.sh/helm/v4/cmd/helm@$(HELM_VERSION) && \
	$(HELM_ENV) bin/helm repo add --force-update nats https://nats-io.github.io/k8s/helm/charts/ && \
	$(HELM_ENV) bin/helm dependency build $(HELM_CHART) && \
	rm -rf bin/helm-package && \
	$(HELM_ENV) bin/helm package $(HELM_CHART) --version "$$v" --app-version "$$v" -d bin/helm-package && \
	$(HELM_ENV) MQTTNB_HELM=$(CURDIR)/bin/helm MQTTNB_HELM_PACKAGE=$(CURDIR)/bin/helm-package/mqtt-nats-broker-$$v.tgz \
		MQTTNB_HELM_PACKAGE_VERSION="$$v" MQTTNB_HELM_CHART_REGISTRY='$(CHART_REGISTRY)' \
		go test -count=1 -v -run '^TestHelmChartPackage$$' ./cmd/mqtt-nats-broker/

# Publishes the archive helm-package built and checked: logs in with the same helm
# configuration the push reads, pushes, and checks what the registry serves back. The
# registry must be canonical (OCI repository-name components, no empty or dot segment, no
# trailing slash), so that the repository helm push resolves with path.Join, the one the
# collision check compares, and the one pulled back are the same string.
CHART_REGISTRY_RE := ^oci://[^/]+(/[a-z0-9]+([._-][a-z0-9]+)*)+$$
chart-registry-check:
	@printf '%s\n' '$(CHART_REGISTRY)' | grep -Eqx '$(CHART_REGISTRY_RE)' \
		|| { echo 'not a canonical chart registry: "$(CHART_REGISTRY)" (want oci://<host>/<path>, lowercase OCI path components, no empty or dot segments, no trailing slash)' >&2; exit 1; }

helm-publish: chart-registry-check helm-package ## Push the packaged chart to CHART_REGISTRY (oci://...) and check the chart pulled back
	@v=$$(printf '%s\n' '$(TAG)' | $(MAKE) -s release-version) || exit 1; \
	if [ -n '$(HELM_REGISTRY_USER)' ]; then \
		host=$$(echo '$(CHART_REGISTRY)' | sed 's|^oci://||; s|/.*||'); \
		printf '%s' "$$HELM_REGISTRY_PASSWORD" | $(HELM_ENV) bin/helm registry login "$$host" $(HELM_PUSH_FLAGS) \
			--username '$(HELM_REGISTRY_USER)' --password-stdin || exit 1; \
	fi; \
	$(HELM_ENV) bin/helm push bin/helm-package/mqtt-nats-broker-$$v.tgz '$(CHART_REGISTRY)' $(HELM_PUSH_FLAGS) && \
	$(HELM_ENV) MQTTNB_HELM=$(CURDIR)/bin/helm MQTTNB_HELM_PACKAGE='$(CHART_REGISTRY)/mqtt-nats-broker' \
		MQTTNB_HELM_PACKAGE_VERSION="$$v" MQTTNB_HELM_CHART_REGISTRY='$(CHART_REGISTRY)' MQTTNB_HELM_REGISTRY_FLAGS='$(HELM_PUSH_FLAGS)' \
		go test -count=1 -v -run '^TestHelmChartPackage$$' ./cmd/mqtt-nats-broker/

helm-smoke: helm-check ## Install the chart in a kind cluster, every topology, with both images built for the host (needs Docker)
	GOBIN=$(CURDIR)/bin go install sigs.k8s.io/kind@$(KIND_VERSION)
	arch=$$(go env GOARCH); want=$$(case $$arch in amd64) echo $(KUBECTL_SHA256_amd64);; arm64) echo $(KUBECTL_SHA256_arm64);; esac); \
	if ! echo "$$want  bin/kubectl" | sha256sum -c --status 2>/dev/null; then \
		curl -fsSL -o bin/kubectl https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/linux/$$arch/kubectl && \
		echo "$$want  bin/kubectl" | sha256sum -c && chmod 0755 bin/kubectl; fi
	docker build --build-arg VERSION=$(VERSION) -t mqtt-nats-broker:helm-smoke .
	docker build --target embedded --build-arg VERSION=$(VERSION) -t mqtt-nats-broker:helm-smoke-embedded .
	$(HELM_ENV) MQTTNB_HELM=$(CURDIR)/bin/helm MQTTNB_KIND=$(CURDIR)/bin/kind MQTTNB_KUBECTL=$(CURDIR)/bin/kubectl \
		MQTTNB_HELM_SMOKE_IMAGE=mqtt-nats-broker:helm-smoke MQTTNB_HELM_SMOKE_EMBEDDED_IMAGE=mqtt-nats-broker:helm-smoke-embedded \
		go test -count=1 -v -timeout 60m ./test/helmsmoke/

docker-multiarch: ## Build (and optionally push) the multi-arch image
	docker buildx build --platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

# Where `make wiki` renders a local preview: a new temporary directory unless WIKI_OUT
# names one, which must be absent or empty and outside the checkout, so neither
# markdownlint nor git sees generated pages. Nothing is deleted: wikigen refuses an
# output it could misplace. The release job renders and publishes on its own.
WIKI_OUT ?= $(shell mktemp -d)

wiki: ## Render the GitHub Wiki from docs/ for a local preview (WIKI_OUT=<absent or empty dir>; docs/WIKI.md lists the pages)
	go run ./cmd/wikigen render -out '$(WIKI_OUT)'


conformance: ## Run the MQTT conformance suites against a running broker+NATS
	./test/conformance/run.sh

tidy: ## go mod tidy
	go mod tidy

clean:
	rm -f mqtt-nats-broker
	rm -rf dist
