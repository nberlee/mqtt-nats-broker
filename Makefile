VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE   ?= ghcr.io/nberlee/mqtt-nats-broker

.PHONY: build test vet lint lint-go lint-md core-build core-test full-suite experimental-shared-qos2-tests cluster-failover-tests image-smoke-external image-smoke-embedded bench-inbound bench-jspaths depproof soak resource-soak run docker docker-multiarch conformance tidy clean

# Paired soak knobs (see the `soak` target).
SOAK_RUNS ?= 20
SOAK_RUN  ?= TestQoS2Move
SOAK_BASE ?= HEAD
SOAK_DIR  ?= /tmp/mqtt-nats-broker-soak

build: ## Build the binary for the host platform
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o mqtt-nats-broker ./cmd/mqtt-nats-broker

test: ## Run unit tests (default tree)
	go test -race -count=1 ./...

vet: ## go vet the default tree
	go vet ./...

lint: lint-go lint-md ## Run all linters (Go and Markdown)

# Four passes, one configuration, each must be clean (F-055):
#   1. production only: the one pass that sees a declaration only tests still reach. Every
#      pass that compiles tests counts a test caller as a use.
#   2. the default build with its tests.
#   3. the mqttport_fulltests build, where most of the package's tests live.
#   4. the experimental shared-QoS 2 build, which selects different files than pass 3 (the
#      experimental suite and the disabled twin it replaces).
lint-go: ## Run golangci-lint over production, the default tests and both tagged builds
	golangci-lint run --tests=false ./...
	golangci-lint run ./...
	golangci-lint run --build-tags=mqttport_fulltests ./...
	golangci-lint run --build-tags=mqttport_fulltests,mqttport_experimental_shared_qos2 ./...

lint-md: ## Run markdownlint over the project's own Markdown
	@command -v markdownlint >/dev/null || { \
		echo "markdownlint not found: install with 'npm i -g markdownlint-cli'"; exit 1; }
	markdownlint '**/*.md'

full-suite: ## Run the ported upstream MQTT suite against the standalone broker.
	go test -tags mqttport_fulltests -timeout 60m ./internal/mqtt/

experimental-shared-qos2-tests: ## Run the parked, non-gating shared-QoS2 suite.
	go test -tags 'mqttport_fulltests mqttport_experimental_shared_qos2' ./internal/mqtt/

cluster-failover-tests: ## Run the 3-node JetStream tests (nodes run js_ack_fc_v2): Will-outbox failover, takeover arbitration, shared group ownership, the cross-broker orphan sweep, the v2 acknowledgement-layout witness, and an embedded-NATS cluster forming from a staggered start.
	MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -tags mqttport_fulltests -race -run '^(TestMQTTWillOutboxReplicated|TestMQTTTakeover.*Replicated|TestMQTTSharedGroup.*Replicated|TestMQTTQoS0Downgrade.*Replicated|TestMQTTAckReplyLayoutV2Replicated)' ./internal/mqtt/
	MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -race -run '^TestEmbeddedClusterFormsAfterFirstProbeFails$$' ./cmd/mqtt-nats-broker/

depproof: ## Dependency canary: JetStream write-contract regressions plus every mutation
	./test/depproof/run.sh --matrix

bench-inbound: ## Inbound QoS 1/2 acknowledgement throughput, standalone and R3 (MQTTNB_INBOUND_ACK_BENCH_OUT=<csv> to record rows).
	MQTTNB_INBOUND_ACK_BENCH=1 GOMEMLIMIT=3000MiB go test -tags mqttport_fulltests -p 1 -count 1 -v -timeout 90m -run '^TestMQTTInboundAckThroughput$$' ./internal/mqtt/

bench-jspaths: ## JetStream request cost per MQTT operation, standalone and R3 (MQTTNB_JS_PATH_BENCH_OUT=<csv> to record rows, MQTTNB_JS_PATH_BENCH_LOADS=<a,b> to select loads).
	MQTTNB_JS_PATH_BENCH=1 GOMEMLIMIT=3000MiB go test -tags mqttport_fulltests -p 1 -count 1 -v -timeout 90m -run '^TestMQTTJetStreamPathCosts$$' ./internal/mqtt/

resource-soak: ## Resource-lifetime soak, run alone: churn, failover, TLS reloads, WebSocket churn and shutdown leave every broker-owned resource at its expected count (MQTTNB_RESOURCE_SOAK_DIR=<dir> keeps the per-block heap profiles).
	MQTTNB_RESOURCE_SOAK=1 MQTTNB_CLUSTER_FAILOVER_TESTS=1 GOMEMLIMIT=3000MiB go test -tags mqttport_fulltests -p 1 -count 1 -v -timeout 90m -run '^(TestResourceChurnSingleBroker|TestResourceChurnFailover|TestResourceDisplacedDisposalCollected|TestResourceShutdownComplete|TestResourceTLSReloadChurn|TestResourceWebSocketChurn)$$' ./internal/mqtt/

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

docker-multiarch: ## Build (and optionally push) the multi-arch image
	docker buildx build --platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

conformance: ## Run the MQTT conformance suites against a running broker+NATS
	./test/conformance/run.sh

tidy: ## go mod tidy
	go mod tidy

clean:
	rm -f mqtt-nats-broker
	rm -rf dist
