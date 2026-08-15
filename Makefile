VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE   ?= ghcr.io/nberlee/mqtt-nats-bridge

.PHONY: build test vet lint lint-go lint-md core-build core-test full-suite cluster-failover-tests depproof soak run docker docker-multiarch conformance tidy clean

# Paired soak knobs (see the `soak` target).
SOAK_RUNS ?= 20
SOAK_RUN  ?= TestQoS2Move
SOAK_BASE ?= HEAD
SOAK_DIR  ?= /tmp/mqtt-nats-bridge-soak

build: ## Build the binary for the host platform
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o mqtt-nats-bridge ./cmd/mqtt-nats-bridge

test: ## Run unit tests (default tree)
	go test -race -count=1 ./...

vet: ## go vet the default tree
	go vet ./...

lint: lint-go lint-md ## Run all linters (Go and Markdown)

lint-go: ## Run golangci-lint
	golangci-lint run

lint-md: ## Run markdownlint over the project's own Markdown
	@command -v markdownlint >/dev/null || { \
		echo "markdownlint not found: install with 'npm i -g markdownlint-cli'"; exit 1; }
	markdownlint '**/*.md'

full-suite: ## Run the ported upstream MQTT suite against the standalone broker.
	go test -tags mqttport_fulltests ./internal/mqtt/

cluster-failover-tests: ## Run the 3-node JetStream tests: Will-outbox failover and takeover arbitration.
	MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -tags mqttport_fulltests -race -run '^(TestMQTTWillOutboxReplicated|TestMQTTTakeover.*Replicated)' ./internal/mqtt/

depproof: ## Dependency proof for F-030: the JetStream route regressions plus every mutation
	./test/depproof/run.sh --matrix

soak: ## Paired flakiness soak: same gated tests on this tree vs a clean baseline
	SOAK_RUNS=$(SOAK_RUNS) SOAK_RUN='$(SOAK_RUN)' SOAK_BASE=$(SOAK_BASE) SOAK_DIR=$(SOAK_DIR) \
		./test/soak.sh

run: build ## Build and run against a local NATS on :4222 (serves MQTT on :1883)
	./mqtt-nats-bridge

docker: ## Build a single-arch image for the host
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

docker-multiarch: ## Build (and optionally push) the multi-arch image
	docker buildx build --platform linux/amd64,linux/arm64 \
		--build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

conformance: ## Run the MQTT conformance suites against a running broker+NATS
	./test/conformance/run.sh

tidy: ## go mod tidy
	go mod tidy

clean:
	rm -f mqtt-nats-bridge
	rm -rf dist
