VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
IMAGE   ?= ghcr.io/nberlee/mqtt-nats-bridge

.PHONY: build test vet lint core-build core-test full-suite cluster-failover-tests run docker docker-multiarch conformance tidy clean

build: ## Build the binary for the host platform
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o mqtt-nats-bridge ./cmd/mqtt-nats-bridge

test: ## Run unit tests (default tree)
	go test -race -count=1 ./...

vet: ## go vet the default tree
	go vet ./...

lint: ## Run golangci-lint
	golangci-lint run

full-suite: ## Run the ported upstream MQTT suite against the standalone broker.
	go test -tags mqttport_fulltests ./internal/mqtt/

cluster-failover-tests: ## Run the gated 3-node JetStream Will-outbox failover tests.
	MQTTNB_CLUSTER_FAILOVER_TESTS=1 go test -tags mqttport_fulltests -race -run '^TestMQTTWillOutboxReplicated' ./internal/mqtt/

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
