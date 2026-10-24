# syntax=docker/dockerfile:1

# ---- build stage ----
# Cross-compiles for the target platform selected by buildx (linux/amd64, linux/arm64).
FROM --platform=$BUILDPLATFORM golang:1.26 AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src

# Cache modules first.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/mqtt-nats-broker ./cmd/mqtt-nats-broker

# The embedded image's JetStream store; owned by the runtime user in that stage.
RUN mkdir -m 0755 /out/data

# ---- runtime stage: the embedded-NATS image (build with --target embedded) ----
# The same binary running its own nats-server in-process from the baked configuration,
# which takes its credentials from MQTTNB_NATS_USER/PASS and MQTTNB_DEFAULT_USER/PASS
# and refuses to start without them. /data is the JetStream store: a volume, so the
# root stays read-only, owned by the runtime user so a fresh volume is writable.
FROM scratch AS embedded

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/mqtt-nats-broker /usr/local/bin/mqtt-nats-broker
COPY embedded/nats.conf /etc/mqtt-nats-broker/nats.conf
COPY --from=build --chown=65532:65532 /out/data /data

ENV MQTTNB_NATS_EMBEDDED_CONFIG=/etc/mqtt-nats-broker/nats.conf
VOLUME ["/data"]
EXPOSE 1883 8080
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/mqtt-nats-broker"]

# ---- runtime stage: the external-NATS image (the default target, so it stays last) ----
# Nothing but the static binary and the CA bundle: no shell, no package manager, no
# passwd database, and nothing the broker writes, so it runs with a read-only root.
# The bundle is load-bearing: a tls:// NATS URL is verified against the system root
# pool, which Go reads from this path first. It is architecture-independent, so the
# build platform's copy serves every target.
FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/mqtt-nats-broker /usr/local/bin/mqtt-nats-broker

EXPOSE 1883 8080
# Numeric, so Kubernetes runAsNonRoot can verify it without a passwd database; the
# group too, so the process is not left in group 0.
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/mqtt-nats-broker"]
