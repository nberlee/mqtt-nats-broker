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

# ---- runtime stage ----
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /out/mqtt-nats-broker /usr/local/bin/mqtt-nats-broker

EXPOSE 1883 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/mqtt-nats-broker"]
