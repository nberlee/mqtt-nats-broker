# Installation

The broker runs in front of a NATS/JetStream deployment it does not run (the external
topology), or with its own JetStream server in-process (the embedded topology). Every
setting is in [Configuration](CONFIGURATION.md).

## Prerequisites

**nats-server 2.15.1 or newer, on every JetStream peer.** Earlier releases are
unsupported: not degraded, not fallback-handled. The broker's durability guarantees rest
on specific JetStream behaviors (per-subject CAS, interest retention, deduplication,
per-message TTL, double ack); those are asserted against the supported line, and the
streams' prerequisites are validated at startup rather than worked around. The external
cluster's version is an operator prerequisite: the broker cannot see it, and a
mixed-version cluster is unsupported. `go.mod` pins the exact version tested; "JetStream
behaviors this design depends on" in [Architecture](ARCHITECTURE.md) lists what a
version bump has to re-verify.

**One server option.** Configure `jetstream { limits { default_max_consumers: -1 } }` on
every peer, or give the account tier an explicit positive `max_consumers` above your
working set. From 2.15.0 the server otherwise caps each stream at 1000 consumers, and
asking for unlimited arms that cap rather than opting out of it. The broker cannot detect
the setting and cannot refuse early (F-095); the symptom is in
[Troubleshooting](TROUBLESHOOTING.md#subscribe-fails-once-a-stream-has-1000-consumers).
The embedded topology sets it for you.

**NATS permissions.** Every identity an MQTT client authenticates as is denied `$MQTT.>`
and the JetStream API and allowed to ask who it is
([Authentication and authorization](AUTHENTICATION.md#what-every-mqtt-identity-needs));
the broker's own service identity needs the grants in
[Recommended NATS permissions](ARCHITECTURE.md#recommended-nats-permissions).

**Stream replication.** Set `-stream-replicas` to the JetStream cluster size for HA; the
default creates one-replica streams ([Configuration](CONFIGURATION.md#stream-replicas)).

## Run it locally

A broker and a NATS server, from the repository:

```sh
docker compose -f test/conformance/docker-compose.yml up --build
```

## The binary

A release publishes binaries for Linux and macOS on amd64 and arm64. From source:

```sh
go build ./cmd/mqtt-nats-broker
./mqtt-nats-broker -nats nats://nats-1:4222,nats://nats-2:4222 -port 1883
```

The NATS URLs carry no credentials (they are refused); the service identity is given
with `-nats-user`/`-nats-pass` or `-nats-creds`.

## The container images

Both images are multi-arch (amd64 and arm64), `FROM scratch` (the binary and the system
CA bundle only), run as the numeric user `65532:65532`, and support a read-only root
filesystem.

The external-NATS image:

```sh
docker run --rm --read-only --cap-drop ALL -p 1883:1883 -p 8080:8080 \
  -e MQTTNB_NATS_URLS=nats://nats:4222 \
  ghcr.io/nberlee/mqtt-nats-broker:latest
```

A `tls://` NATS URL is verified against the image's CA bundle. For a NATS server under a
private CA, mount the CA and name it with `SSL_CERT_FILE` (for example
`-e SSL_CERT_FILE=/etc/nats-ca/ca.crt`). The image sets no `HOME`, so `-nats-creds` takes
an absolute path. Log timestamps are UTC because the image has no time zone data
([details](ARCHITECTURE.md#the-runtime-image)).

The embedded-NATS image (tags suffixed `-embedded`) runs its own JetStream in-process, so
no NATS deployment is needed. Its baked configuration takes the credentials from four
variables and refuses to start without them; MQTT clients without credentials connect
as the default user, which is kept out of the broker's reserved namespace and the
JetStream API:

```sh
docker run --rm --read-only --cap-drop ALL -p 1883:1883 -p 8080:8080 \
  -v mqtt-data:/data \
  -e MQTTNB_NATS_USER=service -e MQTTNB_NATS_PASS=change-me \
  -e MQTTNB_DEFAULT_USER=mqtt -e MQTTNB_DEFAULT_PASS=change-me-too \
  ghcr.io/nberlee/mqtt-nats-broker:latest-embedded
```

Mount your own nats-server configuration and point `MQTTNB_NATS_EMBEDDED_CONFIG` at it
for other accounts, TLS, or a cluster of broker pods (`embedded/nats-cluster.conf` is
an example) ([details](ARCHITECTURE.md#embedded-nats)).

## Kubernetes

A Helm chart, example manifests, and values that run the broker inside the official
NATS chart's pods: see [Kubernetes](KUBERNETES.md).

## Migrating

From nats-server's own MQTT listener: see [Migration](MIGRATION.md).
