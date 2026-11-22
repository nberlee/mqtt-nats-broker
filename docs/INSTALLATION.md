# Installation

The broker runs in front of a NATS/JetStream deployment it does not run (the external
topology), or with its own JetStream server in-process (the embedded topology). Every
setting is in [Configuration](CONFIGURATION.md).

## Prerequisites

**nats-server 2.15.2 or newer, on every JetStream peer.** Earlier releases are
unsupported: not degraded, not fallback-handled. The broker's durability guarantees rest
on specific JetStream behaviors (per-subject CAS, interest retention, deduplication,
per-message TTL, double ack); those are asserted against the supported line, and the
streams' prerequisites are validated at startup rather than worked around. The broker
refuses to start when the NATS server it connects to is older, or when the server answering
JetStream (the meta leader in a cluster) reports a JetStream API level below 5; it cannot
see the other peers, so the version on every peer stays the operator's prerequisite, and a
mixed-version cluster is unsupported. `go.mod` pins the exact version tested; "JetStream
behaviors this design depends on" in [Architecture](ARCHITECTURE.md) lists what a
version bump has to re-verify.

**Not v2.15.3.** It is above the floor and passes every startup check, but while a peer
recovers a consumer it can report it not found, and the broker then replaces that
client's durable and skips what it still owed (CONSUMERINFO-NONODE-NOTFOUND,
[known upstream issues](known-upstream-issues.md); the residual is in
[Conformance](CONFORMANCE.md#broker-owned-stream-prerequisites-startup-refusal-a-snapshot-and-dropped-fallbacks)).
Run v2.15.2 on every peer until a release carries
[nats-server#8894](https://github.com/nats-io/nats-server/pull/8894).

**Header support.** Do not set `no_header_support` on any peer; the broker refuses a
connected server that has it.

**One server option.** Configure `jetstream { limits { default_max_consumers: -1 } }` on
every peer, or give the account tier an explicit positive `max_consumers` above your
working set. From 2.15.0 the server otherwise caps each stream at 1000 consumers, and
asking for unlimited arms that cap rather than opting out of it. The broker cannot detect
the setting and cannot refuse early
([issue #97](https://github.com/nberlee/mqtt-nats-broker/issues/97)); the symptom is in
[Troubleshooting](TROUBLESHOOTING.md#subscribe-fails-once-a-stream-has-1000-consumers).
The embedded topology sets it for you.

**NATS permissions.** Every identity an MQTT client authenticates as is denied `$MQTT.>`
and the JetStream API and allowed to ask who it is
([Authentication and authorization](AUTHENTICATION.md#what-every-mqtt-identity-needs));
the broker's own service identity needs the grants in
[Recommended NATS permissions](ARCHITECTURE.md#recommended-nats-permissions).

**Stream replication.** `-stream-replicas` is required: set it to the JetStream cluster
size for HA, `1` for a single server ([Configuration](CONFIGURATION.md#stream-replicas)).

## Run it locally

A broker and a NATS server, from the repository:

```sh
docker compose -f test/conformance/docker-compose.yml up --build
```

## The binary

A release publishes binaries for Linux and macOS on amd64 and arm64. From source:

```sh
go build ./cmd/mqtt-nats-broker
./mqtt-nats-broker -nats nats://nats-1:4222,nats://nats-2:4222 -stream-replicas 3 -port 1883
```

`-stream-replicas` is the JetStream cluster size, `3` here; use `1` against a single
server.

The NATS URLs carry no credentials (they are refused); the service identity is given
with `-nats-user`/`-nats-pass` or `-nats-creds`.

## The container images

Both images are multi-arch (amd64 and arm64), `FROM scratch` (the binary and the system
CA bundle only), run as the numeric user `65532:65532`, and support a read-only root
filesystem.

The external-NATS image:

```sh
docker run --rm --read-only --cap-drop ALL -p 1883:1883 -p 8080:8080 \
  -e MQTTNB_NATS_URLS=nats://nats:4222 -e MQTTNB_STREAM_REPLICAS=3 \
  ghcr.io/nberlee/mqtt-nats-broker:latest
```

`MQTTNB_STREAM_REPLICAS` is the JetStream cluster size, as for the binary.

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
  -v mqtt-data:/data -e MQTTNB_STREAM_REPLICAS=1 \
  -e MQTTNB_NATS_USER=service -e MQTTNB_NATS_PASS=change-me \
  -e MQTTNB_DEFAULT_USER=mqtt -e MQTTNB_DEFAULT_PASS=change-me-too \
  ghcr.io/nberlee/mqtt-nats-broker:latest-embedded
```

The image sets no stream replica count: `1` above is the one server its baked
configuration runs, and a mounted cluster configuration takes the cluster size.

Mount your own nats-server configuration and point `MQTTNB_NATS_EMBEDDED_CONFIG` at it
for other accounts, TLS, or a cluster of broker pods (`embedded/nats-cluster.conf` is
an example) ([details](ARCHITECTURE.md#embedded-nats)).

### Commit images

Every commit on `main` whose CI passes is also published, both images, multi-arch, with
provenance and an SBOM: `sha-<short commit>` for that commit, the embedded image suffixed
as before (`sha-<short commit>-embedded`). `main` and `main-embedded` move to the head of
`main` once that commit's images are published, so a head whose CI went red leaves them
on an earlier commit. These
are builds of an unreleased tree with no compatibility promise; a commit whose CI went red
publishes nothing. `latest` and the version tags are only ever releases.

A pull request's images (`pr-<number>`, `pr-<number>-<short commit>`, and their
`-embedded` variants) are built only after the repository owner approves that run; a run
stops before building if the approval environment is missing or does not require the
owner's review.

## Kubernetes

A Helm chart, example manifests, and values that run the broker inside the official
NATS chart's pods: see [Kubernetes](KUBERNETES.md).

## Migrating

From nats-server's own MQTT listener: see [Migration](MIGRATION.md).
