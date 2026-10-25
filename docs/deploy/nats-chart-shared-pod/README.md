# The broker in the official NATS chart's pods

Values for the official NATS chart (<https://github.com/nats-io/k8s>, `helm/charts/nats`,
version 2.15.1) that add mqtt-nats-broker as an extra container to every NATS pod. Each
broker connects to its own pod's server over loopback with the external-NATS image.

- `values.yaml`: one NATS server and one broker.
- `values-cluster.yaml`, on top of it: three NATS servers in one JetStream cluster, a
  broker in each pod, the broker's streams replicated three ways.

```sh
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm install nats nats/nats --version 2.15.1 -f values.yaml                       # one server
helm install nats nats/nats --version 2.15.1 -f values.yaml -f values-cluster.yaml  # cluster
```

## Before installing

- **The Secret `mqtt-nats-broker-nats`** with `user`, `password`, `default-user` and
  `default-password`: the broker's service identity and the identity anonymous MQTT
  clients get. Both the nats container (whose account block references them) and the
  broker read it.
- **For the cluster, the Secret `mqtt-nats-broker-routes`** with `user` and `password`,
  the route credentials (URL-safe: letters, digits, `-`, `.`, `_`, `~`). The routes are
  authenticated, so reaching port 6222 is not joining the cluster; the credentials reach
  the server as environment variables and never appear in its rendered configuration.
- **The image tag** in `podTemplate.patch` is a placeholder. Replace it with a published
  release tag or digest of the external-NATS image.

## What it sets

- JetStream, with `default_max_consumers` unlimited, and an account `MQTT` whose default
  user is denied `$MQTT.>`, `$JS.>`, `$KV.>` and `$O.>` both ways. NATS Box is off.
- Restricted Pod Security for every container: NATS and its reloader as uid 1000, the
  broker as the image's 65532.
- The MQTT port on the chart's client Service.
- **The shutdown order.** NATS must outlive the broker, which drains its clients and then
  drains and flushes its own NATS connection. The NATS chart's preStop hook enters lame
  duck mode at once, which would evict that connection mid-drain, so these values replace
  it: the hook waits in the pod's shared PID namespace until the broker process is gone,
  then enters lame duck. The grace is 120 s: the broker's 60, then lame duck (10 + 30 s),
  then 20 s of shutdown overhead. Raise it with `config.lameDuckGracePeriod`,
  `config.lameDuckDuration` or the broker's `MQTTNB_DRAIN_DELAY`.

Startup is not ordered: a broker that starts before its server answers exits and is
restarted until it does (`docs/CONFORMANCE.md`). `docs/ARCHITECTURE.md`, "The Helm chart",
has the mechanism.
