# Kubernetes

Three ways to run the broker on Kubernetes, each with its own page:

- **[The Helm chart](../charts/mqtt-nats-broker/README.md)** deploys either topology:
  embedded (the broker and its JetStream server in one pod, a StatefulSet, one server
  or a cluster of three or more) or external (stateless broker pods, a Deployment, in
  front of a NATS you run or the official NATS chart installed beside it). Start here.
- **[The example manifests](deploy/kubernetes/README.md)** are a kustomize tree for the
  external topology only: a Deployment and a Service, with TLS and plaintext-beside-TLS
  overlays.
- **[The broker in the NATS chart's pods](deploy/nats-chart-shared-pod/README.md)** is a
  values file for the official NATS chart that adds the broker as a container in each
  NATS pod.

## Three numbers an HA deployment sets

They are independent, and confusing them is how an HA deployment ends up without HA:

| Number | What it is | Where it is set |
|---|---|---|
| Broker pods | how many MQTT frontends accept connections; any client may reconnect to any of them | chart `replicas` (external), the Deployment's `replicas` |
| NATS servers | how many servers form the JetStream cluster | the NATS deployment; chart `replicas` in the embedded topology, where each pod is one |
| Stream replicas | on how many of those servers each broker-owned stream keeps its data | `-stream-replicas`, chart `broker.streamReplicas` |

The embedded topology ties the last two together: each pod is one JetStream server and
the chart sets the stream replicas to the pod count. In the external topology the chart
cannot see the NATS cluster: `broker.streamReplicas` defaults to `0`, which creates
one-replica streams ([Configuration](CONFIGURATION.md#stream-replicas)), so set it to
the NATS cluster size, normally `3`. The example manifests do not set it either: add
`MQTTNB_STREAM_REPLICAS` to their Deployment. The shared-pod values set it to the NATS
chart's cluster size. Set it before the first start: existing streams keep the count
they were created with.

To check what a running deployment has, ask NATS about the ten broker-owned streams,
for example with the `nats` CLI as the broker's service user:

```sh
nats stream info '$MQTT_sess' --json | jq .config.num_replicas
```

All ten streams carry the same count; `$MQTT_sess`, `$MQTT_msgs`, `$MQTT_out` and
`$MQTT_rmsgs` are four of them ([Operations](OPERATIONS.md#the-ten-streams)).

## Probes and shutdown

The broker answers `/healthz` and `/readyz` on its health port (`-http-addr`, default
`:8080`), which no Service should expose. Readiness turns 503 at the first SIGTERM, and
the broker waits `-drain-delay` (chart `broker.drainDelay`, default 10 s in the chart)
for the endpoint to leave the Service before it disconnects clients. The pod's
termination grace must cover that wait plus the broker's shutdown budgets; the chart
sets 60 s, 75 s embedded ([Operations](OPERATIONS.md#shutdown)).

## TLS Secrets

Mount the TLS Secret as a directory, never with `subPath`, or certificate renewals never
reach the broker ([TLS](TLS.md#kubernetes-secrets)).
