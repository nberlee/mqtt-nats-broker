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
cannot see the NATS cluster: `broker.streamReplicas` has no default and the chart
refuses to render without it ([Configuration](CONFIGURATION.md#stream-replicas)), so set
it to the NATS cluster size, normally `3`, or `1` for a single server. The example
manifests set `MQTTNB_STREAM_REPLICAS` to `3` in their Deployment; change it to your
cluster size. The shared-pod values set it to the NATS chart's cluster size. Set it
before the first start: existing streams keep the count they were created with.

To check what a running deployment has, ask NATS about the seven broker-owned streams,
for example with the `nats` CLI as the broker's service user:

```sh
nats stream info '$MQTT_sess' --json | jq .config.num_replicas
```

All seven streams carry the same count; `$MQTT_sess`, `$MQTT_msgs`, `$MQTT_out` and
`$MQTT_rmsgs` are four of them ([Operations](OPERATIONS.md#the-seven-streams)).

## Probes and shutdown

The broker answers `/healthz` and `/readyz` on its health port (`-http-addr`, default
`:8080`), which no Service should expose. Readiness turns 503 at the first SIGTERM, and
the broker waits `-drain-delay` (chart `broker.drainDelay`, default 10 s in the chart)
for the endpoint to leave the Service before it disconnects clients. The pod's
termination grace must cover that wait plus the broker's shutdown budgets; the chart
sets 60 s, 75 s embedded ([Operations](OPERATIONS.md#shutdown)).

## Backups and upgrades

The procedures are in [Operations](OPERATIONS.md#backup-and-restore) and
[Operations](OPERATIONS.md#upgrades-and-rollbacks); what is particular to the chart:

- **Rollouts.** `helm upgrade` and `helm rollback` replace the pods the workload's way.
  The external topology's Deployment uses Kubernetes' default rolling update (a quarter
  of the pods surged and a quarter unavailable, rounded up and down): with up to three
  replicas no old broker stops before a new one is ready, with four or more up to a
  quarter can stop at once. The embedded StatefulSet replaces one pod at a time, highest
  ordinal first. A release that refuses the streams never turns ready, so the rollout
  stops there; in the embedded topology the refusing pod's NATS server is down with it
  (one replica: an outage) until it is rolled back.
- **Holding the embedded StatefulSet at zero pods**, for a cold copy, its restore or an
  offline upgrade: `kubectl scale statefulset <name> --replicas=0`, and on every
  `helm upgrade` until the hold ends pass
  `--set-json 'statefulSet.patch=[{"op":"replace","path":"/spec/replicas","value":0}]'`
  (keeping any `statefulSet.patch` entries of your own in the list). The top-level
  `replicas: 0` does not hold it (the chart reads an empty count as its default), and
  `replicas` keeps sizing the cluster configuration for the pods that come back.
  `helm rollback` takes no values and reapplies a revision's stored replica count, so
  during a hold roll back with `helm upgrade` to the previous chart or image instead.
  End the hold with a `helm upgrade` that sets the patch list back explicitly, for
  example `--reuse-values --set-json 'statefulSet.patch=[]'`: an upgrade that only
  leaves the patch out keeps it, because Helm reuses the release's stored values when
  none are given.
- **The volumes.** Each pod's store is the claim `data-<statefulset>-<ordinal>`; scaling
  down keeps the claims. Copy them once every pod has terminated and its volume is
  detached. To restore, keep the hold, delete each claim and recreate it under the same
  name from its copy (a volume snapshot as its `dataSource`), each to the ordinal it came
  from, then end the hold.
- **Resetting the streams across a format change**: hold at zero, delete every `data-`
  claim, deploy the new release under the hold, end it. This discards everything the
  embedded servers store, so it applies only when they hold nothing but the seven
  broker-owned streams (the chart's configuration; the broker does not check it). An
  embedded configuration that stores other streams has no supported reset across a
  format change ([Conformance](CONFORMANCE.md#backup-and-restore-what-the-procedures-leave)).

## TLS Secrets

Mount the TLS Secret as a directory, never with `subPath`, or certificate renewals never
reach the broker ([TLS](TLS.md#kubernetes-secrets)).
