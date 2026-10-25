# mqtt-nats-broker Helm chart

An MQTT 3.1.1 and 5.0 broker over NATS JetStream. The chart is unpublished: there is no
release yet, and its image tags are placeholders until there is.

## Topologies

- **Embedded** (`topology: embedded`, the default): the embedded-NATS image, each pod
  running its own JetStream server in-process, in a StatefulSet. One server, which is
  what a bare install deploys, or with `embedded.cluster.enabled` an odd number of at
  least three peers of one JetStream cluster (`replicas`, default 3).
- **External** (`topology: external`): N stateless broker frontends (`replicas`, default
  3), a Deployment, in front of a NATS/JetStream deployment they do not run. Either a
  NATS you run (`broker.natsURLs`), or the official NATS chart installed beside the
  broker as a dependency (`nats.enabled: true`), one server or a JetStream cluster.
- **Shared pod**: the broker as an extra container in the official NATS chart's own pods.
  That is values for the NATS chart, not this one: see
  `docs/deploy/nats-chart-shared-pod`.

`ci/` holds one values file per shipped variant; each is a working starting point.

```sh
helm dependency build charts/mqtt-nats-broker
helm install broker charts/mqtt-nats-broker      # one embedded broker with its own JetStream
helm install broker charts/mqtt-nats-broker -f charts/mqtt-nats-broker/ci/optional-nats-values.yaml
```

## Before installing

- **The Secret `mqtt-nats-broker-nats`** (`broker.credentials.secretName`), which the chart
  never creates: `user` and `password`, the broker's NATS service identity, and
  `default-user` and `default-password`, the NATS identity anonymous MQTT clients get.
  The default keys are optional only against a NATS you run yourself; the dependency
  and the embedded topology reference them. An MQTT client presenting the service
  credentials acts with the service user's rights, so they must stay private.

  ```sh
  kubectl create secret generic mqtt-nats-broker-nats \
    --from-literal=user=<service user> --from-literal=password=<service password> \
    --from-literal=default-user=<anonymous user> --from-literal=default-password=<anonymous password>
  ```

- **Against a NATS you run**: JetStream enabled, `default_max_consumers` unlimited (-1),
  and every MQTT identity denied `$MQTT.>`, `$JS.>`, `$KV.>` and `$O.>` in both
  directions and allowed to publish `$SYS.REQ.USER.INFO` (`docs/ARCHITECTURE.md`, "What a
  deployment must allow"). The dependency's defaults do all of that.
- **For an embedded cluster**, the Secret `mqtt-nats-broker-routes`
  (`embedded.cluster.routeSecretName`) with the route credentials `user` and `password`.
  Both are embedded in route URLs, so they must be URL-safe: letters, digits, `-`, `.`,
  `_` and `~`.
- **With TLS** (`mqtt.tls.enabled`), a `kubernetes.io/tls` Secret named by
  `mqtt.tls.secretName`. It is mounted as a directory and re-read every 10 s, so a
  renewal reaches new handshakes without a restart. `mqtt.tls.caKey` names a CA bundle
  in the same Secret to verify client certificates; `mqtt.tls.plaintext` serves
  plaintext MQTT on `mqtt.port` beside TLS.

## What it sets

- Restricted Pod Security: the image's uid and gid 65532, the RuntimeDefault seccomp
  profile, every capability dropped, no privilege escalation, a read-only root, no
  service-account token, no Service links. The dependency's pod runs Restricted too.
- Probes on the health port, which no Service exposes: readiness on `/readyz`, withdrawn
  after one failure, liveness on `/healthz`, and in the embedded topology a startupProbe
  bounding the JetStream wait at ten minutes.
- A termination grace of 60 s (75 s embedded), the broker's shutdown budgets plus
  headroom. Raise it with `broker.drainDelay`.
- A PodDisruptionBudget of one pod; an optional HorizontalPodAutoscaler for the external
  topology (`autoscaling.enabled`).

## Customizing

The chart follows the official NATS chart's convention, with the same helpers. Every
rendered object has:

- `merge`: deep-merged into the object. A list in it **replaces** the chart's list.
- `patch`: an RFC 6902 JSON patch applied after the merge. `op: add` with a path ending
  in `/-` **appends** to a list the chart renders.

Values may carry `$tplYaml` (a template whose output is parsed as YAML) and
`$tplYamlSpread`, resolved before the merge and the patch. `extraResources` adds any
other object. The hooks are `deployment`, `statefulSet`, `podTemplate`, `container`,
`service`, `headlessService`, `configMap`, `podDisruptionBudget`, `autoscaling`,
`networkPolicy` and `embedded.storage`; `broker.env` appends environment variables.

An init container, a node selector and tolerations, merged into the pod:

```yaml
podTemplate:
  merge:
    spec:
      nodeSelector:
        kubernetes.io/os: linux
      tolerations:
        - key: dedicated
          value: mqtt
          effect: NoSchedule
      initContainers:
        - name: wait-for-dns
          image: busybox:1.37
          command: ["sh", "-c", "until nslookup nats; do sleep 1; done"]
```

An extra volume and its mount, appended to what the chart renders (with TLS enabled the
pod already has a volume, which a `merge` of `volumes` would replace):

```yaml
podTemplate:
  patch:
    - op: add
      path: /spec/volumes/-
      value:
        name: ca
        configMap:
          name: nats-ca
container:
  patch:
    - op: add
      path: /volumeMounts/-
      value:
        name: ca
        mountPath: /etc/nats-ca
        readOnly: true
broker:
  env:
    SSL_CERT_FILE: /etc/nats-ca/ca.crt
```

A sidecar, and an object named after the release:

```yaml
podTemplate:
  patch:
    - op: add
      path: /spec/containers/-
      value:
        name: exporter
        image: example/exporter:1.0
extraResources:
  - apiVersion: monitoring.coreos.com/v1
    kind: PodMonitor
    metadata:
      name:
        $tplYaml: >
          {{ include "mqtt-nats-broker.fullname" $ | quote }}
    spec:
      podMetricsEndpoints:
        - port: health
```

What the chart ships is checked against the binary (`make helm-check`) and installed in
a kind cluster (`make helm-smoke`). What a customization adds is yours: a `merge` or a
`patch` can override any setting those checks judge, the security context included.

## The NATS dependency

Everything under `nats:` is the official NATS chart's own values
(<https://github.com/nats-io/k8s>, `helm/charts/nats`, version 2.15.1), passed through
untranslated. The chart's defaults there enable JetStream with the consumer limit the
broker needs, create the broker's account with the deny ACL for anonymous clients, take
the four credential variables from `broker.credentials.secretName`, disable NATS Box,
and run the server Restricted. For a JetStream cluster, start from
`ci/optional-nats-cluster-values.yaml`: the routes must be authenticated (the chart refuses
a cluster without route authorization, route credentials or route TLS), and that file
takes the route credentials from the Secret `mqtt-nats-broker-routes` (`user`,
`password`, URL-safe) without putting them in the rendered configuration. Set
`broker.streamReplicas` to the server count. No NetworkPolicy is rendered for the
dependency's route port; add one with `extraResources` where the network is shared.

`docs/ARCHITECTURE.md`, "The Helm chart", has the mechanism, and `docs/CONFORMANCE.md`
what it does not cover.
