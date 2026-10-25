# Example Kubernetes deployment

N stateless MQTT frontends, the external-NATS image, in front of a NATS/JetStream
deployment they do not run. It is a kustomize tree:

- `base`: the Deployment and a LoadBalancer Service, plaintext MQTT on 1883.
- `components/tls`: MQTT over TLS on 8883 from a mounted Secret.
- `overlays/tls`: the base with the TLS component, the entry point for TLS.
- `components/plaintext`: plaintext MQTT on 1883 beside TLS on 8883, for clients that
  cannot do TLS yet. Credentials and payloads cross 1883 in clear, and no client
  certificate is checked on it.
- `overlays/tls-plaintext`: the base with the TLS and plaintext components, both ports
  on one Service.

```sh
kubectl apply -k docs/deploy/kubernetes/base          # plaintext
kubectl apply -k docs/deploy/kubernetes/overlays/tls  # TLS
kubectl apply -k docs/deploy/kubernetes/overlays/tls-plaintext  # TLS 8883 + plaintext 1883
```

This example runs the broker against an external NATS. The embedded-NATS deployment
(the broker and its JetStream peer in one pod, a StatefulSet) is section 5's topology
bullet in `docs/ROADMAP.md`.

The plaintext base provides no transport encryption. Use it on a trusted network or
behind separately configured TLS termination.

## Before applying

- **NATS with JetStream**, reachable as `nats://nats:4222` from the broker's namespace.
  Change `MQTTNB_NATS_URLS` otherwise.
- **The client ACL.** Every MQTT identity must be denied `$MQTT.>`, `$JS.>`, `$KV.>` and
  `$O.>` in both directions, and allowed to publish `$SYS.REQ.USER.INFO`. See
  `docs/ARCHITECTURE.md`, "What a deployment must allow".
- **The Secret `mqtt-nats-broker-nats`**, holding the service identity under `user` and
  `password`, both required. Optional keys `default-user` and `default-password` hold the
  NATS identity anonymous MQTT clients get; without them, an anonymous client connects
  to NATS with no credentials, and the server's own authorization decides.

```sh
kubectl create secret generic mqtt-nats-broker-nats \
  --from-literal=user=<service user> --from-literal=password=<service password>
```

- **With TLS, the Secret `mqtt-nats-broker-tls`** of type `kubernetes.io/tls`
  (`tls.crt`, `tls.key`), which is what cert-manager issues. It is required once the
  overlay is applied. The broker re-reads it every `MQTTNB_TLS_RELOAD_INTERVAL` (10 s),
  so a renewal reaches new handshakes without a restart. To verify client certificates,
  add a CA file to the mount and set `MQTTNB_TLS_CA` to its path.
- **The image tag** in `base/deployment.yaml` is a placeholder. Replace it with a
  published release tag or digest.

## What it sets

- Restricted Pod Security: non-root as the image's uid and gid 65532, the RuntimeDefault
  seccomp profile, every capability dropped, no privilege escalation, a read-only root
  filesystem. No service-account token is mounted.
- Probes on the health port: readiness on `/readyz`, withdrawn after one failed probe,
  and liveness on `/healthz`.
- Requests and limits. The broker derives its Go memory limit from `limits.memory`.
- A 60 s termination grace, sized as described in `docs/ARCHITECTURE.md`, "The example
  Kubernetes deployment". Raise it together with `MQTTNB_DRAIN_DELAY`.

`go test ./cmd/mqtt-nats-broker/` checks the tree against the binary, and
`make kubernetes-render` renders it with the pinned kustomize and checks the output.
