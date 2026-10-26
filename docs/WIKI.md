# mqtt-nats-broker

A standalone, horizontally scalable MQTT 3.1.1 and 5.0 broker that keeps sessions,
retained messages and QoS state in NATS JetStream.

This wiki is generated from the documentation under `docs/` in the source repository,
at the commit of a published release; the footer of every page names it. Between
releases the repository's documentation moves ahead of the wiki. Edits made in the wiki
itself are overwritten when the next release is published, so change the documentation
in the repository instead.

This page is `docs/WIKI.md`. Its list below decides which documents the wiki publishes,
in which order and under which titles; a document too large for one wiki page is
published as one page per section.

## Pages

- [Installation](INSTALLATION.md)
- [Configuration](CONFIGURATION.md)
- [TLS](TLS.md)
- [Kubernetes](KUBERNETES.md)
- [Helm chart](../charts/mqtt-nats-broker/README.md)
- [Kubernetes manifests](deploy/kubernetes/README.md)
- [Broker in the NATS chart's pods](deploy/nats-chart-shared-pod/README.md)
- [Authentication and authorization](AUTHENTICATION.md)
- [Migration](MIGRATION.md)
- [Operations](OPERATIONS.md)
- [Troubleshooting](TROUBLESHOOTING.md)
- [Architecture](ARCHITECTURE.md)
- [Conformance](CONFORMANCE.md)
- [Known upstream issues](known-upstream-issues.md)
- [Provenance](PROVENANCE.md)
