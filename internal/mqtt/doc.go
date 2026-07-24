// Package mqtt is the protocol core of the standalone MQTT 3.1.1 / 5.0 broker.
//
// The bulk of this package (mqtt.go and the shim_*/compat_* files) is a port of the
// MQTT implementation from NATS server's internal `server` package (Apache-2.0). The
// port replaces NATS-server's internal runtime — the `client`/`Server`/`Account`
// machinery and the internal JetStream-assets transport — with the public nats.go
// client, so the broker can run as a stateless frontend against an external
// NATS/JetStream cluster.
//
// The behavioral shims live in shim_*.go and the vendored NATS helpers in
// compat_*.go; see docs/PORTING.md for how the port maps to upstream. The
// upstream regression/conformance test suite (mqtt_test.go, mqtt_v5_test.go,
// mqtt_ex_*) is retained behind the `mqttport_fulltests` build tag until its
// NATS test-server harness is ported.
package mqtt
