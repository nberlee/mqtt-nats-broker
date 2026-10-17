// Package mqtt is the protocol core of the standalone MQTT 3.1.1 / 5.0 broker.
//
// The bulk of this package is a port of the MQTT implementation from NATS server's
// internal `server` package (Apache-2.0): the shim_*/compat_* files, and the core
// derived from nats-server's server/mqtt.go — mqtt.go plus the files split from it
// along this broker's responsibilities, not upstream's layout: conn_disconnect.go,
// conn_lifecycle.go, connect_ack.go, connect_process.go, jsa.go, protocol_codec.go,
// protocol_output.go, protocol_properties.go, publish_ack.go, publish_deliver.go,
// publish_inbound.go, qos_pending.go, qos_track.go, retained_msgs.go, sess_clear.go,
// sess_evict.go, sess_expiry.go, sess_manager.go, sess_persist.go, sess_state.go,
// sub_consumer.go, sub_process.go, sub_rollback.go, subject_convert.go, testhooks.go,
// unsub_process.go and will_disposition.go. Each keeps the NATS Authors' header. The
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
