// Package mqtt is the protocol core of the standalone MQTT 3.1.1 / 5.0 broker.
//
// The bulk of this package derives from the MQTT implementation in NATS server's
// internal `server` package (Apache-2.0): the shim_*/compat_* files, and the core
// derived from nats-server's server/mqtt.go — mqtt.go plus the files split from it
// along this broker's responsibilities, not upstream's layout: conn_disconnect.go,
// conn_lifecycle.go, connect_ack.go, connect_process.go, jsa.go, protocol_codec.go,
// protocol_output.go, protocol_properties.go, publish_ack.go, publish_deliver.go,
// publish_inbound.go, qos_pending.go, qos_track.go, retained_msgs.go, sess_clear.go,
// sess_evict.go, sess_expiry.go, sess_manager.go, sess_persist.go, sess_state.go,
// sub_consumer.go, sub_process.go, sub_rollback.go, subject_convert.go, testhooks.go,
// unsub_process.go and will_disposition.go. The WebSocket transport,
// transport_websocket.go and transport_websocket_upgrade.go, adapts nats-server's
// server/websocket.go. Each keeps the NATS Authors' header. This broker
// replaces NATS-server's internal runtime — the `client`/`Server`/`Account`
// machinery and the internal JetStream-assets transport — with the public nats.go
// client, so the broker can run as a stateless frontend against an external
// NATS/JetStream cluster.
//
// The behavioral shims live in shim_*.go and the vendored NATS helpers in
// compat_*.go; "The shim surface" in docs/ARCHITECTURE.md says what they stand
// for, and docs/PROVENANCE.md records the upstream source and the divergence. The
// upstream test suite (mqtt_test.go, mqtt_v5_test.go) runs behind the
// `mqtt_fulltests` build tag on an embedded-nats-server harness
// (fulltests_harness_test.go); its cluster and TLS cases are parked, unbuilt, behind
// `mqtt_cluster_reference` and `mqtt_tls_reference`. See CONTRIBUTING.md.
package mqtt
