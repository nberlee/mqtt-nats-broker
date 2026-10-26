# TLS

This page covers TLS on the broker's MQTT listeners. The broker's own connection to NATS
is configured by its NATS URL: a `tls://` URL is verified against the system CA bundle,
or against a private CA mounted and named by `SSL_CERT_FILE`
([details](ARCHITECTURE.md#the-runtime-image)).

## MQTT over TLS

Setting `-tls-cert` and `-tls-key` turns the MQTT listener into a TLS listener;
adding `-tls-ca` additionally requires and verifies client certificates (mTLS).
`-port` selects the MQTT-over-TCP listener in either mode, and there is no implicit
port change when the TLS flags are set: TLS deployments should pass `-port 8883` (the
conventional MQTT-over-TLS port). A TLS handshake must complete within 2 seconds.

TLS configuration fails closed: an unreadable or invalid certificate, key, or
client CA aborts startup before anything is bound, so a broken TLS setup can
never fall back to a plaintext listener. A certificate outside its validity
window counts as invalid.

## Plaintext beside TLS

To serve plaintext 1883 and TLS 8883 at once, while clients move to TLS, add
`-plain-port 1883` beside TLS. Credentials and payloads cross that port in clear, and no
client certificate is checked on it ([details](ARCHITECTURE.md#plaintext-mqtt-beside-tls)).
`-max-conns` counts the connections of both listeners together.

## Certificate rollover

Certificates roll over without a restart. The broker re-reads the three files
every `-tls-reload-interval` (default 10 s) and applies a complete, valid replacement to
new handshakes. Existing MQTT connections stay connected on the material they were
admitted under. An update that is incomplete, malformed, expired, or whose key does not
match its certificate is refused and logged, and the last valid configuration stays
active ([details](ARCHITECTURE.md#tls-material-reload)). The log records are in
[Troubleshooting](TROUBLESHOOTING.md#tls).

## Kubernetes Secrets

Mount the files from a Kubernetes Secret, for example one cert-manager renews, as a
volume: the kubelet updates it in place, so no Stakater Reloader or other rollout
controller is needed. A `subPath` mount never receives updates, so a renewal never
reaches the broker. The Helm chart (`mqtt.tls.enabled`, `mqtt.tls.secretName`) and the
example manifests' TLS overlay mount the Secret as a directory
([Kubernetes](KUBERNETES.md)).

## Secure WebSocket

With `-tls-cert`/`-tls-key` set, the WebSocket listener (`-ws-port`) serves secure
WebSocket (`wss://`) from the same material, reloaded with it. `-ws-no-tls` serves
plaintext WebSocket instead, for an ingress that terminates TLS; WebSocket clients then
get neither encryption nor client-certificate verification from the broker
([Configuration](CONFIGURATION.md#mqtt-over-websocket)).
