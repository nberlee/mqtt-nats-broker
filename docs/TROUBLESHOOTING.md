# Troubleshooting

Symptoms, what causes them, and what to change. Quoted text is what the broker logs or
prints, with the variable parts elided; its records are JSON on stdout
([Operations](OPERATIONS.md#logging)).

## The broker does not start

A startup failure prints `fatal: <reason>` on stderr and exits with status 1.

**A setting is refused.** The configuration is validated before anything starts, and the
reason names the setting:

| Message | Cause |
|---|---|
| `invalid MQTTNB_<NAME>: ...` | a malformed duration in the environment; also fatal when the flag is given too |
| `invalid command line: <kind> (-<flag>)` | an unknown flag, or a flag value that does not parse |
| `... URL #N (...) carries credentials, which would replace every auth-passthrough client's own; ...` | a NATS URL with a user or password in it; give the service identity with `-nats-user`/`-nats-pass` or `-nats-creds` |
| `nats (MQTTNB_NATS_URLS) does not apply with nats-embedded-config: ...` | `-nats` set together with the embedded topology |
| `tls-cert and tls-key must be set together`, `tls-ca requires tls-cert and tls-key` | an incomplete TLS configuration |
| `plain-port requires tls-cert and tls-key: ...` | `-plain-port` without TLS, where `-port` is already plaintext |
| `ws-host, ws-path, ws-handshake-timeout, ws-no-tls and ws-allowed-origins require ws-port` | a WebSocket setting without the WebSocket listener |
| `invalid js-domain (MQTTNB_JS_DOMAIN) ...: must be a single subject token, ...` | a JetStream domain with a dot, a wildcard or whitespace |
| `invalid connect-timeout ... (must be > 0)` | the CONNECT deadline cannot be disabled |
| `invalid log level ...` | anything but `debug`, `info`, `warn`, `error` |
| `invalid MQTT configuration: ...` | an option the MQTT core refuses, such as a WebSocket path that is not an escaped absolute path |

**The TLS material is invalid.** `MQTT TLS configuration: ...` names the file and the
problem: an expired or not-yet-valid certificate, a key that does not match the
certificate, a key file without exactly one private key, or text outside the PEM blocks.
The broker never falls back to plaintext ([TLS](TLS.md)).

**NATS or JetStream is not reachable.** `connect to NATS: ...` is the dial;
`JetStream not available on service connection: ...` means the connection works but
JetStream did not answer within 5 s: JetStream is off, the account has none, or
`-js-domain` names a domain the server does not serve.

**The service identity cannot ask who it is.** With auth passthrough the broker starts by
asking NATS for its own identity: `MQTT auth passthrough: identifying the service
connection: ...` means the service user is missing a permission from
[Recommended NATS permissions](ARCHITECTURE.md#recommended-nats-permissions), typically
the reply subjects under `$MQTT.>`.

**The broker-owned streams do not conform.** The message starts with
`start MQTT listener:` and names the stream and the reason, for example
`replicas is 1 (want 3)`, `storage is memory (want file)` or a `max_consumers` limit. The
streams are one unit and are not repaired in place: stop every broker for the account,
delete all ten streams, and start again, which discards the broker state
([Operations](OPERATIONS.md#the-ten-streams)). Two related refusals:

- `this account holds N of the 10 broker-owned streams ...`: a partial set that stayed
  partial for 30 s. If another broker is starting, wait for it and restart this one;
  only with every broker stopped, delete all ten. Migrating from nats-server's own MQTT
  listener leaves such a set ([Migration](MIGRATION.md)).
- `this account still holds $MQTT_origin, ...`: a stream an earlier build created;
  stop every broker and delete it.

**The embedded configuration is refused.** Messages start with
`embedded NATS configuration`: a variable the configuration references is unset, or
the file enables something the embedded topology does not allow (JetStream off, no
`store_dir`, a `default_max_consumers` other than `-1`, nats-server's own MQTT listener,
leaf nodes, an unauthenticated cluster route, `log_file`, a user with an empty
password) ([details](ARCHITECTURE.md#embedded-nats)).

## A client is refused at CONNECT

The broker answers a refused CONNECT with a CONNACK, then closes the connection:

| Situation | 3.1.1 | 5.0 | Reason String |
|---|---|---|---|
| NATS refused the client's user name and password | 0x05 | 0x86 | `bad user name or password` |
| NATS refused the default credentials of a client that sent no user name | 0x05 | 0x87 | `not authorized` |
| The client's identity is one the broker will not act on (another account, malformed, expired) | 0x05 | 0x87 | `not authorized` |
| Admission could not complete: the broker's own account is not established, the client's NATS connection dropped, or the identity request went unanswered | 0x03 | 0x88 | `server unavailable` |

The log says which: `MQTT: refusing CONNECT for client ...: <reason>`. A refused password
is logged only at `debug`, as `MQTT auth passthrough rejected client ...`.

- **0x03 / 0x88 for every client:** the client identities cannot publish to
  `$SYS.REQ.USER.INFO`. A publish deny on `$SYS.>` causes it, and cannot be carved back
  with an allow ([Authentication](AUTHENTICATION.md#what-every-mqtt-identity-needs)).
- **0x03 / 0x88 for some clients:** their identities belong to another NATS account,
  whose identity request goes unanswered. The broker serves one account.
- **A password without a user name** is refused: an MQTT 5.0 client gets 0x81, a 3.1.1
  client is disconnected.
- **0x05 / 0x87 with a Will:** the Will topic is in the reserved `$MQTT/` namespace.
- **No CONNACK at all:** the connection was over `-max-conns`, or it sent no CONNECT
  within `-connect-timeout`.
- **In the embedded topology, a wrong password** is answered 0x03 / 0x88 about 10 s
  late, a NATS server behavior (`docs/known-upstream-issues.md`).

## A publish or subscription is refused

- **SUBACK 0x80 on a `/#` filter:** in MQTT, `sensors/#` also matches `sensors`, so the
  broker subscribes to both `sensors` and `sensors.>`. An ACL allowing only `sensors.>`
  refuses the whole filter.
- **Topics under `$MQTT/`** are the broker's own. A SUBSCRIBE is refused with 0x80
  (`cannot subscribe to internal server subject`); a PUBLISH is dropped and logged
  (`Rejecting PUBLISH to reserved internal subject ...`), answered 0x87 to an MQTT 5.0
  QoS 1 or 2 publisher.
- **An MQTT 3.1.1 publisher is disconnected** when the client's NATS identity is denied
  the publish; 3.1.1 has no way to say why.

## SUBSCRIBE fails once a stream has 1000 consumers

New subscriptions are refused, a QoS 2 subscription or a session restore fails, while
everything that already exists keeps working. The JetStream error is
`JSMaximumConsumersLimitErr` (10026). nats-server 2.15 caps each stream at 1000
consumers unless every peer sets `jetstream { limits { default_max_consumers: -1 } }`,
or the account sets `max_consumers` above the working set; the stream's own
configuration looks right either way ([Installation](INSTALLATION.md#prerequisites)).

## Nothing is acknowledged

The broker starts and accepts publishes, but no QoS 1 or 2 message is ever
acknowledged: the service identity is allowed `$JS.ACK.<stream>.>` subjects for
particular streams instead of `$JS.ACK.>`. Grant `$JS.ACK.>`
([Recommended NATS permissions](ARCHITECTURE.md#recommended-nats-permissions)).

## Clients are disconnected

- **Reason 0x8E (session taken over):** another connection with the same client
  identifier connected, on this broker or another, over TCP or WebSocket.
- **Reason 0x8B (server shutting down):** the broker is stopping.
- **Without a reason:** the client's own NATS connection dropped (each client has one
  under auth passthrough), or the client could not take a write within 10 s.
- **Every client at once, with** `MQTT: the service connection reconnected into account
  ...`: the broker's own NATS connection came back in another account, so no identity
  checked against the old one stands.

## Broker state is lost when a NATS server fails

The streams were created with one replica: `-stream-replicas` was left at `0`, which
creates one-replica streams on a clustered NATS
([Configuration](CONFIGURATION.md#stream-replicas)). Check with
`nats stream info '$MQTT_sess'`. Moving to three replicas means recreating the ten
streams, which discards the broker state.

## TLS

- `MQTT TLS reload refused; keeping the active configuration`: the files on disk are
  incomplete, malformed, expired, or the key does not match; the last valid material
  stays in use and the files are read again every `-tls-reload-interval`. A restart
  while the files are invalid fails.
- `MQTT TLS certificate has expired and no valid replacement is on disk`: the expired
  certificate is still served. Renew it.
- A renewal never arrives: the Secret is mounted with `subPath`
  ([TLS](TLS.md#kubernetes-secrets)).
- `TLS handshake error on ... connection: ...`: a client failed the handshake, for
  example with an untrusted or missing client certificate under mTLS, or took longer
  than 2 s.

## WebSocket

A refused upgrade is answered with an HTTP status and logged at `error` as
`... MQTT over WebSocket: websocket upgrade refused (<status>): <reason>`:

| Status | Cause |
|---|---|
| 400 | the client did not offer the `mqtt` subprotocol (`mqttv3.1` alone is refused) |
| 403 | the page's origin is not the listener's own and not in `-ws-allowed-origins`; behind a TLS-terminating ingress, list the page origins |
| 404 | the request path is not `-ws-path` |

Browsers report close code 1006 for every close the broker makes.
