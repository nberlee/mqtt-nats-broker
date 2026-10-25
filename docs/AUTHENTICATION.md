# Authentication and authorization

The broker holds no user database. NATS decides who an MQTT client is and what it
may do, and the broker enforces that decision on every route it owns. The
mechanism, and what it does not promise, is in
[Auth passthrough](ARCHITECTURE.md#auth-passthrough) and in
[Authorization of the authenticated identity](CONFORMANCE.md#authorization-of-the-authenticated-identity--closed-with-residuals).
This page is the operator's view. Moving from nats-server's own MQTT listener is in
[MIGRATION.md](MIGRATION.md).

## Three identities

**The service connection** is the broker's own. It writes the broker's state, so it
needs the whole `$MQTT.>` namespace and the JetStream API in its account. Give it
`-nats-user` and `-nats-pass`, or a `.creds` file with `-nats-creds`; never put
credentials in the `-nats` URL, which the broker refuses (a credential there would
become every MQTT client's identity). Keep it away from MQTT clients: anyone
presenting it acts with the broker's rights.

**Anonymous clients**, those that send no user name, connect to NATS with
`-default-user` and `-default-pass`. Without them the broker connects with no
credentials and the NATS server's own configuration decides; a server that requires
authentication refuses the client.

**Every other client** is checked by auth passthrough. Its MQTT user name and
password open an ordinary NATS client connection, and the broker asks the server
who that is (`$SYS.REQ.USER.INFO`): the account, and the publish and subscribe
permissions. The broker refuses the CONNECT unless that account is the service
connection's account, then holds the client to those permissions on every route,
including the ones that run over the service connection (stored QoS 1 and QoS 2
messages, retained messages, Wills). The permissions are read once, at CONNECT.

A user in another account authenticates to NATS but is refused, and what an
operator sees is a two-second delay and this log line, not an account mismatch:

```text
MQTT: refusing CONNECT for client "<client id>": $SYS.REQ.USER.INFO: nats: timeout
```

The server answers the identity request in the client's own account, and the broker
collects answers in the service account, so the answer never arrives. The account
comparison stands behind it.

With `-auth-passthrough=false` none of this happens: every client acts as the
service identity, and no per-client authorization exists.

## What does not authenticate through passthrough

Passthrough carries exactly a user name and a password, over a standard NATS client
connection. Credential forms that nats-server's own MQTT listener accepted do not
all carry over:

- **A password with no user name** is refused at CONNECT: MQTT 3.1.1 forbids it,
  and the broker answers MQTT 5 with CONNACK 0x81 (Malformed Packet), a stated
  deviation ([CONFORMANCE.md](CONFORMANCE.md#a-password-without-a-user-name)). So a
  token sent in the password field does not work.
- **A user JWT in the password** is passed on as an ordinary password. The server
  does not read it as a JWT; only an auth callout that chooses to accept that string
  would admit it.
- **A TLS client certificate** is verified when `-tls-ca` is set, and maps to no
  user. A client that presents a certificate and no user name is an anonymous
  client: it gets the `-default-user` identity and exactly that identity's rights,
  or, without one, whatever the server grants a connection with no credentials.
  Give the default identity the least it needs, or leave it unset against a server
  that requires authentication.
- **NKEY** authentication has no MQTT form.
- **A user whose `allowed_connection_types` lacks `STANDARD`**, for example one
  limited to `MQTT` for the embedded listener, is refused by the server.
- `-nats-creds` is the service connection's credential, not a client one.

## What every MQTT identity needs

- **Denied, publish and subscribe: `$MQTT.>`, `$JS.>`, `$KV.>` and `$O.>`.** An
  MQTT topic maps onto a NATS subject, so without these an MQTT client could forge
  the broker's state or, publishing to `$JS/API/STREAM/DELETE/$MQTT_sess`, delete
  its streams. The broker cannot check this for you; it is a deployment requirement
  ([What a deployment must allow](ARCHITECTURE.md#what-a-deployment-must-allow)).
- **Allowed to publish `$SYS.REQ.USER.INFO`.** A publish deny on `$SYS.>` refuses
  every CONNECT, and NATS evaluates deny before allow, so it cannot be carved back.
- **Subscribe permissions written for both levels of a `/#` filter.** In MQTT,
  `sensors/#` also matches `sensors` itself, so the broker subscribes to both
  `sensors` and `sensors.>`, and an allow list naming only `sensors.>` refuses the
  whole filter (SUBACK 0x80). nats-server's own listener maps filters the same way,
  so ACLs written for it should already carry both.

## Example: users and passwords

[`deploy/nats-auth/nats.conf`](deploy/nats-auth/nats.conf) is a complete server
configuration: one JetStream account `APP` with the service user, an anonymous user
that may publish and subscribe on `public/#` and nowhere else, a device that may
publish only its own sensor topics and read its own commands, and a dashboard that
reads every sensor and sends commands; and a second account `OTHER` whose user the
broker refuses. The passwords come from the
environment. Run the broker against it:

```sh
mqtt-nats-broker -nats nats://nats:4222 \
  -nats-user mqtt-broker -nats-pass "$SERVICE_PASSWORD" \
  -default-user anonymous -default-pass "$ANONYMOUS_PASSWORD"
```

`TestUserPasswordExampleAuthorizesPerUserTopics` (in `cmd/mqtt-nats-broker`) runs
the broker against this file and checks each promise: every identity is off the
reserved subjects, a device's publish to another device's topic and to the
JetStream API is refused with 0x87, a denied filter is refused beside a granted
one, and the `OTHER` user is refused although NATS authenticates it.

## Example: operator (JWT) mode with an auth callout

In operator mode the server trusts JWTs, and a user name and password mean nothing
to it on their own. The service connection uses a `.creds` file for a user of the
JetStream account. MQTT clients, which arrive with a user name and password, get an
identity only from an **account auth callout**: your service, which the server asks
to turn the credentials into a user JWT.

Accounts, all signed by the operator:

| Account | Holds |
|---|---|
| `APP` | JetStream, the broker's streams, the service user, and every MQTT user the callout issues |
| `AUTH` | The callout: `ExternalAuthorization` naming the responder's user public key in `AuthUsers` and `APP` in `AllowedAccounts`, optionally an `XKey` to encrypt requests |
| `SYS` | The system account |

The server configuration (JWTs and keys elided):

```text
operator: <operator JWT>
system_account: <SYS public key>
resolver: MEM
resolver_preload: {
  <SYS public key>: <SYS JWT>
  <APP public key>: <APP JWT>
  <AUTH public key>: <AUTH JWT>
}
# A bearer user JWT of AUTH with every publish and subscribe denied. A connection
# that presents no JWT, which is every connection the broker opens for an MQTT
# client, is treated as this user and so sent to AUTH's callout.
default_sentinel: <sentinel user JWT>
jetstream { limits { default_max_consumers: -1 } }
```

Who signs what:

| JWT | Signed by | Carries |
|---|---|---|
| The service user | `APP` (or a signing key of it) | nothing special; give it full rights in `APP` |
| The responder's user | `AUTH` | the identity your callout service connects with |
| The default sentinel | `AUTH` | `bearer_token`, publish and subscribe deny `>` |
| The authorization response | `AUTH` | the issued user JWT, or an error |
| The issued user | `APP` (or a signing key of it) | the MQTT user's permissions, which must include the denies above |

The responder receives each request on `$SYS.REQ.USER.AUTH` with the client's user
name and password, checks them against your own source of truth, and answers with
a user JWT for the request's user key. The broker then reads that user's account and
permissions through `$SYS.REQ.USER.INFO` like any other identity, so the account the
callout places a user in must be `APP`; a user placed elsewhere is refused.

What each key can do, and so what to protect:

- the responder's credentials let it answer for any connection sent to `AUTH`;
- `AUTH`'s key makes a response valid;
- `APP`'s key decides what an issued user may do, so whatever holds it must apply
  the denies above to every user it issues.

`TestOperatorModeAuthCallout` (in `cmd/mqtt-nats-broker`) builds this deployment
in-process and is the runnable reference. Its responder is a test fixture with a
fixed table of users, not an authorization service to deploy. The test checks that
the service starts from its `.creds` file (and does not without it), that a user is
admitted through the callout and held to the permissions it issued, publish and
subscribe, that an anonymous client gets the default identity through the callout,
that a user the callout places in another account is refused, and that a JWT sent as
the password reaches the callout as a password.

**Connection types in issued users.** nats-server 2.15.1 does not apply
`allowed_connection_types` to a user an auth callout issues: such a user limited to
`MQTT` is still admitted on a standard connection. Do not rely on that restriction in
a callout; see
[CALLOUT-CONN-TYPES](known-upstream-issues.md#callout-conn-types-an-auth-callout-issued-users-connection-types-are-not-applied).
Configuration-file users and directly presented JWTs are checked.

## Kubernetes

**Credentials in Secrets.** The example deployment in
[`deploy/kubernetes`](deploy/kubernetes/README.md) and the Helm chart in
`charts/mqtt-nats-broker` (`broker.credentials.secretName`) read the service user
and password, and the default identity, from one Secret:

```sh
kubectl create secret generic mqtt-nats-broker-nats \
  --from-literal=user=mqtt-broker --from-literal=password='<service password>' \
  --from-literal=default-user=anonymous --from-literal=default-password='<password>'
```

Both examples carry a user and password. For operator mode, mount a Secret holding
the `.creds` file and set `MQTTNB_NATS_CREDS` to its absolute path in place of the
user and password; the image sets no `HOME`, so the path cannot start with `~`.

**TLS from a Secret, renewed without a restart.** Put the certificate and key in a
`kubernetes.io/tls` Secret, which is what cert-manager issues, and mount it as a
directory volume: the TLS component of the example deployment
(`deploy/kubernetes/overlays/tls`) and the chart's `mqtt.tls.secretName` do. The
kubelet updates a directory mount in place when the Secret changes; a `subPath`
mount never sees an update. The broker re-reads the files every
`-tls-reload-interval` (10 s) and uses a complete, valid replacement for new
handshakes; connected clients keep the material they connected with, and an
incomplete, malformed, expired or mismatched replacement is refused and logged while
the last valid one stays in use
([TLS material reload](ARCHITECTURE.md#tls-material-reload)). To verify client
certificates, add the CA to the Secret and set `-tls-ca` (the chart's
`mqtt.tls.caKey`).

**Transport.** Passthrough credentials cross the MQTT listener. On a plaintext port,
including `-plain-port` beside TLS, user names and passwords travel in clear.
