# Migrating from nats-server's MQTT listener

This guide moves an MQTT deployment from the listener built into nats-server (the
`mqtt {}` block of a server configuration) to this broker. Authentication and
authorization are covered in [AUTHENTICATION.md](AUTHENTICATION.md); how MQTT
topics map onto NATS subjects, and why `$MQTT.>` is reserved, is in
[Reserved `$MQTT.>` namespace](ARCHITECTURE.md#reserved-mqtt-namespace).

**The short version: no persisted MQTT state migrates.** Clients reconnect to the
broker with no session, nothing queued and no retained messages, and the
embedded listener's streams must be deleted before the broker can start in the
same account. A rollback restores service with empty MQTT state; it does not
bring back what the embedded listener held.

## What carries over

| State | Carried over? | What happens |
|---|---|---|
| Sessions and their subscriptions | No | A client connecting with Clean Session 0 (Clean Start 0 in MQTT 5) gets Session Present 0 and must subscribe again. A client that ignores Session Present keeps its connection and receives nothing. |
| Messages queued for offline sessions | No | Discarded with the embedded listener's streams. |
| QoS 1 and QoS 2 exchanges in flight, both directions | No | A client that resends an unacknowledged PUBLISH after reconnecting creates a new message at the broker, so a subscriber may see it twice. An outbound QoS 2 message the client had acknowledged with PUBREC is never released with PUBREL. |
| Retained messages | No | The broker does not read the old streams. An application that needs them must publish them again itself. |
| Wills | No | The embedded listener stores none. When a server shuts down it can publish the Will of each established client that did not send DISCONNECT; whether that delivery completes is not promised, and a server that dies abruptly attempts none. |
| Delayed Wills | Nothing to migrate | MQTT 3.1.1 has no Will Delay, and the embedded listener speaks only 3.1.1. A delayed Will exists only once an MQTT 5 client of this broker creates one. |
| Topic-to-subject mapping | Unchanged | `sensors/alice/t` is still the NATS subject `sensors.alice.t`, so core-NATS applications on mapped subjects keep working. |

## Why the account must be emptied first

The embedded listener keeps its state in five streams in each account it serves:
`$MQTT_sess`, `$MQTT_msgs`, `$MQTT_rmsgs`, `$MQTT_qos2in` and `$MQTT_out`. A server
creates them the first time it initializes MQTT for that account, which happens on
the first MQTT CONNECT that authenticates into it after the server starts. Every one
of those names is also one of the ten streams this broker keeps in the account, and
the ten are one unit: the broker starts on an account holding none of them (it
creates them) or all ten, and refuses anything in between (see
[The ten streams are one recreation unit](ARCHITECTURE.md#the-ten-streams-are-one-recreation-unit)).
`-js-domain` does not separate them: stream names carry no domain.

Started on an account the embedded listener has used, the broker rescans for 30 s
and then stops with:

```text
this account holds 5 of the 10 broker-owned streams — present: [$MQTT_sess $MQTT_msgs $MQTT_qos2in $MQTT_out $MQTT_rmsgs]; absent: [$MQTT_willq $MQTT_teardown $MQTT_qos2assign $MQTT_inflight $MQTT_shared] — and no rescan over 30s saw all 10. Another broker may still be provisioning this account: ...
```

During a migration that message has one meaning. The five "present" streams are
the embedded listener's, no other broker is provisioning anything, and waiting will
not change it: delete those five (step 5 below). The rest of the message, which
describes recovering this broker's own bundle, does not apply yet.

`TestEmbeddedListenerCutover` in `internal/mqtt` runs this whole sequence against a
real nats-server: the embedded listener creating its streams, the broker's refusal,
the restart without `mqtt {}`, the deletion, the broker's start, and Session
Present 0 for a client that had a session.

## Configuration

Every key the `mqtt {}` block accepts, and what replaces it. Broker flags each have
an `MQTTNB_` environment variable; see [Configuration](CONFIGURATION.md).

| `mqtt {}` key (aliases) | Broker | Notes |
|---|---|---|
| `listen`, `host` (`net`), `port` | `-host`, `-port` | The broker listens itself; clients move to its address. |
| `tls` | `-tls-cert`, `-tls-key`, `-tls-ca` | `-tls-ca` verifies client certificates and maps none to a user: `verify_and_map` has no equivalent (see [AUTHENTICATION.md](AUTHENTICATION.md#what-does-not-authenticate-through-passthrough)). No pinned certificates. Certificates reload without a restart. |
| `authorization` (`authentication`) | none | Users live in the NATS account and are checked by NATS through auth passthrough. A user, password or token that only the `mqtt {}` block knew has to become a NATS user. |
| `no_auth_user` | `-default-user`, `-default-pass` | Different mechanism: the broker connects to NATS with these credentials for a client that sends no user name. |
| `ack_wait` (`ackwait`) | none | |
| `max_ack_pending` (`max_pending`, `max_inflight`) | none | |
| `js_api_timeout` (`api_timeout`) | none | |
| `js_domain` | `-js-domain` | Baked into the broker's stream subjects: choose it before the first start. |
| `stream_replicas` | `-stream-replicas` | |
| `consumer_replicas` | none | nats-server ignores this key with a warning. The broker has no consumer replica setting either: its durable consumers inherit their stream's replica count, which JetStream requires on their interest-retention streams ([High availability](ARCHITECTURE.md#high-availability)). Drop `-consumer-replicas` from the launch arguments, where it now fails startup, and `MQTTNB_CONSUMER_REPLICAS` from the environment, where it is ignored; neither ever changed replication. |
| `consumer_memory_storage` | none | |
| `consumer_inactive_threshold` (`consumer_auto_cleanup`) | none | The broker manages its consumers' lifetimes and refuses a stream-level inactivity limit where it would delete a consumer it still needs ([Stream prerequisites](ARCHITECTURE.md#stream-prerequisites)). |
| `reject_qos2_publish`, `downgrade_qos2_subscribe` | none | QoS 2 is supported. A shared subscription asking for QoS 2 is granted QoS 1. |

## Before the downtime

Check these while the embedded listener still serves; each one found late extends
the outage.

- **nats-server 2.15.1 or newer on every peer**, and
  `jetstream { limits { default_max_consumers: -1 } }` on every peer (see
  [Installation](INSTALLATION.md#prerequisites)).
- **Every MQTT identity works as a standard NATS client, in the service's
  account.** The broker connects to NATS with each client's user name and password
  as an ordinary client connection, then asks the server who it is. Check each
  identity the same way, with the NATS CLI:

  ```sh
  nats --server nats://nats:4222 --user alice --password '<password>' \
    request '$SYS.REQ.USER.INFO' ''
  ```

  The reply's `account` must be the account of the broker's service user. A user
  whose `allowed_connection_types` lacks `STANDARD`, or a credential form other than
  a user name and password, does not authenticate through the broker; see
  [What does not authenticate through passthrough](AUTHENTICATION.md#what-does-not-authenticate-through-passthrough).
- **The client ACL**: every MQTT identity denied `$MQTT.>`, `$JS.>`, `$KV.>` and
  `$O.>` in both directions ([AUTHENTICATION.md](AUTHENTICATION.md)).
- **Certificates**: the broker's TLS certificate must name the host name clients
  will use after the switch.
- **Clients**: anything that does not subscribe again on Session Present 0 stops
  receiving after the cutover.

## Cutover in the same account

The embedded listener and the broker cannot serve one account at the same time, so
this is a cutover with downtime. Steps in order; each states what it depends on.

1. **Prepare the broker, but do not start it.** Create its Secrets and keep its
   manifests or chart values ready; apply them only at step 6. Nothing must start a
   broker early: one started now refuses the account (above), and a restart
   controller will keep retrying it.
2. **Point of no return: fence the old endpoint.** Take the embedded listener's
   address out of the load balancer or DNS, so clients stop reconnecting to servers
   about to restart. Up to here, nothing has changed. From the next step on, this
   guide promises no way back to the embedded listener's state: restarting a server
   without `mqtt {}` can already lose some of it (consumers kept in memory with
   `consumer_memory_storage`, or deleted after `consumer_inactive_threshold`), and
   step 5 discards the rest: every session, queued message, exchange in flight and
   retained message. If you need a copy, take it now (`nats stream backup` of the
   five streams); this guide does not test restoring one.
3. **Restart every nats-server without the `mqtt {}` block.** Removing it is a
   restart, not a configuration reload: nats-server refuses to reload a change to
   that block. Restart one server at a time and let each rejoin the cluster and
   JetStream catch up before the next. Each restart disconnects that server's MQTT
   clients, who may reconnect to a server not yet restarted, so disconnects, Wills
   and reconnects can repeat until the last server is done. Hold off alerting that
   is driven by Wills for this window.
4. **Confirm** that no server is configured with `mqtt {}` any more and no MQTT
   client is connected.
5. **Delete the five streams**, in the right account (and JetStream domain, if you
   use one). Quote the names, or the shell expands `$MQTT_sess`:

   ```sh
   for s in '$MQTT_sess' '$MQTT_msgs' '$MQTT_rmsgs' '$MQTT_qos2in' '$MQTT_out'; do
     nats --context <account-context> stream rm -f "$s"
   done
   ```

6. **Start the brokers** and wait for `/readyz` on each. The first to start creates
   the ten streams; the others adopt them.
7. **Move clients**: point DNS or the load balancer at the broker. How fast clients
   follow depends on their reconnect back-off, cached resolutions and long-lived
   connections as much as on a DNS TTL.
8. **Verify** (below).

### If it stops part-way

| Stopped | State | Recovery |
|---|---|---|
| Before step 3 | Nothing has changed. | Put the old endpoint back. |
| During step 3 or 4 | Some or all servers run without `mqtt {}`. The five streams still exist; how much of their state still serves clients is not promised. | Either continue, or restore `mqtt {}` on every server and accept what the embedded listener resumes. |
| During step 5 | Some of the five are deleted; the old state is partly gone. | Delete the rest, with every server still without `mqtt {}`, and continue. |
| During step 6 | A broker stopped or failed while creating its ten streams; any subset may exist. | Stop every broker, restart controllers included, list the `$MQTT_*` streams, and if the set is incomplete delete all of them and start again. This is the broker's own whole-bundle reset ([The ten streams are one recreation unit](ARCHITECTURE.md#the-ten-streams-are-one-recreation-unit)). |
| After step 6 | The broker holds new state. | Going back is a rollback, below. |

## Cutover into a fresh account

To run both at once, put the broker's service user and its MQTT users in a new
account. Nothing collides, and clients can move gradually. Subjects and users
belong to an account, though, so the two MQTT populations do not see each other's
messages, and every MQTT user is created again in the new account. This guide gives
no recipe for bridging the two accounts.

## Verify

- `/readyz` answers 200 on every broker.
- The account holds ten `$MQTT_*` streams (`nats stream ls`).
- An MQTT client round-trips a QoS 1 message, sets a retained message and reads it
  back, and on reconnecting with a persistent session gets Session Present 1.
- The client ACL holds, checked without harming anything: an MQTT 5 client that
  publishes at QoS 1 to `$JS/API/INFO` and to `$MQTT/x` gets PUBACK reason 0x87 (Not
  authorized), while the same client publishing to one of its own topics gets
  0x00. An MQTT 3.1.1 client cannot show this: the broker closes its connection for
  a denied publish, and acknowledges a `$MQTT/` publish without acting on it.
  `TestUserPasswordExampleAuthorizesPerUserTopics` runs this check.

## Rollback

A rollback restores service through the embedded listener with **empty MQTT
state**:

1. Stop every broker, and whatever would restart them.
2. Delete all ten broker streams. This discards everything the broker accepted
   since the cutover, delayed Wills included. Never start the embedded listener on
   any of them, a partial set included: nats-server uses existing streams of its
   five names as it finds them, and would read records it did not write.
3. Put `mqtt {}` back and restart the servers one at a time.
4. Point DNS or the load balancer back.

With a backup taken at step 2 of the cutover, the five old streams could be restored
before step 3 here; that is untested.
