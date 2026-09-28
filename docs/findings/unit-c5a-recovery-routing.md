# Unit C5a recovery routing audit

Scope: plan-unitC-rev3 §7 routing and §7.2. C5b floor handoff is unchanged.
No generation retry owner or timer-invoked recovery is introduced.

| Creation / route | Continuation and exit |
|---|---|
| Valid fact + abandoned slot | C3 shared installation arms the exact disposal; `settleAbandonedDeliveryLocked` installs the ACK binding before removing the exact PUBLISH. Source confirmation removes only the disposal; a client ending removes the binding. |
| Valid fact + intended slot | C3 source owner/suppression plus existing intended reservation and owed materialization. Same-object resident reservations are enrolled too. No abandoned-only early exit. |
| Valid fact + ready slot | C3 source owner/suppression plus normal canonical PUBREL recovery. Source completion and MQTT completion remain independent. |
| Valid fact + pending-drain terminal | C3 source owner/suppression plus existing terminal reservation, token-holder drain and PUBCOMP cleanup. A live token is not completion. |
| Valid fact + Terminal/Rejected publish-terminal | C4 source/terminal owner via `holdTerminalSourceLocked`; existing C3 instances retain handles until their normal fenced handoff. No PUBLISH. |
| No fact + Terminal/Rejected, source Live/Unknown | Same C4 installation, using durable register reply. Existing `recoverPublishTerminalSlot` must establish the pair and source completion before its deletion gate. Its live-action early return now also has a previously installed source owner. |
| No fact + Terminal/Rejected, source Complete/Lost | Existing identity/output/freshness-fenced C4 deletion and owner discharge. No abandonment is invented. |
| Ended exchange, legitimately cleaned slot | Existing custody census observes the ending and removes an exact old ACK binding. No register route or new owner; historical fact remains durable. |
| Ordinary publish/Publish before landing | Ordinary recovery. An existing unlanded C3 instance, evidence and handles survive; successor floor/authorization handoff remains C5b. |
| Missing slot beside a fact-bearing unended exchange | Opaque reservation from the existing custody census; no inferred source completion, ACK binding or delivery. |
| Unreadable, unsupported or inconsistent pair | Opaque reservation; reused scheduling entries are restricted through the existing reservation API. The independent PUBREL scan cannot make this PI actionable. No new source-settlement owner or deletion authority. |
| Recovery exits after local owner installation | C3 registry and C4 retry/owed APIs retain their continuations. Exact local reads do not modify identifier/custody incompleteness or latest/completed-walk provenance. |

Consult 68 property: **C5 routing non-interference**. Only fact-bearing pairs
(metadata on either side) and ordinary terminal source carriers enter C5 routing.
The existing custody census selects exchange-only facts; selected pairs still need
fresh validation. Other records retain their pre-C5 route and read sequence.
Census uncertainty stays in custody; it does not invalidate a completed register
apply. An unresolved C5b handoff still withholds apply completion.

The C3 installation body is shared by live denial and validated recovery, preserving
the existing instance rather than replacing maps. C5-owned pairs receive a fresh
exchange point read; unowned ordinary registers receive no additional read.
Absent, failed, malformed and mismatched reads of owned pairs enter opaque routing;
none authorizes source settlement. C4 retains its independent deletion checks.

`TestUnitCRecoveryRouting` covers all rows on cold and same-object paths.
Post-landing abandonment uses a real commit; pre-landing retains the ordinary
durable pair. Terminal/rejected snapshots use the real ACK advance. PUBREL
successor snapshots isolate recovery routing and do not replace action/ACK
boundary tests. Local reconciliation has a separate incompleteness assertion.

Validation status: proposed diff only. No build, test or mutation run is claimed.
