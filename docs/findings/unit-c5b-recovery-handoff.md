# C5b — displaced ordinary recovery handoff

Only the final ordinary reserve population of a completed synchronous register
and custody reconstruction can replace an old denial. Preparation leases the
exact instance, settles its handles, rereads the exact register/exchange pair,
and requires ordinary publish/Publish with no fact or action token. A busy or
unreadable candidate leaves the apply incomplete. ACK-first terminal pairs keep
C5a's terminal route.

The replacement is installed through the C3 installation API. Its source, PI
and server indexes exist before the old indexes are removed. It remains a
restriction, including against allowing callbacks and ordinary send admission.
Only the frame-local recovery floor capability permits enrollment alongside it.

| Creation / exit | Owner and restriction |
| --- | --- |
| Failed walk, busy owner, unresolved handle or unreadable pair | Original exact instance, evidence and unresolved handles remain; no transfer |
| Landed abandonment / ACK-first terminal | C5a source or terminal owner; never an ordinary recovery handoff |
| Ordinary pair, live source, fresh allow | New C3 recovery hold plus exact ordinary pending entry and custody |
| Ordinary pair, live source, fresh deny | New C3 recovery hold with B's independently obtained evidence; A's evidence is unchanged |
| Ordinary pair, drained durable | New C3 recovery hold; existing floor enrollment adds only eligible claims |
| Before record save | Hold blocks source ACK, callback dispatch and send admission, even with the floor present |
| Definite save failure | Remove only floor claims added by this apply; retain replacement restriction |
| Ambiguous save | Keep staged floor and replacement restriction; existing unresolved record save owns the bytes |
| Local positive save readback after refusal | Does not activate the hold or complete recovery; later successful resume is required |
| Landed save, later CONNECT refusal | Hold remains; no activation |
| Landed save and successful CONNECT, allow/drain | Remove hold; exact ordinary entry/custody and durably saved floor own normal admission |
| Landed save and successful CONNECT, fresh deny | Same newly installed instance becomes normal C3 denial work |
| Valid MQTT ending while held | Existing C3 worker may hand off to C4 or settle source; it cannot submit the staged ordinary denial |
| Generation retirement | Existing permit-bearing disposal discard |

The CONNECT accept transaction performs the classified save before CONNACK.
Activation occurs past the final CONNECT refusal, before the restore gate opens
and before post-accept drain recovery. No timer runs register recovery. No disposal
callback saves a floor, retries a generation, or clears global incompleteness.
The existing C3 timer remains solely an identity-bound cleanup continuation.

Proposed evidence: TestUnitCRecoveryHandoff covers T-C23, T-C25d-f, T-C32,
owner/index snapshots before/during/after transfer and before persistence,
allowing callback withholding, fresh-deny provenance, ordinary send admission,
and persisted-floor cold readback. TestUnitCRecoveryNonLandingCertificate exposes
the unreadable classifier and ACK-first exclusion directly. Existing
TestUnitCDisposalDisplacedRecovery and TestUnitCRecoveryRouting supply the
T-C25a-c displacement/landed routing controls. The hold remains until a later
successful synchronous resume on both failed-save branches.

No build, test, vet or mutation was run in consult 64. These are proposed tests,
not measured acceptance evidence. Consults 46/48's accepted residual remains;
this does not close F-105/F-106 or accept unit C.
