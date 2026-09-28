# Recovery reservation lifecycle: F-111 and C5

Consult 72 separates installation authority from lifecycle machinery.

C5 creates raw quarantine and resident demotion provenance only for refused
C5 pairs. Its local pair validation, ordinary fallback, terminal/drop handoff,
and complete-census absence path must dispose those owners. Absence is staged
before a successful CONNECT commits it; exclusion is installed before removal.
A local exact read cannot complete the custody census or register apply.

Raw ordinary recovery, marker reservations, and known recovered PUBREL
placeholders already exist at 7de0a61. Their general lifecycle repair belongs
to F-111. C5 preserves their constructors and marker replacement rule.
L1 and L2 have both old and C5-created cases; L3 is widened by C5; L4's
conflicting-resident attribution is introduced by C5.

C5 landed in 96d0a72. The F-111 lifecycle change landed after it, under its
own holistic review, full suite and route-checked mutation run, including the
unconfirmed-marker successor regression below.

The F-111 implementation extends the shared lifecycle machinery to ordinary
raw and marker reservations and tags known recovered PUBREL placeholders.
The latter participate in absence reconciliation but do not block delivery.
Ordinary opaque repair is covered by TestUnitCRecoveryOpaqueOrdinaryTwoPass;
the marker and known-PUBREL cases extend the owner tests. This extension
changes pre-C5 recovery behavior and is not attributed to C5 non-interference.

Marker-opaque constructors preserve the durable obligation's QoS: the landed
record's QoS after a commit, the occupant's QoS when replacement is withheld,
and the validated record's QoS during recovery. This completes resident identity
without granting send or settlement authority. TestConsult75UnconfirmedMarkerTerminalHandoff
uses the actual unconfirmed store path and a valid C5 terminal successor to
check repeated handoff, retained PI exclusion and resumed allocation.

The unified replacement predicate must not narrow the N-7.2 marker licence.
The reserve loop keeps its nonce-only marker exception independently of identity
completeness. TestMQTTInflightMarkerOpaqueReservation explicitly clears cached
QoS before ordinary resume so removal of that exception remains detectable.
