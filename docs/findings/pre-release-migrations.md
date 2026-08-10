# Pre-release persistence compatibility findings

This dossier records internal persistence paths that preserve development-era
formats even though the bridge has no released persisted-state contract. The
governing compatibility boundary is in `CONTRIBUTING.md` and
`docs/ARCHITECTURE.md`: before the first release, prefer one current schema and
actionable recreate guidance over migrations, dual readers, or mixed-version
protocols.

This audit does **not** apply to the roadmap's operator migration guide from NATS
Server's embedded MQTT listener. That guide describes cutover from a different
product and remains first-release documentation.

Every finding in this audit is now resolved and its record lives in the
resolved-findings archive: [F-056](resolved.md#f-056) (the retained-message format
and delete protocol), [F-057](resolved.md#f-057) (the reserved-subscription
migration) and [F-058](resolved.md#f-058) (the generationless delayed-Will outbox
record).
