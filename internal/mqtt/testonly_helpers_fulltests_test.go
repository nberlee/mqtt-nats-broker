//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"fmt"
)

// ---- from handoff_outcome.go ----

// mqttHandoffClientFilter enumerates one client's handoff outcomes, and only those.
func mqttHandoffClientFilter(domainTk, cidHash string) string {
	return mqttHandoffSubjectPrefix + domainTk + cidHash + ".*"
}

// ---- from inflight_slot_classify.go ----

// mqttSlotDispositionNeedsMarker reports whether carrying out this verdict requires the
// occupant's unsent marker to be observed.
//
// Only the replacement arm does: reading the marker for an arm that does not need it is a
// wasted round trip, and failing to read it for the arm that does is the defect where a
// batch's shape gets taken for the store's answer. Test vocabulary (F-054): no production code
// calls it; the tests state the rule with it.
func mqttSlotDispositionNeedsMarker(d mqttSlotDisposition) bool {
	return d == mqttSlotDispReplace
}

// ---- from jsa.go ----

func (jsa *mqttJSA) updateStream(cfg *StreamConfig) (*StreamInfo, error) {
	cfgb, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	scri, err := jsa.newRequest(mqttJSAStreamUpdate, fmt.Sprintf(JSApiStreamUpdateT, cfg.Name), 0, cfgb)
	if err != nil {
		return nil, err
	}
	scr := scri.(*JSApiStreamUpdateResponse)
	return scr.StreamInfo, scr.ToError()
}

// ---- from outbound_continuation.go ----

// classifyOutstandingDuty reads the authoritative records for one outstanding duty and reports
// what it owes.
//
// It performs NO writes and acquires nothing. Every read failure is WITHHELD rather than
// resolved: a duty this broker cannot read is one it cannot conclude anything about, and the
// sweep's own rule — a missing or malformed entry is an inconsistency, never completed work —
// applies just as much to the records a duty points at as to the entry itself.
func (jsa *mqttJSA) classifyOutstandingDuty(id, epoch, gen string, d *mqttDutyEntry) mqttDutyVerdict {
	return jsa.classifyOutstandingDutyIn(id, epoch, gen, false, d)
}

// ---- from outbound_directory.go ----

// mqttRootCaughtUp reports that a sweep has examined everything allocated so far without
// resolving the generation.
//
// A DIFFERENT STATE FROM RETIRED, and naming it is the point: an OPEN generation whose sweep has
// finished is caught up, not completed. Its root stays discoverable and is revisited for growth
// or for sealing, and treating the two alike would remove a root that is still accruing work.
func mqttRootCaughtUp(r *mqttRecoveryRoot, led *mqttGenLedger) bool {
	return r != nil && led != nil && mqttSweepFinished(r) && !mqttRootRetires(r, led)
}

// sweepOneBatch reads up to mqttSweepBatchMax entries from a root's resume position and reports
// what it established.
//
// It RESOLVES NOTHING. Reading an entry cannot complete a duty — only the duty's own transition
// can — so this establishes which ordinals are still outstanding and hands them back. The caller
// is what acts on them, under whatever concurrency it chooses, and this stays a pure traversal
// that a test can pin without a work executor in the way.
func (jsa *mqttJSA) sweepOneBatch(r *mqttRecoveryRoot) ([]mqttSweptDuty, mqttSweepOutcome, error) {
	return jsa.sweepOneBatchCharged(r, nil, false)
}

// ---- from outbound_exchange.go ----

// mqttExchangeWalkFilter enumerates one generation's exchanges, and only those. Bounded by
// construction: one generation holds at most as many exchanges as it has outstanding
// obligations, and each names its own subject.
func mqttExchangeWalkFilter(idHash, gen string) string {
	return mqttF105SubjectPrefix() + mqttExchangeClassTk + "." + idHash + "." + gen + ".*"
}

// mqttExchangeAbandonmentEqual compares two facts by VALUE. A durable record is never compared
// by pointer identity: two reads of one record are two allocations.
func mqttExchangeAbandonmentEqual(a, b *mqttExchangeAbandonment) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// ---- from outbound_transition.go ----

// resolveTransition classifies one attempted transition from positive evidence only.
//
// The attempt is required, not optional: the outcome subject is named by a transition id
// alone, so a receipt found there proves only that SOME batch under that id committed. The
// caller states what it tried to do, and a standing outcome describing anything else is a
// contradiction rather than an answer.
//
// The authority read serves exactly one distinction — landed versus superseded — and never
// decides whether the attempt happened. Its three admissible shapes are: a strictly later
// revision (superseded), the same revision carrying the same ownership (landed), and
// anything else, which is an inconsistency this broker refuses to interpret. An authority
// at an EARLIER revision than a committed outcome is the sharpest of those: it cannot have
// preceded a transition that claims to have produced a later one.
func (jsa *mqttJSA) resolveTransition(t *mqttAuthorityTransition) mqttTransitionVerdict {
	if t == nil || t.Attempt == nil {
		return mqttTransitionUnresolved
	}
	want, derr := mqttTransitionMutationDigest(t)
	if derr != nil {
		return mqttTransitionUnresolved
	}
	return jsa.classifyAgainstOutcome(t.Attempt, want)
}

// ---- from sess_persist.go ----

// mqttAcceptOwnershipBatchSize is how many $MQTT_sess messages an accepted CONNECT commits
// once its accept save IS the outbound-ownership metadata commit (F-105 cutover): the permanent
// acceptance fact, the COMMITTED ownership receipt, and the session record, committed as ONE
// atomic three-member batch (handoff_metadata.go compose). A caller counting the record advance
// across a CONNECT accept observes this, not mqttAcceptBatchSize.
const mqttAcceptOwnershipBatchSize = 3

// ---- from transport_binding.go ----

// closeCert returns the established certificate, or nil.
//
// NIL IS NOT "STILL OPEN". It means no certificate has been established here, which is the
// state a broker is in for every connection it does not own. A caller that read nil and
// concluded the connection was alive would be inferring from an absence.
func (b *mqttConnBinding) closeCert() *mqttCloseCert {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cert
}

// mqttBindingCertifies reports whether cert is a certificate FOR this binding.
//
// A certificate names a run and a connection, and a caller holding one must check that it
// describes the connection whose output it is reasoning about. Accepting any certificate
// would let one connection's closure be read as another's — the cross-identity mistake this
// project has paid for repeatedly in other shapes.
func mqttBindingCertifies(b *mqttConnBinding, cert *mqttCloseCert) bool {
	return b != nil && cert != nil && cert.Run == b.run && cert.Conn == b.conn
}
