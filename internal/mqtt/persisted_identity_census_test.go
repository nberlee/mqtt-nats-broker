// The persisted-identity census, as a drift guard.
//
// F-034 inherits the cross-stream identity enumeration F-033 deliberately did not
// attempt. Four earlier attempts were each falsified by review, and every one failed
// the same way: it was a FILTER presented as a sweep — the streams already known, the
// `json:` tags, a keyword grep, a hand-listed carrier set. Two of them were refuted by
// an entry in the table they themselves introduced.
//
// So the enumeration in docs/findings/persisted-identity-census.md is not trusted to
// stay complete on care alone. This file is what makes it fail loudly instead: every
// field of every persisted artifact type carries exactly one disposition, derived by
// REFLECTION rather than listed by hand, so a dependency bump or a new field breaks the
// bijection by name.
//
// Three properties are load-bearing, and each exists because a specific way of getting
// this wrong is already on the record:
//
//  1. THE MANIFEST IS KEYED BY TYPE, NOT BY PATH. ConsumerConfig is 35 fields, and
//     mqttPersistedSession reaches it at FOUR sites (Cons, PubRel, Q2Aff, Drains.Cons)
//     with mqttPersistedDrain reaching it at a fifth. Path-keying would therefore carry
//     140 ConsumerConfig rows inside that one record where type-keying carries 35 — 105
//     of them duplicates, and five chances for the copies to disagree. A named type is
//     dispositioned ONCE and every embedding site inherits it; the site is itself a row
//     asserting which type it embeds.
//
//  2. A `json:` TAG IS NOT THE TEST OF PERSISTENCE. mqttRetainedMsg carries NO json tags
//     at all: since F-056 removed the legacy reader it is persisted as headers, its
//     `expires` field is persisted and UNEXPORTED, and two of its exported fields are not
//     persisted at all. A tag-driven walk sees one of the nine streams as having no fields
//     whatsoever. So the walk covers unexported fields and records each field's ENCODING.
//
//  3. A CONTAINER MAY NOT BE A LEAF. Go reflection does not recurse into identities
//     carried by map KEYS, slice membership, or an encoded []byte. `Cons` and `Drains`
//     keys are sids; `Props` is a structured blob with its own decoder; `Msg` is opaque
//     application payload. A map, slice or []byte field must declare which of those it is
//     and what its keys and elements carry.
//
// See docs/findings/persisted-identity-census.md for the census itself, and
// docs/findings/ha-arbitration.md#f-034 for what the audit established.

package mqtt

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// censusKind is what a field IS, for the purposes of cross-stream identity. Every
// field takes exactly one, including the ones that carry no identity — "not an
// identity" is a verdict the census records, not a reason to omit a row.
type censusKind int

const (
	// identityLocal: minted and interpreted entirely within this artifact's own
	// lifecycle. Recreating another stream cannot invalidate it.
	identityLocal censusKind = iota
	// identityCrossStream: issued by another stream, so recreating that stream
	// strands this reference. issuer names it.
	identityCrossStream
	// identityComposite: neither this field nor its partner identifies the thing
	// alone. composite names the binding, which the independent oracle below must
	// also carry.
	identityComposite
	// notAnIdentity: payload, flags, timestamps, counters.
	notAnIdentity
	// embeddedType: a struct or pointer-to-struct whose own fields are dispositioned
	// under embeds, once, wherever it appears.
	embeddedType
	// container: a map or slice whose keys and/or elements carry something the
	// walk cannot see.
	container
	// structuredBlob: a []byte with an interpretable structure; decoder names what
	// reads it.
	structuredBlob
	// opaquePayload: a []byte the bridge never interprets. Deliberately distinct
	// from structuredBlob — demanding a decoder for application bytes would invite
	// a fictional one.
	opaquePayload
)

// String makes a failure message name the kind rather than print its ordinal. These
// diagnostics ARE the audit's user interface — the mutation runs that pin this file
// printed "dispositioned 1" and "dispositioned 3", which tells a future maintainer
// nothing about what the census thought the field was.
func (k censusKind) String() string {
	switch k {
	case identityLocal:
		return "identityLocal"
	case identityCrossStream:
		return "identityCrossStream"
	case identityComposite:
		return "identityComposite"
	case notAnIdentity:
		return "notAnIdentity"
	case embeddedType:
		return "embeddedType"
	case container:
		return "container"
	case structuredBlob:
		return "structuredBlob"
	case opaquePayload:
		return "opaquePayload"
	}
	return fmt.Sprintf("censusKind(%d)", int(k))
}

// censusEncoding is HOW a field reaches durable storage. It is a separate axis from
// censusKind because mqttRetainedMsg proves the two are independent: a field can be
// an identity carried in a header, or an identity carried in the message envelope,
// or exported and not persisted at all.
type censusEncoding int

const (
	encJSON           censusEncoding = iota // a json body field
	encHeader                               // a NATS/Nmqtt-* header on the stored message
	encEnvelope                             // derived on read from the message's own subject
	encEncodedIgnored                       // written by the encoder, ignored by the decoder
	encNotPersisted                         // in-memory only
	// encRawBody is the stored message's BODY, written and read as raw bytes — neither a
	// json field nor a header. Distinct from encJSON because it is reached by no JSON
	// encoder at all: review found mqttRetainedMsg.Msg dispositioned encJSON while
	// mqttEncodeRetainedMessage appends it verbatim after the header block and
	// mqttDecodeRetainedMessage assigns the raw NATS body straight into it. The row's own
	// prose already said "the message BODY", so the encoding contradicted its reason and
	// nothing checked.
	encRawBody
)

func (e censusEncoding) String() string {
	switch e {
	case encJSON:
		return "encJSON"
	case encHeader:
		return "encHeader"
	case encEnvelope:
		return "encEnvelope"
	case encEncodedIgnored:
		return "encEncodedIgnored"
	case encNotPersisted:
		return "encNotPersisted"
	case encRawBody:
		return "encRawBody"
	}
	return fmt.Sprintf("censusEncoding(%d)", int(e))
}

// censusBespokeEncoders names the persisted types whose wire form is written by a NAMED
// encoder rather than by encoding/json, and the decoder that reads it back.
//
// The point of the map is a rule the reflection tests can enforce without a second
// manifest: for a type in here, `encJSON` is FORBIDDEN outright, because the body this type
// produces is not JSON and no exportedness check can tell the difference. That is what let
// mqttRetainedMsg.Msg sit as encJSON — the field is exported, so the only rule in force
// passed it.
var censusBespokeEncoders = map[string]struct{ encoder, decoder string }{
	"mqttRetainedMsg": {encoder: "mqttEncodeRetainedMessage", decoder: "mqttDecodeRetainedMessage"},
}

type censusDisposition struct {
	kind     censusKind
	encoding censusEncoding
	issuer   string // identityCrossStream: the stream that issues the value
	compos   string // identityComposite: the binding's name
	embeds   string // embeddedType: the named type carrying the fields
	keys     string // container: what the keys carry
	elems    string // container: what the elements carry
	// lifecycle is the stream whose RECREATION invalidates this value, when that is
	// not whoever minted it.
	//
	// Review of the first implementation found the model could not express
	// ConsumerConfig.Durable: the bridge MINTS the name before the create, so no stream
	// issues it — but the consumer it names lives on a stream, and recreating that
	// stream strands the name. Calling it identityLocal contradicted identityLocal's own
	// definition, and the F-066 oracle accepted it anyway because it only checked
	// issuers. Minting authority and lifecycle namespace are two different questions and
	// now have two different fields.
	lifecycle string
	// elemIssuer is set when a container's ELEMENTS are themselves cross-stream
	// references. Added because the F-066 oracle pass found the model could not
	// express CancelledRegs — a slice of $MQTT_inflight register nonces — which is a
	// cross-stream reference the container disposition alone would have hidden.
	elemIssuer string
	decoder    string // structuredBlob: what interprets it
	// reason is the audit. Enforced non-empty for every disposition, on the same
	// reasoning as the stream validators' waiver prose.
	reason string
}

// censusComposite is one binding, in an oracle kept DELIBERATELY INDEPENDENT of the
// manifest above. Round 5 of the plan review showed why: if the oracle were derived
// from the same literal as the dispositions, a mutation that changed both members of
// a composite would move the oracle with them and pass vacuously. So this table
// names its members by hand and the test cross-checks the two.
type censusComposite struct {
	kind    string // what the binding IS, since not all composites are alike
	typ     string // the type carrying the members
	members []string
	reason  string
}

// censusComposites is that oracle. The kinds are distinct on purpose: round 2 of the
// review found a single "reciprocal pair" category obscured that (Dur, Seq) and
// (Q2AffClaim, Q2AffCopy) protect different things.
var censusComposites = []censusComposite{
	{
		kind:    "source-delivery key",
		typ:     "mqttInflightRecord",
		members: []string{"Dur", "Seq"},
		reason: "Dur says WHICH consumer delivery of a stream message is meant, and it also selects " +
			"which stream's sequence space Seq lives in — mqttInflightSourceStream maps the affine " +
			"durable prefix to $MQTT_out and everything else to $MQTT_msgs, so a bare Seq names no " +
			"sequence space at all",
	},
	{
		kind:    "cross-artifact binding",
		typ:     "mqttInflightRecord",
		members: []string{"Q2AffClaim", "Q2AffCopy"},
		reason: "F-065 established neither field alone identifies the delivery; the binding is what " +
			"stops a claim on $MQTT_qos2assign being misattributed to a copy on $MQTT_out",
	},
	{
		kind:    "arbitration relation",
		typ:     "mqttInflightRecord",
		members: []string{"Claim", "ClaimSeq", "ActionClaim", "ActionClaimSeq"},
		reason: "Claim identifies the session OBJECT and is minted in process, while ClaimSeq orders " +
			"rival claims by a $MQTT_sess sequence. Either alone cannot arbitrate: the nonce says who " +
			"but not which is newer, and the sequence says which is newer but not whose. " +
			"ActionClaim/ActionClaimSeq are members of this SAME relation rather than a second one: " +
			"they record which custody acquired the live action, and the shape validator requires " +
			"them to equal Claim/ClaimSeq exactly, so an action that outlives a custody rewrite is " +
			"refused as an illegal shape rather than silently arbitrated",
	},
	{
		kind:    "origin interval",
		typ:     "mqttWillOutbox",
		members: []string{"OriginSessionSeq", "OriginSessEpoch"},
		reason: "an mseq is only comparable within the $MQTT_sess incarnation that issued it, so the " +
			"epoch is what makes the sequence mean anything — this is one of the three ad hoc " +
			"couplings F-066 names",
	},
	{
		kind:    "scan interval",
		typ:     "mqttWillOutbox",
		members: []string{"ScanFrom", "ScanEpoch"},
		reason: "the second of $MQTT_willq's two couplings to $MQTT_sess, same reasoning as the origin " +
			"interval and dispositioned separately because the two intervals are independent",
	},
}

// censusManifest is the audited disposition of every field of every persisted
// artifact type, keyed by TYPE then FIELD. See property 1 in the file header for why
// it is not keyed by path.
func censusManifest() map[string]map[string]censusDisposition {
	m := map[string]map[string]censusDisposition{}

	m["mqttInflightRecord"] = map[string]censusDisposition{
		"PI":  {kind: identityLocal, encoding: encJSON, reason: "the packet identifier, authored by the session/client exchange; the register subject also carries it, but the subject is a carrier and the exchange is the authority"},
		"Dur": {kind: identityComposite, compos: "source-delivery key", encoding: encJSON, reason: "see the composite oracle: paired with Seq"},
		"Seq": {kind: identityComposite, compos: "source-delivery key", encoding: encJSON, reason: "see the composite oracle: paired with Dur"},
		"QoS": {kind: notAnIdentity, encoding: encJSON, reason: "the delivery's QoS, a protocol property of the exchange rather than a reference to anything"},
		"Nonce": {kind: identityLocal, encoding: encJSON,
			reason: "minted by mqttNewInflightNonce at allocation and never rewritten; this is what proves a landed write is ours, since the subject is reused within an incarnation (R4)"},
		"Claim":      {kind: identityComposite, compos: "arbitration relation", encoding: encJSON, reason: "see the composite oracle: paired with ClaimSeq"},
		"ClaimSeq":   {kind: identityComposite, compos: "arbitration relation", encoding: encJSON, reason: "see the composite oracle: paired with Claim"},
		"Q2AffClaim": {kind: identityComposite, compos: "cross-artifact binding", encoding: encJSON, reason: "see the composite oracle: paired with Q2AffCopy"},
		"Q2AffCopy":  {kind: identityComposite, compos: "cross-artifact binding", encoding: encJSON, reason: "see the composite oracle: paired with Q2AffClaim"},
		"Retired":    {kind: notAnIdentity, encoding: encJSON, reason: "a fact about this exchange's source having been retired; names nothing"},
		"Cancelled":  {kind: notAnIdentity, encoding: encJSON, reason: "a fact about this allocation never having been transmitted; names nothing"},
		"State": {kind: notAnIdentity, encoding: encJSON,
			reason: "the exchange's PHASE, which says who may act on this slot rather than naming anything. Mandatory and never defaulted: an unknown state is refused, because reading it as PUBLISH would license a PUBLISH under an identifier already past PUBREC"},
		"ActionToken": {kind: identityLocal, encoding: encJSON,
			reason: "the live PUBREL action's licence, minted by the acquiring holder and never rewritten. " +
				"Local because it names nothing outside this record: it is compared for EQUALITY by the " +
				"holder draining it and by nobody else, which is what stops one broker retiring another's " +
				"action after merely observing that one exists. There is deliberately no expiry — a lease " +
				"cannot help, since a broker may pause after its own expiry check and resume into the same " +
				"stale write, and an MQTT client cannot validate a fencing token"},
		"ActionClaim": {kind: identityComposite, compos: "arbitration relation", encoding: encJSON,
			reason: "the custody the action was acquired under; MUST equal Claim, so it is the same " +
				"arbitration relation rather than a second one. A mismatch means one of the two was " +
				"rewritten underneath the other"},
		"ActionClaimSeq": {kind: identityComposite, compos: "arbitration relation", encoding: encJSON,
			reason: "paired with ActionClaim and required to equal ClaimSeq. An ORDERING sequence, NOT an " +
				"acceptance sequence: ClaimSeq is sess.seq snapshotted at store time and sess.seq is " +
				"replaced by every confirmed save, so no comparison against a current or original " +
				"acceptance can be made to work. Current ownership is proved at admission by Claim " +
				"equality against the constructor-minted regClaim; recovery installs that claim only " +
				"after the session acceptance/ordering decision succeeds"},
		"PubRelDurable": {kind: identityCrossStream, issuer: mqttOutStreamName, encoding: encJSON,
			reason: "the PUBREL consumer durable that delivers the canonical record, on $MQTT_out. " +
				"Dispositioned as a cross-stream reference rather than half a composite, following " +
				"mqttQ2AffClaim.CopySubject/AffDurable: the F-066 row carries the pairing, and each " +
				"field is independently a reference into another stream's naming space. It is also " +
				"what makes a record from a retired generation recognisable as belonging to an " +
				"exchange that is gone, since the next incarnation uses a different PUBREL subject"},
		"PubRelStreamSeq": {kind: identityCrossStream, issuer: mqttOutStreamName, encoding: encJSON,
			reason: "an absolute $MQTT_out sequence naming the CANONICAL PUBREL record, so recreating " +
				"that stream strands it — the same hazard class as mqttPersistedDrainSeq.RegSeq. It is " +
				"taken from the stored message or an exact readback and NEVER from Nats-Msg-Id, a drain " +
				"attempt counter or the source sequence. Under F-034 it may not by itself select or " +
				"authorize a destructive operation: the slot subject and the exchange nonce do that, and " +
				"this pair identifies WHICH record the exchange is about"},
	}

	m["mqttInflightMarker"] = map[string]censusDisposition{
		"Nonce": {kind: identityLocal, encoding: encJSON,
			reason: "the register nonce this UNSENT MARKER names (F-072): the marker is the sibling of " +
				"$MQTT.inflight.<hash>.<gen>.<pi>, present iff nothing was transmitted under that " +
				"identifier, and its body carries only the register's own nonce so a stale marker cannot " +
				"be attributed to a later exchange on the reused subject. Deleted at the transmission; " +
				"never rewritten"},
	}

	m["mqttPersistedDrainSeq"] = map[string]censusDisposition{
		"Seq":     {kind: identityCrossStream, issuer: mqttStreamName, encoding: encJSON, reason: "the source stream sequence this drained delivery came from; recreating $MQTT_msgs resets that space"},
		"PubRel":  {kind: notAnIdentity, encoding: encJSON, reason: "the QoS 2 second phase's intent, written before the PUBREL is stored so recovery is positive rather than a guess"},
		"PI":      {kind: identityLocal, encoding: encJSON, reason: "the packet identifier, and since F-034 also what DERIVES the register subject the release addresses — see the drain release in inflight_register.go"},
		"Attempt": {kind: notAnIdentity, encoding: encJSON, reason: "the deduplication attempt counter; a dedupe entry outlives its message, so a verified-absent recreate bumps this rather than trusting the window"},
		"OwnerNonce": {kind: identityLocal, encoding: encJSON,
			reason: "the EXCHANGE identity this drained delivery owns, persisted with the intent and before the PUBREL store. Not redundant with the record's own header: recovery may find the record missing and have to recreate it, and minting a replacement nonce there would create a DIFFERENT exchange that the durable owner slot does not name and could therefore never act"},
		// The F-066 oracle pass corrected this. An earlier draft called it
		// notAnIdentity on the ground that F-034 demoted it to a hint — but demoting
		// what a mechanism RELIES on does not stop the field being a cross-stream
		// reference, and F-066's table records it as one. Both facts belong in the row.
		"RegSeq": {kind: identityCrossStream, issuer: mqttInflightStreamName, encoding: encJSON,
			reason: "an absolute $MQTT_inflight sequence, so recreating that stream strands it — which is " +
				"exactly the V1 defect F-034 closed, since the drain's release used to ADDRESS its delete " +
				"by this value and would then destroy an unrelated register at the reused sequence. " +
				"F-034 demotes it to a HINT: it may never select or authorize a deletion, and the subject " +
				"derived from PI does that instead. It remains a cross-stream reference either way"},
	}

	m["mqttPersistedDrain"] = map[string]censusDisposition{
		"Cons":    {kind: embeddedType, embeds: "ConsumerConfig", encoding: encJSON, reason: "the surviving durable's config; dispositioned once under ConsumerConfig"},
		"Subject": {kind: notAnIdentity, encoding: encJSON, reason: "the NATS subject the subscription delivered on, kept because ConsumerConfig carries no MQTT delivery metadata"},
		"QoS":     {kind: notAnIdentity, encoding: encJSON, reason: "MQTT delivery metadata"},
		"NoLocal": {kind: notAnIdentity, encoding: encJSON, reason: "MQTT delivery metadata"},
		"RAP":     {kind: notAnIdentity, encoding: encJSON, reason: "MQTT delivery metadata"},
		"SubID":   {kind: notAnIdentity, encoding: encJSON, reason: "MQTT subscription identifier, client-scoped"},
		"Started": {kind: container, elems: "mqttPersistedDrainSeq", encoding: encJSON, reason: "one entry per started delivery; the elements carry the identities and are dispositioned under their own type"},
	}

	m["mqttGenRetirement"] = map[string]censusDisposition{
		"ID":  {kind: notAnIdentity, encoding: encJSON, reason: "the client ID, for diagnostics; the subject is what addresses the watermark"},
		"Gen": {kind: identityCrossStream, issuer: mqttSessStreamName, encoding: encJSON, reason: "the retired incarnation generation, issued with the session record"},
		"MarkerSeq": {kind: identityCrossStream, issuer: mqttSessStreamName, encoding: encJSON,
			reason: "the $MQTT_sess sequence this teardown's claim marker landed at, and the ONLY thing " +
				"the advance is decided by. Comparable only within the incarnation epoch $MQTT_teardown " +
				"carries in its stream metadata — one of F-066's three ad hoc couplings"},
		"Nonce":     {kind: identityLocal, encoding: encJSON, reason: "this teardown's own identity"},
		"Origin":    {kind: notAnIdentity, encoding: encJSON, reason: "the authoring broker, for diagnostics; no reader arbitrates on it"},
		"StartedAt": {kind: notAnIdentity, encoding: encJSON, reason: "a wall-clock stamp for diagnostics"},
	}

	m["mqttAcceptFact"] = map[string]censusDisposition{
		"ClientID": {kind: notAnIdentity, encoding: encJSON, reason: "the client ID this acceptance belongs to; the subject addresses it"},
		// LOCAL, not cross-stream: an acceptance fact is stored ON $MQTT_sess, and this
		// is that same stream's incarnation identity, compared against the epoch this
		// account adopted from it. No other stream issues it and no other stream's
		// recreation invalidates it. It is an identity rather than a forensic stamp
		// because it decides whether the fact is comparable and usable at all.
		"SessEpoch": {kind: identityLocal, encoding: encJSON, reason: "the $MQTT_sess incarnation epoch, on a record that lives on $MQTT_sess: it is what makes an acceptance comparable at all, and it is issued by the stream holding it"},
		"AcceptID":  {kind: identityLocal, encoding: encJSON, reason: "this acceptance's own identity, minted per accepted CONNECT"},
		"At":        {kind: notAnIdentity, encoding: encJSON, reason: "a wall-clock stamp"},
	}

	m["mqttQoS2RelClaim"] = map[string]censusDisposition{
		"State": {kind: notAnIdentity, encoding: encJSON, reason: "the claim's phase"},
		// Both LOCAL, not cross-stream — but for TWO DIFFERENT reasons, and collapsing them
		// is what an earlier draft of these rows did. Neither is forensic: each is read and
		// each decides.
		//
		// Gen is the EXCHANGE generation, and NO STREAM ISSUES IT: mqttStoreQoS2MsgOnce
		// mints it in process with nuid.Next(), and mqttNewDeliverableMessage only STAMPS
		// the supplied value into the held record's Nmqtt-Q2Gen header (qos2_move.go:
		// "the exchange's identity for its whole lifetime"). So the old row's error was a
		// FALSE ISSUER — it named $MQTT_sess, whose session incarnation generation this is
		// not — rather than a holder/issuer mix-up.
		"Gen": {kind: identityLocal, encoding: encJSON, reason: "the EXCHANGE generation: minted in process with nuid.Next() at PUBLISH time and stamped onto the hold, shared by the hold and its .rel sibling; no stream issues it, and it decides whether a releasing claim is this exchange's or a foreign one"},
		// Fence is the other reason: $MQTT_qos2in DOES issue it — it is an absolute sequence
		// in that stream's own space — but the record carrying it also lives there, so the
		// reference is not foreign. Its old row named the issuer CORRECTLY and was wrong
		// only about cross-stream-ness, which is exactly what this row's own prose already
		// said ("the hold stream's own space") while its kind claimed otherwise.
		// qos2_release_claim.go states the identity half directly: "It is IDENTITY, not
		// forensics, because it is read and it decides."
		"Fence":  {kind: identityLocal, encoding: encJSON, reason: "F-062's proven purge cutoff: the highest $MQTT_qos2in sequence a discarded marker's writer was entitled to destroy, in the hold stream's own space, on a record stored in that same stream"},
		"Origin": {kind: notAnIdentity, encoding: encJSON, reason: "the authoring broker, for diagnostics"},
		"At":     {kind: notAnIdentity, encoding: encJSON, reason: "a wall-clock stamp"},
	}

	m["mqttSessTeardown"] = map[string]censusDisposition{
		"Origin":    {kind: notAnIdentity, encoding: encJSON, reason: "the authoring broker, for diagnostics"},
		"Nonce":     {kind: identityLocal, encoding: encJSON, reason: "the teardown claim's own identity, matched by the releaser"},
		"StartedAt": {kind: notAnIdentity, encoding: encJSON, reason: "a wall-clock stamp"},
	}

	// ConsumerConfig is the consumer-configuration artifact family. It is dispositioned
	// HERE rather than deferred, because rounds 2, 3 and 4 each said the drift guard has
	// to cover it: some durable configs are nested in session records, but the ephemeral
	// watch and retained consumers' configs are not, so a record-body walk misses them
	// entirely. Reached from five embedding sites and dispositioned once (property 1).
	//
	// The identity-bearing fields are the four names and the two sequence/subject ones;
	// everything else is delivery tuning, which is a protocol property of the consumer
	// rather than a reference to another stream's identity space.
	m["ConsumerConfig"] = map[string]censusDisposition{
		"Durable": {kind: identityLocal, lifecycle: "the stream the consumer is created on", encoding: encJSON,
			reason: "the durable name. MINTED LOCALLY — randomly for a plain consumer, deterministically " +
				"for a shared group — BEFORE the consumer create, so JetStream holds the consumer under a " +
				"name this bridge authored rather than issuing it. Named by F-066's table wherever a record " +
				"persists it: recreating the stream the consumer lives on strands the name"},
		"Name": {kind: identityLocal, lifecycle: "the stream the consumer is created on", encoding: encJSON, reason: "the consumer's name as the server reports it; for a durable it equals Durable, scoped to the same stream lifecycle"},
		"DeliverSubject": {kind: identityLocal, encoding: encJSON,
			reason: "the delivery subject this bridge minted; a repair of a SESSION durable must mint a " +
				"FRESH one rather than reuse a recorded one, because the recorded $JS.ACK/NAK subjects " +
				"embed the name with per-instance counters (sess_resume_reconcile.go). The two " +
				"per-broker EPHEMERAL consumers are the deliberate exception: their subject is derived " +
				"from the server run's identity so a retry re-derives it (ephemeral_identity.go)"},
		"FilterSubject":  {kind: identityLocal, encoding: encJSON, reason: "the subject space this consumer filters, derived from the MQTT filter; it names a subject pattern rather than another stream's identity"},
		"FilterSubjects": {kind: container, elems: "filtered subject patterns", encoding: encJSON, reason: "the multi-filter form of FilterSubject; the elements are subject patterns, not identities another stream issues"},
		"DeliverGroup":   {kind: identityLocal, encoding: encJSON, reason: "the shared-subscription queue group, derived deterministically from the group name so a rejoin returns the same one"},
		"OptStartSeq": {kind: notAnIdentity, encoding: encJSON,
			reason: "audited and UNUSED: no non-test site sets it, so no persisted config carries a start " +
				"sequence that a stream recreation could strand. Recorded rather than omitted, because " +
				"'nobody sets it' is a fact this census asserts and the guard re-checks"},
		"OptStartTime":    {kind: notAnIdentity, encoding: encJSON, reason: "unused, as OptStartSeq; a wall-clock start point, not an identity"},
		"DeliverPolicy":   {kind: notAnIdentity, encoding: encJSON, reason: "where a new consumer starts; delivery semantics"},
		"AckPolicy":       {kind: notAnIdentity, encoding: encJSON, reason: "delivery semantics; AckExplicit everywhere except the two AckNone watch consumers"},
		"AckWait":         {kind: notAnIdentity, encoding: encJSON, reason: "redelivery timing"},
		"MaxDeliver":      {kind: notAnIdentity, encoding: encJSON, reason: "redelivery bound"},
		"BackOff":         {kind: container, elems: "redelivery backoff intervals", encoding: encJSON, reason: "a slice of durations; timing, carrying no identity"},
		"ReplayPolicy":    {kind: notAnIdentity, encoding: encJSON, reason: "delivery pacing"},
		"RateLimit":       {kind: notAnIdentity, encoding: encJSON, reason: "delivery pacing"},
		"SampleFrequency": {kind: notAnIdentity, encoding: encJSON, reason: "ack sampling for monitoring"},
		"MaxWaiting":      {kind: notAnIdentity, encoding: encJSON, reason: "pull-consumer bound; this bridge uses push consumers"},
		"MaxAckPending": {kind: notAnIdentity, encoding: encJSON,
			reason: "the in-flight cap. Load-bearing for Receive Maximum accounting, and a stream-forced " +
				"value below the broker's effective cap is fatal at validation — but it is a limit, not a " +
				"reference to anything"},
		"FlowControl":        {kind: notAnIdentity, encoding: encJSON, reason: "push-consumer flow control"},
		"HeadersOnly":        {kind: notAnIdentity, encoding: encJSON, reason: "delivery shape; this bridge needs bodies"},
		"MaxRequestBatch":    {kind: notAnIdentity, encoding: encJSON, reason: "pull-consumer bound"},
		"MaxRequestExpires":  {kind: notAnIdentity, encoding: encJSON, reason: "pull-consumer bound"},
		"MaxRequestMaxBytes": {kind: notAnIdentity, encoding: encJSON, reason: "pull-consumer bound"},
		"Heartbeat":          {kind: notAnIdentity, encoding: encJSON, reason: "idle heartbeat interval"},
		"InactiveThreshold": {kind: notAnIdentity, encoding: encJSON,
			reason: "the reaping threshold. This is the mechanism whose silent reaping the resume " +
				"reconciliation exists to repair, and it is fatal as a stream-forced limit on $MQTT_out — " +
				"but a duration is not an identity"},
		"Replicas":       {kind: notAnIdentity, encoding: encJSON, reason: "consumer replica count"},
		"MemoryStorage":  {kind: notAnIdentity, encoding: encJSON, reason: "consumer storage mode"},
		"Direct":         {kind: notAnIdentity, encoding: encJSON, reason: "server-set for direct-get consumers; this bridge reads via the JS API"},
		"Sourcing":       {kind: notAnIdentity, encoding: encJSON, reason: "server-set marker for a sourcing consumer"},
		"Description":    {kind: notAnIdentity, encoding: encJSON, reason: "cosmetic"},
		"Metadata":       {kind: container, keys: "metadata keys", elems: "metadata values", encoding: encJSON, reason: "arbitrary key/value metadata. The census records it as a CARRIER because $MQTT_teardown's epoch coupling is carried in STREAM metadata. Consumer metadata is no longer unused: the two per-broker ephemeral consumers stamp owner/purpose/run/schema, and the SERVER compares it as part of the ActionCreate equality check, which is what makes a colliding run's consumer unequal. No broker code reads it back (ephemeral_identity.go)"},
		"PauseUntil":     {kind: notAnIdentity, encoding: encJSON, reason: "a pause deadline; unused by this bridge"},
		"PriorityGroups": {kind: container, elems: "priority group names", encoding: encJSON, reason: "priority-group names; unused by this bridge"},
		"PriorityPolicy": {kind: notAnIdentity, encoding: encJSON, reason: "priority policy; unused by this bridge"},
		"PinnedTTL":      {kind: notAnIdentity, encoding: encJSON, reason: "priority pinning timeout; unused by this bridge"},
	}

	// mqttQ2AffClaim was MISSING from an earlier draft of this manifest entirely, and
	// the F-066 oracle pass is what found it: two of the sample's fourteen rows name
	// this type. That is the oracle working as designed — it is applied AFTER an
	// independent enumeration precisely so a gap surfaces as a failure rather than as
	// a shorter table.
	m["mqttQ2AffClaim"] = map[string]censusDisposition{
		"Winner":       {kind: identityLocal, encoding: encJSON, reason: "the session object that won this assignment's election"},
		"Gen":          {kind: identityCrossStream, issuer: mqttSessStreamName, encoding: encJSON, reason: "the affine generation's owning incarnation"},
		"GroupDurable": {kind: identityCrossStream, issuer: mqttStreamName, encoding: encJSON, reason: "the shared group consumer that delivered the source; a consumer on $MQTT_msgs, so recreating that stream strands it"},
		"SourceSeq":    {kind: identityCrossStream, issuer: mqttStreamName, encoding: encJSON, reason: "the source message's sequence. F-066's table names this: after a recreation a new message reuses the sequence and the source-absence inference the reaper depends on breaks"},
		"ClaimHash":    {kind: identityCrossStream, issuer: mqttStreamName, encoding: encJSON, reason: "derived FROM the source sequence, so it inherits that sequence space and is stranded with it — a derived name is not a local one"},
		"Nonce":        {kind: identityLocal, encoding: encJSON, reason: "the incarnation nonce minted at the election and never rewritten; F-065 made it scope the copy subject and the copy's Nats-Msg-Id"},
		"CopySubject":  {kind: identityCrossStream, issuer: mqttOutStreamName, encoding: encJSON, reason: "the copy's subject on $MQTT_out; F-066's table names it"},
		"AffDurable":   {kind: identityCrossStream, issuer: mqttOutStreamName, encoding: encJSON, reason: "the session-affine consumer that delivers the copy, on $MQTT_out"},
		"SubID":        {kind: notAnIdentity, encoding: encJSON, reason: "the client's subscription identifier, client-scoped"},
		"Retained":     {kind: notAnIdentity, encoding: encJSON, reason: "whether the copy was retained; a fact about the delivery"},
		"Phase":        {kind: notAnIdentity, encoding: encJSON, reason: "the claim's phase, advanced by CAS"},
		"SourceAcked":  {kind: notAnIdentity, encoding: encJSON, reason: "a proven fact about the source message, deliberately outside the incarnation fence because it is a fact about the source rather than about this claim"},
		"AffineDone":   {kind: notAnIdentity, encoding: encJSON, reason: "a terminal copy DISPOSITION, written by seven routes — the session discarded, a redelivery under an already-terminal claim, a subscribe denial, expiry, an oversize discard, retired-source settlement, and the ordinary PUBREC. It is NOT proof that the client answered PUBREC; that is PubRecProven"},
		"PubRecProven": {kind: notAnIdentity, encoding: encJSON, reason: "a proven fact about the client's PUBREC: a stored PUBREL on $MQTT_out carries a bound (claim subject, copy subject) reference to this claim incarnation. The only fact in this record that outranks Lost, and the reason it is separate from AffineDone is that AffineDone's routes include the delivery gate's own Lost arm"},
		"Discarded":    {kind: notAnIdentity, encoding: encJSON, reason: "a teardown fact about the session, deliberately outside the incarnation fence for the same reason as SourceAcked"},
		"Lost":         {kind: notAnIdentity, encoding: encJSON, reason: "the owner-resume verdict that releases a group delivery without re-offering it"},
	}

	m["mqttWillOutbox"] = map[string]censusDisposition{
		"WillID":       {kind: identityLocal, encoding: encJSON, reason: "this Will event's own identity, and the second token of its subject, so each generation is an independent CAS slot"},
		"ClientIDHash": {kind: notAnIdentity, encoding: encJSON, reason: "the client hash, which the subject also carries; the subject is what addresses the record"},
		"ClientID":     {kind: notAnIdentity, encoding: encJSON, reason: "the plaintext client ID, carried for the publish-time authorization check and diagnostics"},
		"Topic":        {kind: notAnIdentity, encoding: encJSON, reason: "the Will's MQTT topic"},
		"Msg":          {kind: opaquePayload, encoding: encJSON, reason: "the Will payload: arbitrary application bytes the bridge never interprets"},
		"QoS":          {kind: notAnIdentity, encoding: encJSON, reason: "MQTT delivery metadata"},
		"Retain":       {kind: notAnIdentity, encoding: encJSON, reason: "MQTT delivery metadata"},
		"Props":        {kind: structuredBlob, decoder: "mqttValidateForwardProps", encoding: encJSON, reason: "the MQTT 5 properties block, validated before being forwarded into an outbound PUBLISH"},
		"User":         {kind: notAnIdentity, encoding: encJSON, reason: "the publishing user, for authorization at publish time"},
		"FireAt":       {kind: notAnIdentity, encoding: encJSON, reason: "the absolute delay deadline"},
		"State":        {kind: notAnIdentity, encoding: encJSON, reason: "the outbox record's phase, advanced by CAS"},
		"ClaimOwner":   {kind: notAnIdentity, encoding: encJSON, reason: "the broker holding the publish claim; a lease holder, not a durable identity anything arbitrates by equality"},
		"ClaimUntil":   {kind: notAnIdentity, encoding: encJSON, reason: "the claim's renewal deadline"},
		"Account":      {kind: notAnIdentity, encoding: encJSON, reason: "the NATS account, for the publish"},
		"OriginSessionSeq": {kind: identityComposite, compos: "origin interval", encoding: encJSON,
			reason: "see the composite oracle: paired with OriginSessEpoch"},
		"OriginSessEpoch": {kind: identityComposite, compos: "origin interval", encoding: encJSON,
			reason: "see the composite oracle: paired with OriginSessionSeq"},
		"Gen":      {kind: identityCrossStream, issuer: mqttSessStreamName, encoding: encJSON, reason: "the incarnation generation that enqueued this Will"},
		"Decision": {kind: notAnIdentity, encoding: encJSON, reason: "publish or cancel, the terminal verdict"},
		"ScanFrom": {kind: identityComposite, compos: "scan interval", encoding: encJSON, reason: "see the composite oracle: paired with ScanEpoch"},
		"ScanEpoch": {kind: identityComposite, compos: "scan interval", encoding: encJSON,
			reason: "see the composite oracle: paired with ScanFrom"},
		"DecisionCutoff": {kind: identityCrossStream, issuer: mqttSessStreamName, encoding: encJSON,
			reason: "the exact $MQTT_sess last sequence the decision was taken against; a pointer so " +
				"'no cutoff recorded' is distinguishable from sequence zero"},
	}

	m["mqttPersistedSession"] = map[string]censusDisposition{
		"Origin": {kind: notAnIdentity, encoding: encJSON, reason: "the authoring broker; the own-write spawn filter reads it and Origin proves no authorship, so it may only ever cost latency"},
		"ID":     {kind: notAnIdentity, encoding: encJSON, reason: "the client ID, validated against the subject's hash at restore"},
		"Clean":  {kind: notAnIdentity, encoding: encJSON, reason: "the negotiated Clean Start flag"},
		"Subs":   {kind: container, keys: "subscription filters (sids)", elems: "the granted QoS", encoding: encJSON, reason: "the KEYS are the identities here — a filter the client can UNSUBSCRIBE — and reflection does not see them"},
		"NoLocal": {kind: container, elems: "subscription filters (sids)", encoding: encJSON,
			reason: "the elements are filters, so the membership is the fact; an empty slice and an absent one mean the same thing"},
		"RetainAsPublished": {kind: container, elems: "subscription filters (sids)", encoding: encJSON, reason: "as NoLocal: membership of a filter set"},
		"SubIDs":            {kind: container, keys: "subscription filters (sids)", elems: "the client's subscription identifier", encoding: encJSON, reason: "keys are filters, values are client-scoped identifiers"},
		"SharedSubs":        {kind: container, keys: "shared-subscription keys ($share/group/filter)", elems: "the granted QoS", encoding: encJSON, reason: "the key is the sole authority for group membership and is parsed back into group plus subject"},
		"Cons":              {kind: container, keys: "subscription filters (sids)", elems: "ConsumerConfig", encoding: encJSON, reason: "keys are filters; the values are consumer configurations dispositioned once under their own type"},
		"PubRel":            {kind: embeddedType, embeds: "ConsumerConfig", encoding: encJSON, reason: "this session's PUBREL delivery consumer"},
		"Q2Aff":             {kind: embeddedType, embeds: "ConsumerConfig", encoding: encJSON, reason: "this session's affine QoS 2 delivery consumer"},
		"Q2AffGen":          {kind: identityLocal, encoding: encJSON, reason: "the affine generation, minted with the affine path and keyed on separately from the session generation — refs.hasAffine, not refs.hasInbound"},
		"Gen":               {kind: identityLocal, encoding: encJSON, reason: "the incarnation generation. Issued WITH this record, so it is local to $MQTT_sess; every other stream that carries it holds a cross-stream reference"},
		"WID":               {kind: identityLocal, encoding: encJSON, reason: "this write's own identity, used by the fork classifier"},
		"PWID":              {kind: identityLocal, encoding: encJSON, reason: "the parent write's identity; DETECTION only since the write-lineage audit deleted the create side"},
		"Drains":            {kind: container, keys: "subscription filters (sids)", elems: "mqttPersistedDrain", encoding: encJSON, reason: "keys are filters; the elements are drains dispositioned under their own type"},
		"CancelledRegs": {kind: container, elems: "in-flight register nonces", elemIssuer: mqttInflightStreamName, encoding: encJSON,
			reason: "the void-register verdicts: membership of a NONCE set, and the nonce is the identity " +
				"precisely because every other candidate is recycled — a teardown frees the packet " +
				"identifier at once, subjects are reused within a generation, and shared group durables " +
				"are named deterministically. Bounded by proof-keyed retirement. The ELEMENTS are " +
				"$MQTT_inflight identities, which is why this row carries elemIssuer"},
		"Teardown":       {kind: embeddedType, embeds: "mqttSessTeardown", encoding: encJSON, reason: "the teardown claim marker, when one is standing"},
		"Will":           {kind: embeddedType, embeds: "mqttPersistedWill", encoding: encJSON, reason: "the session's Will, when it has one"},
		"ExpiryInterval": {kind: notAnIdentity, encoding: encJSON, reason: "the granted Session Expiry Interval"},
		"DisconnectedAt": {kind: notAnIdentity, encoding: encJSON, reason: "the disconnect stamp the expiry deadline is measured from; zero means connected"},
	}

	// mqttRetainedMsg is the counterexample that makes property 2 concrete: no json
	// tags at all, a persisted UNEXPORTED field, two exported fields that are not
	// persisted, and one that the encoder writes and the decoder ignores.
	m["mqttRetainedMsg"] = map[string]censusDisposition{
		"Origin":  {kind: notAnIdentity, encoding: encHeader, reason: "the authoring server, in Nmqtt-ROrigin"},
		"Subject": {kind: notAnIdentity, encoding: encEnvelope, reason: "NOT a body or header field: reconstructed on read from the stored message's own subject, deliberately so a sourced stream with subject transforms still decodes"},
		"Topic": {kind: notAnIdentity, encoding: encEncodedIgnored,
			reason: "the encoder writes Nmqtt-RTopic unconditionally, but the decoder IGNORES it and " +
				"derives the topic from the subject for the same subject-transform reason. So it is " +
				"persisted and unread — neither 'not persisted' nor a live carrier"},
		"Msg":              {kind: opaquePayload, encoding: encRawBody, reason: "the retained payload IS the message body, appended verbatim after the header block and read back as the raw NATS body: arbitrary application bytes with no decoder that could enumerate identities"},
		"Flags":            {kind: notAnIdentity, encoding: encHeader, reason: "in Nmqtt-RFlags, and the one header that identifies the record as one this broker wrote: absent, the bytes are refused rather than interpreted"},
		"Source":           {kind: notAnIdentity, encoding: encHeader, reason: "in Nmqtt-RSource"},
		"Props":            {kind: structuredBlob, decoder: "mqttValidateForwardProps", encoding: encHeader, reason: "base64 in Nmqtt-RProps, validated before use because a non-MQTT publisher could craft it and it is written verbatim into outbound packets"},
		"SessionHash":      {kind: notAnIdentity, encoding: encHeader, reason: "the authenticated No Local origin marker: an HMAC under a per-server secret, so it is a proof rather than an identity anything joins on"},
		"expires":          {kind: notAnIdentity, encoding: encHeader, reason: "UNEXPORTED and persisted, in Nmqtt-RExp: the absolute Message Expiry deadline. A walk over exported fields alone would miss it"},
		"expiresFromCache": {kind: notAnIdentity, encoding: encNotPersisted, reason: "in-memory only: the cached deadline, and the genuinely unpersisted unexported field of this type"},
	}

	m["mqttPersistedWill"] = map[string]censusDisposition{
		"Topic":  {kind: notAnIdentity, encoding: encJSON, reason: "the Will's MQTT topic"},
		"Msg":    {kind: opaquePayload, encoding: encJSON, reason: "the Will payload: arbitrary application bytes the bridge never interprets"},
		"QoS":    {kind: notAnIdentity, encoding: encJSON, reason: "MQTT delivery metadata"},
		"Retain": {kind: notAnIdentity, encoding: encJSON, reason: "MQTT delivery metadata"},
		"Props":  {kind: structuredBlob, decoder: "mqttValidateForwardProps", encoding: encJSON, reason: "the MQTT 5 properties block, forwarded verbatim into outbound PUBLISH packets and therefore validated before use"},
		"User":   {kind: notAnIdentity, encoding: encJSON, reason: "the publishing user, for authorization at publish time"},
		"FireAt": {kind: notAnIdentity, encoding: encJSON, reason: "the absolute delay deadline"},
	}

	// ---- the server-side artifact families -----------------------------------
	// Reached from the roots by the recursive walk. Plan revision 6 requires them:
	// some durable consumer configs are nested in session records, but the ephemeral
	// watch and retained consumers' are not, and the orphan sweep consumes StreamState
	// directly as its cursor.
	//
	// Most of these fields carry no identity, and that is the verdict the census
	// records rather than a reason to omit them. The interesting rows are the ones that
	// name a SUBJECT SPACE or a SEQUENCE, because those are what a recreation
	// invalidates.
	local := func(why string) censusDisposition {
		return censusDisposition{kind: notAnIdentity, encoding: encJSON, reason: why}
	}

	m["StreamConfig"] = map[string]censusDisposition{
		"Name":     {kind: identityLocal, encoding: encJSON, reason: "the stream's own name, which the bridge authors and every $MQTT_* constant fixes"},
		"Subjects": {kind: container, elems: "the stream's subject space", encoding: encJSON, reason: "the subject SPACE is the identity space every record on this stream is addressed in; three of the nine carry the JS domain token and six do not, and the validators compare the set order-independently"},
		"Metadata": {kind: container, keys: "metadata keys", elems: "metadata values", elemIssuer: mqttSessStreamName, encoding: encJSON,
			reason: "the carrier of $MQTT_teardown's adopted incarnation EPOCH — one of F-066's three couplings, and the one no body walk can see. Recorded as cross-stream through its elements for that reason"},
		"Sources":                {kind: container, elems: "StreamSource", encoding: encJSON, reason: "cross-account sourcing, supported on $MQTT_rmsgs and fatal elsewhere; the elements name another stream's identity space"},
		"Mirror":                 {kind: embeddedType, embeds: "StreamSource", encoding: encJSON, reason: "a mirror names another stream; rejected by the shared shape on every broker-owned stream"},
		"SubjectTransform":       {kind: embeddedType, embeds: "SubjectTransformConfig", encoding: encJSON, reason: "a transform rewrites the subject space, so it rewrites the identity space; rejected by the shared shape"},
		"RePublish":              {kind: embeddedType, embeds: "RePublish", encoding: encJSON, reason: "republish names a destination subject space; rejected by the shared shape"},
		"Placement":              {kind: embeddedType, embeds: "Placement", encoding: encJSON, reason: "cluster scheduling; where the stream lives changes nothing its owner does"},
		"ConsumerLimits":         {kind: embeddedType, embeds: "StreamConsumerLimits", encoding: encJSON, reason: "stream-forced consumer limits, two of which are fatal on specific streams"},
		"Retention":              local("the retention policy, which decides whether a store with no consumer keeps anything — load-bearing but not an identity"),
		"Storage":                local("file or memory; the shared shape requires file"),
		"Discard":                local("discard policy"),
		"DiscardNewPer":          local("per-subject discard, inert without DiscardNew"),
		"MaxMsgs":                local("a global limit; every broker-owned stream rejects these because they evict live protocol state"),
		"MaxBytes":               local("a global limit, as MaxMsgs"),
		"MaxAge":                 local("a global limit, as MaxMsgs"),
		"MaxMsgsPer":             local("per-subject retention, which is what makes a subject a single-slot CAS target"),
		"MaxMsgSize":             local("a size cap; no owner tolerates one, since a Will bypasses the publish preflight"),
		"MaxConsumers":           local("a consumer cap"),
		"Replicas":               local("the replica count the validators compare against the configured one"),
		"Sealed":                 local("a sealed stream accepts no writes; rejected"),
		"NoAck":                  local("acknowledgements off would make every store unverifiable; rejected"),
		"Duplicates":             local("the deduplication window. The WINDOW is not an identity; the dedup ENTRY it creates is server state, audited as its own family"),
		"AllowRollup":            local("rollup headers let any publisher purge the stream; rejected as an attacker-triggered eviction"),
		"DenyDelete":             local("whether deletes are permitted; fatal wherever deletion is a record's only exit"),
		"DenyPurge":              local("whether purges are permitted; fatal wherever a bounded purge is load-bearing"),
		"AllowDirect":            local("direct gets; owners read through the JS API"),
		"MirrorDirect":           local("direct gets for mirrors"),
		"AllowMsgTTL":            local("per-message TTL, required wherever MQTT 5 message expiry is stored"),
		"SubjectDeleteMarkerTTL": local("delete-marker TTL; no owner creates delete markers"),
		"AllowMsgCounter":        local("counter streams; rejected"),
		"AllowAtomicPublish":     local("batch publishing, REQUIRED on $MQTT_sess (the accept batch) and on $MQTT_inflight (the register and its unsent marker are one batch) because both carry Nats-Batch-* headers"),
		"AllowBatchPublish":      local("batch publishing capability"),
		"AllowMsgSchedules":      local("scheduled messages; no owner emits schedule headers"),
		"Compression":            local("storage-internal"),
		"FirstSeq":               local("the creation-time initial sequence. NOT an identity: CAS expectations come from re-reads and stored sequences, never from an assumed starting point"),
		"PersistMode":            local("persistence mode; the shared shape requires the default"),
		"Description":            local("cosmetic"),
	}

	m["StreamSource"] = map[string]censusDisposition{
		"Name":              {kind: identityCrossStream, issuer: "the sourced stream", encoding: encJSON, reason: "names another stream outright, which is the whole point of a source"},
		"FilterSubject":     {kind: identityLocal, encoding: encJSON, reason: "the subject filter applied to the source"},
		"OptStartSeq":       {kind: identityCrossStream, issuer: "the sourced stream", encoding: encJSON, reason: "a sequence in the SOURCE stream's space, so recreating that stream strands it"},
		"OptStartTime":      local("a wall-clock start point"),
		"External":          {kind: embeddedType, embeds: "ExternalStream", encoding: encJSON, reason: "the cross-account addressing of an external source"},
		"SubjectTransforms": {kind: container, elems: "SubjectTransformConfig", encoding: encJSON, reason: "transforms rewrite the sourced subject space, which is why the retained decoder reconstructs its subject rather than trusting a stored one"},
		"Consumer":          {kind: embeddedType, embeds: "StreamConsumerSource", encoding: encJSON, reason: "a PRE-EXISTING sourcing consumer named in the source configuration; its fields are dispositioned, because the server treats a non-nil Consumer as configured ownership rather than a report"},
		"iname": {kind: notAnIdentity, encoding: encNotPersisted,
			reason: "the server's internal derived source index. UNEXPORTED and untagged, so " +
				"encoding/json cannot touch it — an earlier revision recorded it as encJSON via the " +
				"`local` helper, which review caught as contradicting this census's own claim that " +
				"each field's ENCODING is dispositioned accurately"},
	}

	// StreamConsumerSource was briefly excluded as "a read-time consumer-sourcing report".
	// Review rejected that and was right: in the JetStream API this is persisted
	// StreamSource CONFIGURATION — it names a PRE-EXISTING sourcing consumer, and the
	// server's stream-update logic treats a non-nil Consumer as configured ownership
	// rather than generated reporting. Since $MQTT_rmsgs explicitly supports sources, the
	// exclusion was hiding durable consumer and delivery-subject identities.
	m["StreamConsumerSource"] = map[string]censusDisposition{
		"Name": {kind: identityLocal, lifecycle: "the sourced stream", encoding: encJSON,
			reason: "the pre-existing sourcing consumer's name, configured rather than generated; " +
				"scoped to the lifecycle of the stream that consumer lives on"},
		"DeliverSubject": {kind: identityLocal, lifecycle: "the sourced stream", encoding: encJSON,
			reason: "that consumer's delivery subject, configured on the source; same lifecycle scoping"},
	}

	m["ExternalStream"] = map[string]censusDisposition{
		"ApiPrefix":     {kind: identityLocal, encoding: encJSON, reason: "the API prefix an external source is reached through"},
		"DeliverPrefix": {kind: identityLocal, encoding: encJSON, reason: "the delivery prefix for an external source"},
	}

	m["SubjectTransformConfig"] = map[string]censusDisposition{
		"Source":      {kind: identityLocal, encoding: encJSON, reason: "the pattern matched; a subject-space rewrite rule"},
		"Destination": {kind: identityLocal, encoding: encJSON, reason: "the pattern written; this is what can make a sourced record's subject differ from its origin's"},
	}

	m["RePublish"] = map[string]censusDisposition{
		"Source":      {kind: identityLocal, encoding: encJSON, reason: "the republish source pattern"},
		"Destination": {kind: identityLocal, encoding: encJSON, reason: "the republish destination pattern"},
		"HeadersOnly": local("whether only headers are republished"),
	}

	m["Placement"] = map[string]censusDisposition{
		"Cluster":   local("target cluster name; scheduling only"),
		"Tags":      {kind: container, elems: "placement tags", encoding: encJSON, reason: "scheduling tags; carry no protocol identity"},
		"Preferred": local("preferred peer; scheduling only"),
	}

	m["StreamConsumerLimits"] = map[string]censusDisposition{
		"InactiveThreshold": local("a stream-forced consumer inactivity threshold. Fatal on $MQTT_out, where it would reap a disconnected session's affine consumer; waived on $MQTT_msgs, where the drain and shared paths refuse it at RUNTIME instead. Since F-036 course E this refusal is also why an uncollected affine consumer leaks with no backstop at all"),
		"MaxAckPending":     local("a stream-forced in-flight cap; fatal when below the broker's effective cap"),
	}

	m["ConsumerInfo"] = map[string]censusDisposition{
		"Stream": {kind: identityCrossStream, issuer: "the stream the consumer is on", encoding: encJSON, reason: "names the stream this consumer belongs to"},
		"Name":   {kind: identityLocal, encoding: encJSON, reason: "the consumer name, which the bridge minted before the create"},
		"Config": {kind: embeddedType, embeds: "ConsumerConfig", encoding: encJSON, reason: "the persisted configuration, dispositioned once under its own type"},
		"Created": {kind: identityLocal, encoding: encJSON,
			reason: "the JetStream-issued consumer INSTANCE fingerprint. The orphan sweep's PUBREL class " +
				"uses it as an identity and explicitly not as an age. A create-or-update leaves it " +
				"UNCHANGED, which is why it cannot see an adoption — the blind spot F-033's audit " +
				"recorded against the AFFINE consumer delete, now HISTORICAL because F-036 course E " +
				"removed that delete outright. Local because the consumer's own lifecycle issues it, " +
				"but it is issued by a different Raft group than the state it guards"},
		"Delivered":      {kind: embeddedType, embeds: "SequenceInfo", encoding: encJSON, reason: "the delivery cursor: consumer STATE, which acks mutate"},
		"AckFloor":       {kind: embeddedType, embeds: "SequenceInfo", encoding: encJSON, reason: "the ack floor: the consumer state confirmAck waits for"},
		"NumAckPending":  local("in-flight count; the accounting Receive Maximum interacts with"),
		"NumRedelivered": local("redelivery count, which a delivery callback interprets"),
		"NumWaiting":     local("pull-request count; this bridge uses push consumers"),
		"NumPending":     local("undelivered count"),
		"Cluster":        {kind: embeddedType, embeds: "ClusterInfo", encoding: encJSON, reason: "transient Raft placement, declared out of scope; naming it here keeps the exclusion visible at the site rather than only in the walker"},
		"PushBound":      local("whether a push consumer has a bound subscription"),
		"Paused":         local("pause state; this bridge never pauses a consumer"),
		"PauseRemaining": local("remaining pause duration"),
		"TimeStamp":      local("the read's own timestamp, not a property of the consumer"),
		"PriorityGroups": {kind: container, elems: "priority group state", encoding: encJSON, reason: "per-group state; this bridge sets no priority groups, which the ConsumerConfig row records"},
	}

	// StreamState is the stream's own durable accounting, and the orphan sweep consumes
	// it directly as a cursor — which is what makes it a census subject rather than a
	// status report.
	m["StreamState"] = map[string]censusDisposition{
		"Msgs":  local("the live message count; the sweep tests it for zero before walking"),
		"Bytes": local("the live byte count"),
		"FirstSeq": {kind: identityLocal, lifecycle: "this stream", encoding: encJSON,
			reason: "the low end of the live sequence range, and half of the orphan sweep's cursor. " +
				"Scoped to THIS stream's incarnation: recreating it resets the space, which is why a " +
				"cursor is never carried across a recreation"},
		"FirstTime": local("the timestamp of the first live message"),
		"LastSeq": {kind: identityLocal, lifecycle: "this stream", encoding: encJSON,
			reason: "the high end, the other half of the sweep's cursor — and the WATERMARK a bounded " +
				"destructive pass is entitled to purge through. Same incarnation scoping as FirstSeq"},
		"LastTime":    local("the timestamp of the last live message"),
		"NumSubjects": local("the count of subjects with live messages"),
		"Subjects": {kind: container, keys: "the stream's occupied subjects", elems: "per-subject message counts", encoding: encJSON,
			reason: "the KEYS are subjects, so they are identities in this stream's own space — and the " +
				"map is sampled AFTER State, which is why a returned subject may sit above LastSeq and " +
				"why that pair may bound a purge but never license an inference about where subjects sit"},
		"NumDeleted": local("the count of deleted sequences inside the live range"),
		"Deleted": {kind: container, elems: "deleted sequences in this stream's space", encoding: encJSON,
			reason: "the gaps in the live range; sequences, so this stream's own identity space"},
		"Lost":      {kind: embeddedType, embeds: "LostStreamData", encoding: encJSON, reason: "the store's report of sequences it could not recover; its fields ARE dispositioned, because a report about identities still carries them"},
		"Consumers": local("the number of consumers on the stream"),
	}

	// LostStreamData was briefly in censusOutOfScope as "a read-time report". Review
	// rejected that: Msgs is a slice of server-issued STREAM SEQUENCES, so describing the
	// whole type as a report suppressed an identity-bearing carrier. A report ABOUT
	// identities still carries them.
	m["LostStreamData"] = map[string]censusDisposition{
		"Msgs": {kind: container, elems: "lost stream sequences", elemIssuer: "this stream", encoding: encJSON,
			reason: "the sequences the store could not recover, in THIS stream's own space — so a " +
				"recreation invalidates them along with everything else that names them"},
		"Bytes": local("how much was lost, in bytes; an amount rather than an identity"),
	}

	// PriorityGroupState likewise: excluded as transient, but it is reachable from a root
	// and this census records a verdict for everything reachable rather than a filtered
	// subset. The verdict happens to be that this bridge sets no priority groups.
	m["PriorityGroupState"] = map[string]censusDisposition{
		"Group":          {kind: identityLocal, encoding: encJSON, reason: "the group name; this bridge sets no priority groups, which the ConsumerConfig row records"},
		"PinnedClientID": {kind: identityLocal, encoding: encJSON, reason: "the pinned consumer instance; unused by this bridge"},
		"PinnedTS":       local("when the pin was taken"),
	}

	m["SequenceInfo"] = map[string]censusDisposition{
		"Consumer": {kind: identityLocal, encoding: encJSON, reason: "the consumer-sequence half of the cursor: per-consumer, reset by a recreate under the same name, which is why a repair mints a FRESH name instead"},
		"Stream":   {kind: identityCrossStream, issuer: "the stream the consumer is on", encoding: encJSON, reason: "the STREAM-sequence half. Recreating that stream resets the space this points into, so a restored cursor names a different message"},
		"Last":     local("the wall-clock time of the last activity at this position"),
	}

	return m
}

// censusWalkFields returns one entry per DIRECT field of a named struct type, with
// enough of its shape for the rules below to be checked. It deliberately does not
// recurse: recursion is what path-keying does, and property 1 in the file header is
// why this manifest does not.
type censusField struct {
	name       string
	kind       reflect.Kind
	elemKind   reflect.Kind
	isByteSlot bool
	isStruct   bool // struct, or pointer to struct, excluding time.Time
	exported   bool
}

// censusOutOfScope names types the walk STOPS at, each with the reason. They are
// reachable from a root but are not persisted state: an API response envelope, or
// transient cluster metadata the server recomputes.
//
// This exists because rooting the walk at StreamInfo pulled in ClusterInfo, PeerInfo,
// StreamAlternate and ApiError — response structure, not durable state. Declaring them
// explicitly keeps the census's subject coherent AND keeps the exclusion honest: a new
// response type reachable from a root fails the walk until somebody says why it is out.
var censusOutOfScope = map[string]string{
	"ApiError":         "an API error envelope: transport-level response detail, never persisted",
	"ClusterInfo":      "transient Raft cluster membership, recomputed by the server on every read",
	"PeerInfo":         "transient per-peer replication state, as ClusterInfo",
	"StreamAlternate":  "a read-time routing hint for mirrors and sources; nothing here persists it",
	"StreamSourceInfo": "live source PROGRESS as reported, not the source configuration; the configuration is StreamSource, which IS audited",
}

// censusReachableTypes walks OUT from the audited roots and returns every named struct
// type reachable through fields, pointers, map values and slice elements.
//
// This is the answer to a review finding on the first Slice A implementation: the type
// universe used to be a hand-written list, so a newly persisted body type — or a nested
// one like SequenceInfo inside ConsumerInfo — was covered only if somebody remembered to
// add it. A hand-written universe is the same shape as the hand-listed carrier set that
// was one of F-033's four falsified attempts, one level up.
//
// time.Time is a leaf: it is a scalar with a custom encoding, not a structure to walk.
// The seen set makes recursive types terminate.
func censusReachableTypes(roots map[string]any) map[string]reflect.Type {
	out := map[string]reflect.Type{}
	var walk func(reflect.Type)
	walk = func(t reflect.Type) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		switch t.Kind() {
		case reflect.Struct:
			if t == reflect.TypeOf(time.Time{}) {
				return
			}
			name := t.Name()
			if name == "" {
				return // an anonymous struct has no manifest key
			}
			if _, skip := censusOutOfScope[name]; skip {
				return // declared not-persisted-state, with a reason
			}
			if _, done := out[name]; done {
				return
			}
			out[name] = t
			for i := 0; i < t.NumField(); i++ {
				walk(t.Field(i).Type)
			}
		case reflect.Map:
			walk(t.Elem())
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Uint8 {
				return
			}
			walk(t.Elem())
		}
	}
	for _, v := range roots {
		walk(reflect.TypeOf(v))
	}
	return out
}

func censusWalkFields(t reflect.Type) []censusField {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	var out []censusField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		ft := f.Type
		cf := censusField{name: f.Name, kind: ft.Kind(), exported: f.PkgPath == ""}
		et := ft
		for et.Kind() == reflect.Pointer {
			et = et.Elem()
		}
		if et.Kind() == reflect.Struct && et != reflect.TypeOf(time.Time{}) {
			cf.isStruct = true
		}
		switch ft.Kind() {
		case reflect.Map:
			cf.elemKind = ft.Elem().Kind()
		case reflect.Slice, reflect.Array:
			cf.elemKind = ft.Elem().Kind()
			cf.isByteSlot = ft.Elem().Kind() == reflect.Uint8
		}
		out = append(out, cf)
	}
	return out
}

// censusRoots are the artifacts this bridge PERSISTS or reads back as configuration and
// state. The audited type universe is everything reachable from them
// (censusReachableTypes), not this list — so a nested type is covered without anybody
// remembering to add it.
//
// StreamConfig, ConsumerInfo and StreamState are roots because plan revision 6 requires
// them: some durable consumer configs are nested in session records, but the ephemeral
// watch and retained consumers' are not, and the orphan sweep consumes StreamState
// directly as its cursor.
func censusRoots() map[string]any {
	return map[string]any{
		"mqttInflightRecord":    mqttInflightRecord{},
		"mqttInflightMarker":    mqttInflightMarker{},
		"mqttPersistedDrainSeq": mqttPersistedDrainSeq{},
		"mqttPersistedDrain":    mqttPersistedDrain{},
		"mqttGenRetirement":     mqttGenRetirement{},
		"mqttAcceptFact":        mqttAcceptFact{},
		"mqttQoS2RelClaim":      mqttQoS2RelClaim{},
		"mqttSessTeardown":      mqttSessTeardown{},
		"mqttPersistedWill":     mqttPersistedWill{},
		"mqttWillOutbox":        mqttWillOutbox{},
		"mqttPersistedSession":  mqttPersistedSession{},
		"mqttRetainedMsg":       mqttRetainedMsg{},
		"mqttQ2AffClaim":        mqttQ2AffClaim{},
		"ConsumerConfig":        ConsumerConfig{},
		"StreamConfig":          StreamConfig{},
		"ConsumerInfo":          ConsumerInfo{},
		// StreamState is a root in its OWN right. An earlier revision named it in the
		// prose above while rooting only at StreamConfig and ConsumerInfo — neither of
		// which reaches it — so the recursive guard passed because the required root had
		// been forgotten, and the dossier claimed coverage the test did not provide.
		// Review caught it; this is why the roots are a list the tests consume rather
		// than a sentence in a comment.
		"StreamState": StreamState{},
	}
}

// censusTypes is the audited universe: the roots plus everything reachable from them.
func censusTypes() map[string]reflect.Type {
	return censusReachableTypes(censusRoots())
}

// TestPersistedIdentityCensusFieldCoverage is the drift guard: a bijection in BOTH
// directions between the reflected field set of every audited type and its
// dispositions, then exactly one disposition per field, every one carrying prose.
//
// A dependency bump or a new field fails this by name, which is the whole point: the
// census's completeness claim rests on this test rather than on care.
func TestPersistedIdentityCensusFieldCoverage(t *testing.T) {
	t.Parallel()
	manifest := censusManifest()

	for name, typ := range censusTypes() {
		disp, ok := manifest[name]
		if !ok {
			t.Errorf("type %q is reachable from a persisted root but has no dispositions at all", name)
			continue
		}
		fields := censusWalkFields(typ)
		seen := map[string]bool{}
		for _, f := range fields {
			seen[f.name] = true
			d, ok := disp[f.name]
			if !ok {
				// Both directions land here: a field was added, or its disposition was
				// removed. The message names both because the mutation runs showed the
				// single-cause wording misreads the second one.
				t.Errorf("%s.%s has no disposition: either the field is new, or its disposition was removed. "+
					"Either way the census no longer covers it", name, f.name)
				continue
			}
			if d.reason == "" {
				t.Errorf("%s.%s has an empty reason: the prose IS the audit", name, f.name)
			}
		}
		for fname := range disp {
			if !seen[fname] {
				t.Errorf("%s.%s has a disposition but no such field: the census names something that no longer exists", name, fname)
			}
		}
	}

}

// TestPersistedIdentityCensusContainersDeclareContents enforces property 3: a map,
// slice or []byte field may not take a leaf disposition, because Go reflection does
// not recurse into what its keys and elements carry. And an embedded struct must name
// the type its fields are dispositioned under, so the type-keyed manifest cannot
// silently skip a nested artifact.
func TestPersistedIdentityCensusContainersDeclareContents(t *testing.T) {
	t.Parallel()
	manifest := censusManifest()

	leaf := func(k censusKind) bool {
		switch k {
		case identityLocal, identityCrossStream, identityComposite, notAnIdentity:
			return true
		}
		return false
	}

	for name, typ := range censusTypes() {
		disp := manifest[name]
		for _, f := range censusWalkFields(typ) {
			d, ok := disp[f.name]
			if !ok {
				continue // reported by the coverage test
			}
			switch {
			case f.isByteSlot:
				if d.kind != structuredBlob && d.kind != opaquePayload {
					t.Errorf("%s.%s is a byte slice but is dispositioned as a leaf: it must declare "+
						"whether it is a structured blob with a decoder or an opaque application payload", name, f.name)
				}
				if d.kind == structuredBlob && d.decoder == "" {
					t.Errorf("%s.%s is a structured blob with no named decoder", name, f.name)
				}
			case f.kind == reflect.Map || f.kind == reflect.Slice:
				if d.kind != container {
					t.Errorf("%s.%s is a %s but is dispositioned %v: a container must declare what its "+
						"keys and elements carry", name, f.name, f.kind, d.kind)
				}
				if d.keys == "" && d.elems == "" {
					t.Errorf("%s.%s is a container that declares neither keys nor elements", name, f.name)
				}
			case f.isStruct:
				if d.kind != embeddedType {
					t.Errorf("%s.%s embeds a struct but is dispositioned %v: it must name the type its "+
						"fields are dispositioned under", name, f.name, d.kind)
					continue
				}
				if d.embeds == "" {
					t.Errorf("%s.%s is an embedded type naming no type", name, f.name)
				}
			default:
				if !leaf(d.kind) {
					t.Errorf("%s.%s is a scalar but is dispositioned %v, which is a container or "+
						"embedding kind", name, f.name, d.kind)
				}
			}
			if d.kind == identityCrossStream && d.issuer == "" {
				t.Errorf("%s.%s is cross-stream but names no issuing stream", name, f.name)
			}
		}
	}
}

// TestPersistedIdentityCensusCompositesArePaired cross-checks the manifest against the
// INDEPENDENT composite oracle. Round 5 of the plan review found the earlier reciprocal
// check passed vacuously when a mutation changed BOTH members of a composite: with the
// oracle derived from the same literal, it moved with them. So the oracle is a separate
// hand-written table and this test requires the two to agree in both directions.
func TestPersistedIdentityCensusCompositesArePaired(t *testing.T) {
	t.Parallel()
	manifest := censusManifest()

	byName := map[string]censusComposite{}
	for _, c := range censusComposites {
		if c.reason == "" {
			t.Errorf("composite %q has an empty reason", c.kind)
		}
		if len(c.members) < 2 {
			t.Errorf("composite %q has %d members: a binding needs at least two", c.kind, len(c.members))
		}
		byName[c.kind] = c

		// Every member the oracle names must exist, and must be dispositioned as a
		// member of THIS composite. This is the direction that kills the
		// both-members mutation: changing them to ordinary identities leaves the
		// oracle still demanding the composite.
		disp, ok := manifest[c.typ]
		if !ok {
			t.Errorf("composite %q names type %q, which the manifest does not cover", c.kind, c.typ)
			continue
		}
		for _, mem := range c.members {
			d, ok := disp[mem]
			if !ok {
				t.Errorf("composite %q names %s.%s, which has no disposition", c.kind, c.typ, mem)
				continue
			}
			if d.kind != identityComposite {
				t.Errorf("composite %q names %s.%s, but it is dispositioned %v rather than a composite member",
					c.kind, c.typ, mem, d.kind)
				continue
			}
			if d.compos != c.kind {
				t.Errorf("%s.%s says it belongs to composite %q, but the oracle names it under %q",
					c.typ, mem, d.compos, c.kind)
			}
		}
	}

	// And the converse: a field claiming membership of a composite the oracle does
	// not carry.
	for tname, disp := range manifest {
		for fname, d := range disp {
			if d.kind != identityComposite {
				continue
			}
			if d.compos == "" {
				t.Errorf("%s.%s is a composite member naming no composite", tname, fname)
				continue
			}
			c, ok := byName[d.compos]
			if !ok {
				t.Errorf("%s.%s claims composite %q, which the oracle does not carry", tname, fname, d.compos)
				continue
			}
			found := false
			for _, mem := range c.members {
				if mem == fname {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("%s.%s claims composite %q, but that composite does not list it as a member",
					tname, fname, d.compos)
			}
		}
	}
}

// TestPersistedIdentityCensusEmbeddingsResolve checks that every type an embedding
// site names is itself audited somewhere — either reflected directly, or reachable as
// a named type. Without this, property 1's type-keying could silently drop a nested
// artifact: the site would say "dispositioned under X" and no X would exist.
func TestPersistedIdentityCensusEmbeddingsResolve(t *testing.T) {
	t.Parallel()
	manifest := censusManifest()
	known := map[string]bool{}
	for name := range manifest {
		known[name] = true
	}
	// Types the census dispositions by reference rather than by walking them here,
	// each with the reason it is deferred. Kept explicit so a new deferral has to be
	// argued rather than assumed.
	deferred := map[string]string{
		"ConsumerConfig": "the consumer-configuration artifact family, audited as its own family in " +
			"docs/findings/persisted-identity-census.md; 35 fields reached from five embedding sites, " +
			"which is why the manifest is type-keyed",
	}
	for name, why := range deferred {
		if why == "" {
			t.Errorf("deferred type %q carries no reason", name)
		}
		known[name] = true
	}
	// A site may embed a type the walk stops at, so long as that exclusion is
	// declared with its reason. Naming it at the site keeps the exclusion visible
	// where a reader meets it, not only in the walker.
	for name, why := range censusOutOfScope {
		if why == "" {
			t.Errorf("out-of-scope type %q carries no reason", name)
		}
		known[name] = true
	}

	var missing []string
	for tname, disp := range manifest {
		for fname, d := range disp {
			switch d.kind {
			case embeddedType:
				if !known[d.embeds] {
					missing = append(missing, fmt.Sprintf("%s.%s embeds %q, which nothing audits", tname, fname, d.embeds))
				}
			case container:
				// An element type that names a struct must resolve too; a scalar
				// element is described in prose and needs no type.
				if d.elems != "" && isCensusTypeName(d.elems) && !known[d.elems] {
					missing = append(missing, fmt.Sprintf("%s.%s has elements of %q, which nothing audits", tname, fname, d.elems))
				}
			}
		}
	}
	sort.Strings(missing)
	for _, m := range missing {
		t.Error(m)
	}
}

// isCensusTypeName reports whether s looks like one of this package's audited type
// names rather than a prose description of what a container carries.
func isCensusTypeName(s string) bool {
	return len(s) > 4 && s[:4] == "mqtt"
}

// ---------------------------------------------------------------------------
// The F-066 oracle
// ---------------------------------------------------------------------------

// censusOracleRow is one row of F-066's verified cross-stream identity sample
// (docs/findings/ha-arbitration.md#f-066), transcribed so the census can be checked
// AGAINST it.
//
// The direction matters and is the whole point. F-066's table is a SAMPLE — its own
// text says "a verified sample with a worked consequence, not a census" — so it is
// used as an ORACLE applied AFTER an independent enumeration, never as the seed for
// one. Seeding from it would inherit its blind spots and make the completeness claim
// circular (feedback_named_sites_are_a_sample). A sample row the census does not
// already carry is a census DEFECT.
//
// It has already earned its keep: the first oracle pass found mqttQ2AffClaim missing
// from the manifest entirely, RegSeq dispositioned as notAnIdentity when the sample
// records it as a cross-stream reference, and no way to express that CancelledRegs'
// ELEMENTS are $MQTT_inflight identities.
type censusOracleRow struct {
	holder string // the stream whose records hold the reference
	what   string // the sample's own description of the carrier
	issuer string // the stream that issues the identity
	// satisfiedBy names the manifest entries that carry this row, as "Type.Field".
	// Empty only when family is set.
	satisfiedBy []string
	// family names the artifact family this row's carrier belongs to when it is not a
	// record body field, with the reason. A row may be deferred to a family, but it
	// may not be silently absent.
	family string
	reason string
}

// censusF066Sample is F-066's fourteen-row table, transcribed row for row.
var censusF066Sample = []censusOracleRow{
	{
		holder: mqttTeardownStreamName, what: "stream Metadata + body MarkerSeq", issuer: mqttSessStreamName,
		satisfiedBy: []string{"mqttGenRetirement.MarkerSeq"},
		family:      "stream configuration",
		reason:      "the body half is a manifest row; the epoch half is carried in STREAM METADATA, which is the stream-configuration family and is the carrier no body walk can see",
	},
	{
		holder: mqttWillQStreamName, what: "body OriginSessionSeq + OriginSessEpoch", issuer: mqttSessStreamName,
		satisfiedBy: []string{"mqttWillOutbox.OriginSessionSeq", "mqttWillOutbox.OriginSessEpoch"},
	},
	{
		holder: mqttWillQStreamName, what: "body ScanFrom + ScanEpoch", issuer: mqttSessStreamName,
		satisfiedBy: []string{"mqttWillOutbox.ScanFrom", "mqttWillOutbox.ScanEpoch"},
	},
	{
		holder: mqttQoS2AssignStreamName, what: "subject, body SourceSeq/GroupDurable/ClaimHash", issuer: mqttStreamName,
		satisfiedBy: []string{"mqttQ2AffClaim.SourceSeq", "mqttQ2AffClaim.GroupDurable", "mqttQ2AffClaim.ClaimHash"},
		family:      "message subjects",
		reason:      "the three body fields are manifest rows; the subject half belongs to the message-subject family",
	},
	{
		holder: mqttQoS2AssignStreamName, what: "body CopySubject, AffDurable", issuer: mqttOutStreamName,
		satisfiedBy: []string{"mqttQ2AffClaim.CopySubject", "mqttQ2AffClaim.AffDurable"},
	},
	{
		holder: mqttOutStreamName, what: "affine copy: subject; header Nmqtt-Q2Claim; Nats-Msg-Id", issuer: mqttQoS2AssignStreamName,
		family: "message headers and subjects",
		reason: "the affine copy carries no JSON body of ours — its identities are entirely in the subject, the Nmqtt-Q2Claim header and the deduplication id, so this row is the headers-and-subjects family rather than any body type",
	},
	{
		holder: mqttInflightStreamName, what: "body PubRelDurable + PubRelStreamSeq", issuer: mqttOutStreamName,
		satisfiedBy: []string{"mqttInflightRecord.PubRelDurable", "mqttInflightRecord.PubRelStreamSeq"},
	},
	{
		holder: mqttOutStreamName, what: "stored PUBREL: headers Nmqtt-Q2SrcDur/Nmqtt-Q2SrcSeq; Nats-Msg-Id", issuer: mqttStreamName,
		family: "message headers",
		reason: "pubrel_correlation.go stores the correlation as headers, deliberately, and the attempt counter in the Nats-Msg-Id exists because a dedup entry outlives its message — so the whole row is the header family",
	},
	{
		holder: mqttInflightStreamName, what: "body Dur, Seq", issuer: mqttStreamName + " / " + mqttOutStreamName,
		satisfiedBy: []string{"mqttInflightRecord.Dur", "mqttInflightRecord.Seq"},
	},
	{
		holder: mqttInflightStreamName, what: "body Q2AffClaim", issuer: mqttQoS2AssignStreamName,
		satisfiedBy: []string{"mqttInflightRecord.Q2AffClaim"},
	},
	{
		holder: mqttInflightStreamName, what: "body ClaimSeq", issuer: mqttSessStreamName,
		satisfiedBy: []string{"mqttInflightRecord.ClaimSeq"},
	},
	{
		holder: mqttSessStreamName, what: "body Q2Aff.Durable, PubRel.Durable, Cons[].Durable", issuer: mqttOutStreamName + " / " + mqttStreamName,
		satisfiedBy: []string{"ConsumerConfig.Durable"},
		reason:      "all three sites embed ConsumerConfig, dispositioned once under that type — which is exactly why the manifest is type-keyed",
	},
	{
		holder: mqttSessStreamName, what: "body drain durable, Started[].Seq", issuer: mqttStreamName,
		satisfiedBy: []string{"ConsumerConfig.Durable", "mqttPersistedDrainSeq.Seq"},
	},
	{
		holder: mqttSessStreamName, what: "body drain Started[].RegSeq", issuer: mqttInflightStreamName,
		satisfiedBy: []string{"mqttPersistedDrainSeq.RegSeq"},
		reason:      "this is the carrier of F-034's own V1 defect; the census records it as a cross-stream reference even though F-034 demotes it to a hint",
	},
	{
		holder: mqttSessStreamName, what: "body CancelledRegs[]", issuer: mqttInflightStreamName,
		satisfiedBy: []string{"mqttPersistedSession.CancelledRegs"},
	},
}

// TestPersistedIdentityCensusRediscoversF066Sample applies the oracle. Every row of
// F-066's sample must already be carried by the census — either by a named manifest
// entry whose issuer agrees, or by an explicitly named artifact family with a reason.
//
// A row may be DEFERRED to a family; it may not be silently absent, and it may not be
// satisfied by an entry that disagrees about which stream issues the identity.
func TestPersistedIdentityCensusRediscoversF066Sample(t *testing.T) {
	t.Parallel()
	manifest := censusManifest()

	// FIFTEEN, and the fifteenth is the one this file predicted. The durable QoS 2 owner
	// slot couples `$MQTT_inflight` body PubRelDurable/PubRelStreamSeq to `$MQTT_out`, and
	// it is a genuinely NEW coupling: the pre-existing Dur/Seq row couples this record to a
	// SOURCE DELIVERY, where this names one specific PUBREL RECORD's sequence in a specific
	// consumer's space, and recreating $MQTT_out resets that space.
	//
	// The two fields were deleted once, when nothing produced them, and the row went with
	// them because a row asserting a coupling no record carries is a claim about nothing.
	// They are back, with the admission half that writes them, so the row is back too.
	// ActionClaim/ActionClaimSeq still have no row of their own: they are members of the
	// arbitration relation the ClaimSeq row carries, not a second coupling to $MQTT_sess.
	if len(censusF066Sample) != 15 {
		t.Fatalf("the oracle carries %d rows; F-066's sample has 15. A row was dropped or added "+
			"without re-reading the dossier", len(censusF066Sample))
	}

	for _, row := range censusF066Sample {
		label := fmt.Sprintf("%s / %s", row.holder, row.what)
		if len(row.satisfiedBy) == 0 && row.family == "" {
			t.Errorf("%s: the oracle row is neither satisfied by a manifest entry nor assigned to an "+
				"artifact family — a sample row the census does not carry is a census defect", label)
			continue
		}
		if row.family != "" && row.reason == "" {
			t.Errorf("%s: deferred to family %q with no reason", label, row.family)
		}
		for _, ref := range row.satisfiedBy {
			var tname, fname string
			if n := indexByte(ref, '.'); n > 0 {
				tname, fname = ref[:n], ref[n+1:]
			} else {
				t.Errorf("%s: satisfiedBy entry %q is not in Type.Field form", label, ref)
				continue
			}
			disp, ok := manifest[tname]
			if !ok {
				t.Errorf("%s: names type %q, which the census does not cover", label, tname)
				continue
			}
			d, ok := disp[fname]
			if !ok {
				t.Errorf("%s: names %s.%s, which has no disposition", label, tname, fname)
				continue
			}
			// The row must be recorded as carrying an identity of some kind. A sample
			// row dispositioned notAnIdentity is the disagreement the first oracle
			// pass caught on RegSeq.
			switch d.kind {
			case identityCrossStream, identityComposite, identityLocal, container:
			default:
				t.Errorf("%s: %s.%s is dispositioned %v, but F-066's sample records it as carrying a "+
					"cross-stream identity", label, tname, fname, d.kind)
			}
			// And a row F-066 records as CROSS-STREAM may not be satisfied by a bare
			// local identity. Review found this hole: ConsumerConfig.Durable was
			// identityLocal and the oracle accepted it, because it checked issuers and
			// a local identity has none. A local identity satisfies a cross-stream
			// sample row ONLY if it declares the lifecycle namespace that makes it
			// cross-stream — which is the distinction between who minted a value and
			// what invalidates it.
			if d.kind == identityLocal && row.issuer != "" && d.lifecycle == "" {
				t.Errorf("%s: %s.%s is identityLocal with no lifecycle namespace, but F-066's sample "+
					"records this row as issued by %q — a locally minted value satisfies a cross-stream "+
					"row only when it names what recreating invalidates it",
					label, tname, fname, row.issuer)
			}
			// Where the census names an issuer directly, it must agree with the sample.
			if d.kind == identityCrossStream && d.issuer != "" && row.issuer != "" {
				if !containsStream(row.issuer, d.issuer) {
					t.Errorf("%s: %s.%s says its issuer is %q, but the sample says %q",
						label, tname, fname, d.issuer, row.issuer)
				}
			}
			if d.kind == container && d.elemIssuer != "" && row.issuer != "" {
				if !containsStream(row.issuer, d.elemIssuer) {
					t.Errorf("%s: %s.%s says its elements' issuer is %q, but the sample says %q",
						label, tname, fname, d.elemIssuer, row.issuer)
				}
			}
		}
	}
}

// indexByte is strings.IndexByte, spelled out to keep this file's imports to what the
// reflection walk needs.
func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// containsStream reports whether want appears in the sample's issuer field, which may
// name two streams for a carrier whose sequence space is selected by a sibling field
// (the (Dur, Seq) row names both $MQTT_msgs and $MQTT_out).
func containsStream(sampleIssuer, want string) bool {
	for i := 0; i+len(want) <= len(sampleIssuer); i++ {
		if sampleIssuer[i:i+len(want)] == want {
			return true
		}
	}
	return false
}

// TestPersistedIdentityCensusEncodingsAreConsistent ties an `encJSON` disposition to what
// `encoding/json` can actually reach.
//
// Review found `StreamSource.iname` recorded as `encJSON` while being unexported and
// untagged, so no JSON encoder could ever touch it — and the reflection tests did not
// notice, because they check that a field HAS a disposition, not that the disposition is
// TRUE. An encoding axis nobody verifies is a comment, and this census leans on it: the
// retained-message rules exist precisely because encoding and identity are independent.
//
// Rule 1: a field dispositioned `encJSON` must be exported, since `encoding/json` ignores
// unexported fields regardless of tags. Fields carried by another mechanism declare it —
// `encHeader`, `encEnvelope`, `encEncodedIgnored`, `encNotPersisted`, `encRawBody` — and
// those may be unexported, which is exactly `mqttRetainedMsg.expires`.
//
// Rule 1 was the whole test, and a LATER review round showed why one rule was not enough:
// `mqttRetainedMsg.Msg` was dispositioned `encJSON` while being the raw message body, and
// exportedness passed it. Exportedness is necessary for `encJSON` and nowhere near
// sufficient.
//
// Rule 2 closes that: for a type in `censusBespokeEncoders` — one whose wire form a NAMED
// encoder writes — `encJSON` is refused outright, because the body it produces is not JSON.
//
// Rule 3 is the independent oracle, and the only check here that consults the PRODUCTION
// encoder rather than the manifest: every `mqttRetainedMsg` field is given a distinct
// sentinel, the record is encoded, and where the sentinel actually lands decides the
// verdict — header slice, body slice, or nowhere. A declared carrier that disagrees with
// the bytes is an error whichever direction it disagrees in.
func TestPersistedIdentityCensusEncodingsAreConsistent(t *testing.T) {
	t.Parallel()
	manifest := censusManifest()
	for name, typ := range censusTypes() {
		disp := manifest[name]
		bespoke, isBespoke := censusBespokeEncoders[name]
		for _, f := range censusWalkFields(typ) {
			d, ok := disp[f.name]
			if !ok {
				continue // reported by the coverage test
			}
			if d.encoding == encJSON && !f.exported {
				t.Errorf("%s.%s is dispositioned encJSON but is UNEXPORTED, so encoding/json "+
					"cannot reach it: its real carrier must be named", name, f.name)
			}
			if d.encoding == encJSON && isBespoke {
				t.Errorf("%s.%s is dispositioned encJSON, but %s's wire form is written by %s "+
					"and read by %s, not by encoding/json: no field of it can be a json body "+
					"field, so the real carrier must be named",
					name, f.name, name, bespoke.encoder, bespoke.decoder)
			}
		}
	}

	censusAssertRetainedCarriers(t, manifest["mqttRetainedMsg"])
}

// censusAssertRetainedCarriers is rule 3: it checks each declared carrier of
// `mqttRetainedMsg` against the bytes `mqttEncodeRetainedMessage` actually produces.
//
// The payload is deliberately NON-EMPTY. An empty `Msg` is the tombstone shape, which
// `mqttEncodeRetainedMessage` treats as a deletion and for which it suppresses the expiry
// headers — so a zero-value round-trip would find several sentinels missing and conclude
// they were not persisted at all.
// censusRetainedProps is a GENUINELY VALID MQTT 5.0 properties block, and it has to be.
//
// Props is dispositioned structuredBlob with mqttValidateForwardProps as its decoder, and
// that validator returns nil for anything it cannot parse — so an arbitrary sentinel like
// "census-props" round-trips as EMPTY and the oracle reads a live header as an ignored one.
// The first version of this oracle did exactly that, and the failure was the oracle's, not
// the census's: a decoder in the path means the sentinel must be something the decoder
// accepts.
//
// One User Property, whose key and value are distinctive enough to be unmistakable in the
// header block: length prefix, 0x26, then two UTF-8 string pairs.
func censusRetainedProps() []byte {
	pair := func(v string) []byte {
		return append([]byte{byte(len(v) >> 8), byte(len(v))}, v...)
	}
	body := append([]byte{mqttPropUserProperty}, pair("census-props-key")...)
	body = append(body, pair("census-props-value")...)
	return append([]byte{byte(len(body))}, body...)
}

func censusAssertRetainedCarriers(t *testing.T, disp map[string]censusDisposition) {
	t.Helper()

	rm := &mqttRetainedMsg{
		Origin:      "census-origin",
		Subject:     "census.subject",
		Topic:       "census/topic",
		Msg:         []byte("census-payload"),
		Flags:       mqttPubQos1,
		Source:      "census-source",
		SessionHash: "census-sessionhash",
		Props:       censusRetainedProps(),
		expires:     time.Now().Add(time.Hour),
	}
	// expiresFromCache is the type's genuinely unpersisted field, so it is set to a value
	// DISTINCT from expires: sharing the deadline would make "found nowhere" unprovable,
	// since expires' own sentinel is in the header block.
	rm.expiresFromCache = rm.expires.Add(time.Hour)
	natsMsg, hdrLen := mqttEncodeRetainedMessage(rm)
	hdrBytes, body := natsMsg[:hdrLen], string(natsMsg[hdrLen:])

	// Headers are read BY KEY rather than by substring. Review round 7: a substring search
	// over the whole header block cannot say WHICH header carried a value, so a value that
	// happens to appear inside another header's text satisfies the wrong row.
	headers := map[string][]byte{}
	for _, k := range []string{
		mqttNatsRetainedMessageOrigin, mqttNatsRetainedMessageFlags, mqttNatsRetainedMessageSource,
		mqttNatsRetainedMessageProps, mqttNatsRetainedMessageExpiry, mqttNatsRetainedMessageSessionHash,
		mqttNatsRetainedMessageTopic,
	} {
		headers[k] = nil
	}
	mqttSliceHeaders(headers, hdrBytes)

	// Every field carries an EXACT expectation, and there is no skip. Round 7 found that
	// Flags had no sentinel and fell through a `continue`, which let encHeader be swapped
	// for encNotPersisted without the oracle noticing. A field this table does not name now
	// FAILS rather than passing quietly.
	//
	// header names the key the value must be under, so a declared header carrier is checked
	// positionally rather than by presence anywhere in the block.
	type carrier struct {
		value  string
		header string
	}
	expect := map[string]carrier{
		"Origin":      {value: rm.Origin, header: mqttNatsRetainedMessageOrigin},
		"Source":      {value: rm.Source, header: mqttNatsRetainedMessageSource},
		"SessionHash": {value: rm.SessionHash, header: mqttNatsRetainedMessageSessionHash},
		"Props":       {value: base64.StdEncoding.EncodeToString(rm.Props), header: mqttNatsRetainedMessageProps},
		"expires":     {value: strconv.FormatInt(rm.expires.UnixNano(), 10), header: mqttNatsRetainedMessageExpiry},
		// Flags is persisted as two HEX digits rather than its decimal byte, so it needs an
		// encoded expectation rather than a literal one.
		"Flags": {value: fmt.Sprintf("%x", rm.Flags), header: mqttNatsRetainedMessageFlags},
		"Topic": {value: rm.Topic, header: mqttNatsRetainedMessageTopic},
		"Msg":   {value: string(rm.Msg)},
		// Neither of these may appear anywhere, so neither names a header key: Subject is
		// rebuilt by the decoder from the message's own subject, and expiresFromCache is
		// in-memory only.
		"Subject":          {value: rm.Subject},
		"expiresFromCache": {value: strconv.FormatInt(rm.expiresFromCache.UnixNano(), 10)},
	}
	// The decoder's own verdict, which is what separates encHeader from encEncodedIgnored:
	// both are headers, and the difference is whether anything READS them back. Round 7 was
	// right that handling the two identically let Topic and Origin be swapped.
	back, err := mqttDecodeRetainedMessage(rm.Subject, hdrBytes, natsMsg[hdrLen:])
	if err != nil {
		t.Fatalf("the census sentinel record did not decode: %v", err)
	}
	decoded := map[string]string{
		"Origin":      back.Origin,
		"Source":      back.Source,
		"SessionHash": back.SessionHash,
		"Props":       base64.StdEncoding.EncodeToString(back.Props),
		"Flags":       fmt.Sprintf("%x", back.Flags),
		"Msg":         string(back.Msg),
		"Subject":     back.Subject,
		// Topic is the encEncodedIgnored case: the encoder writes Nmqtt-RTopic and the
		// decoder derives the topic from the SUBJECT instead. Subject is "census.subject"
		// and Topic "census/topic", so a decoder that read the header would return the
		// latter and this row would fail.
		"Topic": back.Topic,
	}
	decoded["expires"] = _EMPTY_
	if !back.expires.IsZero() {
		decoded["expires"] = strconv.FormatInt(back.expires.UnixNano(), 10)
	}
	decoded["expiresFromCache"] = _EMPTY_
	if !back.expiresFromCache.IsZero() {
		decoded["expiresFromCache"] = strconv.FormatInt(back.expiresFromCache.UnixNano(), 10)
	}

	for field, d := range disp {
		want, named := expect[field]
		if !named {
			t.Errorf("mqttRetainedMsg.%s has no expectation in this oracle, so its declared %v is "+
				"asserted by nothing: add one rather than letting the field through",
				field, d.encoding)
			continue
		}
		inNamedHdr := want.header != _EMPTY_ && string(headers[want.header]) == want.value
		anyHdr := bytes.Contains(hdrBytes, []byte(want.value))
		inBody := strings.Contains(body, want.value)
		readBack := decoded[field] == want.value

		switch d.encoding {
		case encHeader:
			if !inNamedHdr {
				t.Errorf("mqttRetainedMsg.%s declares encHeader, but header %q carries %q, not %q",
					field, want.header, headers[want.header], want.value)
			}
			if !readBack {
				t.Errorf("mqttRetainedMsg.%s declares encHeader — a LIVE header — but a round-trip "+
					"decoded %q, not %q: a header the decoder ignores is encEncodedIgnored",
					field, decoded[field], want.value)
			}
		case encEncodedIgnored:
			if !inNamedHdr {
				t.Errorf("mqttRetainedMsg.%s declares encEncodedIgnored, but header %q carries %q, "+
					"not %q: the encoder must still write it", field, want.header,
					headers[want.header], want.value)
			}
			if readBack {
				t.Errorf("mqttRetainedMsg.%s declares encEncodedIgnored, but a round-trip decoded "+
					"%q: the decoder DOES read it, so it is an ordinary encHeader",
					field, decoded[field])
			}
		case encRawBody:
			if !inBody {
				t.Errorf("mqttRetainedMsg.%s declares encRawBody, but %q is not in the message body",
					field, want.value)
			}
			if anyHdr {
				t.Errorf("mqttRetainedMsg.%s declares encRawBody, but %q also appears in the header "+
					"block: the carrier is ambiguous", field, want.value)
			}
			if !readBack {
				t.Errorf("mqttRetainedMsg.%s declares encRawBody but decoded as %q", field, decoded[field])
			}
		case encEnvelope:
			// Absent from the stored bytes AND present after a decode — the decoder rebuilds
			// it from the message's own subject. Both halves are the assertion.
			if anyHdr || inBody {
				t.Errorf("mqttRetainedMsg.%s declares encEnvelope, yet %q IS in the stored bytes "+
					"(header=%v body=%v)", field, want.value, anyHdr, inBody)
			}
			if !readBack {
				t.Errorf("mqttRetainedMsg.%s declares encEnvelope but decoded as %q: nothing "+
					"reconstructs it", field, decoded[field])
			}
		case encNotPersisted:
			if anyHdr || inBody {
				t.Errorf("mqttRetainedMsg.%s declares encNotPersisted, yet %q IS in the stored bytes "+
					"(header=%v body=%v)", field, want.value, anyHdr, inBody)
			}
			if readBack {
				t.Errorf("mqttRetainedMsg.%s declares encNotPersisted but survived a round-trip as %q",
					field, decoded[field])
			}
		default:
			t.Errorf("mqttRetainedMsg.%s declares %v, which this oracle does not know how to "+
				"check: extend it rather than exempting the field", field, d.encoding)
		}
	}
}
