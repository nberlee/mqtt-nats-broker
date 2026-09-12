package mqtt

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestInflightSlotShapeMatrix generates one case per row of the legal-shape matrix, and
// asserts each ILLEGAL combination is refused SEPARATELY rather than by one shared
// outcome.
//
// Per-case rather than pooled, because a folded refusal is exactly the defect the matrix
// exists to prevent: two different unsupported shapes sharing one rejection path means a
// mutation that collapses them changes nothing observable.
//
// The rows about the five deleted fields moved to
// TestInflightRecordDecodeIsAClosedSchema, where their premise is the one that now holds:
// those keys are refused because the SCHEMA is closed, not because a typed field was
// declared in order to reject itself.
func TestInflightSlotShapeMatrix(t *testing.T) {
	t.Parallel()
	const nonce = "xxxxxxxxxxxxxxxxxxxxxx"
	const other = "yyyyyyyyyyyyyyyyyyyyyy"
	base := func() *mqttInflightRecord {
		return &mqttInflightRecord{
			PI: 7, Dur: "$MQTT_msgs_x", Seq: 41, QoS: 2,
			State: mqttInflightPublish, Nonce: nonce,
			// A legal base carries a well-formed LINEAGE, so every row below still fails
			// for the fault it was written to catch rather than for a missing lineage.
			// The lineage rows are separate, at the end.
			Lin: nonce,
			// And CUSTODY, for the same reason: it is mandatory in every phase and at
			// every QoS (C-III clause 1), so a base without it would make every row
			// below fail for the wrong reason.
			Claim: other,
		}
	}
	for _, tc := range []struct {
		name  string
		build func(*mqttInflightRecord)
		legal bool
	}{
		{"QoS 2 publish, custody with a sequence", func(r *mqttInflightRecord) {
			r.Claim, r.ClaimSeq = other, 9
		}, true},
		{"QoS 2 publish, custody with NO sequence", func(r *mqttInflightRecord) {
			r.Claim = other
		}, true},
		// CUSTODY IS MANDATORY NOW, at every QoS and in every phase. The two rows that
		// stood here asserting a custody-free PUBLISH slot legal are INVERTED rather
		// than deleted: they are the exact population C-III's first clause closes, and a
		// deleted row would leave nothing holding the new rule.
		{"QoS 2 publish, NO custody", func(r *mqttInflightRecord) {
			r.Claim, r.ClaimSeq = _EMPTY_, 0
		}, false},
		{"QoS 1 publish, NO custody", func(r *mqttInflightRecord) {
			r.QoS, r.Claim, r.ClaimSeq = 1, _EMPTY_, 0
		}, false},
		{"a sequence with NO claimant", func(r *mqttInflightRecord) {
			r.Claim, r.ClaimSeq = _EMPTY_, 9
		}, false},
		{"absent state", func(r *mqttInflightRecord) { r.State = _EMPTY_ }, false},
		{"unknown state", func(r *mqttInflightRecord) { r.State = "somewhere-else" }, false},
		// Known PUBREL state names still need their own required field combinations.
		{"a PUBREL state without its canonical tuple", func(r *mqttInflightRecord) {
			r.State = "pubrel_ready"
		}, false},
		{"a terminal state without its canonical tuple", func(r *mqttInflightRecord) {
			r.State = "pubrel_terminal"
		}, false},
		{"no packet identifier", func(r *mqttInflightRecord) { r.PI = 0 }, false},
		{"no source durable", func(r *mqttInflightRecord) { r.Dur = _EMPTY_ }, false},
		{"no source sequence", func(r *mqttInflightRecord) { r.Seq = 0 }, false},
		{"nonce from a foreign alphabet", func(r *mqttInflightRecord) {
			r.Nonce = "!!!!!!!!!!!!!!!!!!!!!!"
		}, false},
		{"nonce of the wrong length", func(r *mqttInflightRecord) { r.Nonce = "short" }, false},
		{"QoS neither 1 nor 2", func(r *mqttInflightRecord) { r.QoS = 3 }, false},
		{"publish marked retired", func(r *mqttInflightRecord) { r.Retired = true }, true},
		{"publish cancelled", func(r *mqttInflightRecord) { r.Cancelled = true }, true},
		{"partial affine reference", func(r *mqttInflightRecord) {
			r.Q2AffClaim = "$MQTT_qos2assign.x"
		}, false},
		{"the other half of a partial affine reference", func(r *mqttInflightRecord) {
			r.Q2AffCopy = "$MQTT.out.x"
		}, false},
		{"QoS 1 publish, with custody", func(r *mqttInflightRecord) { r.QoS = 1 }, true},
		{"QoS 1 publish_terminal", func(r *mqttInflightRecord) {
			r.QoS, r.State = 1, mqttInflightPublishTerminal
		}, true},
		{"QoS 2 publish_terminal", func(r *mqttInflightRecord) {
			r.State = mqttInflightPublishTerminal
		}, true},
		{"publish_terminal naming a canonical PUBREL record", func(r *mqttInflightRecord) {
			r.State = mqttInflightPublishTerminal
			r.PubRelDurable, r.PubRelStreamSeq = "PUBREL", 9
		}, false},
		{"pubrel_intended with NO canonical tuple", func(r *mqttInflightRecord) {
			// The one past-PUBREC phase that legally names none: the phase is written
			// BEFORE the store, so a crash between them leaves a slot that says "past
			// PUBREC, canonical record OWED" rather than a `publish` slot a peer reaps.
			r.State = mqttInflightPubRelIntended
		}, true},
		{"a PUBLISH-phase slot carrying an action", func(r *mqttInflightRecord) {
			// LEGAL NOW, and that is C-I's representation change: a PUBLISH transmission
			// holds a durable action exactly as a PUBREL transmission does.
			r.ActionToken, r.ActionClaim, r.ActionClaimSeq = nonce, r.Claim, r.ClaimSeq
		}, true},
		{"a pubrel_terminal slot carrying an action", func(r *mqttInflightRecord) {
			// Also legal: the acknowledgement and the holder's drain are independent
			// events and either may land first.
			r.State = mqttInflightPubRelTerminal
			r.PubRelDurable, r.PubRelStreamSeq = "PUBREL", 9
			r.ActionToken, r.ActionClaim, r.ActionClaimSeq = nonce, r.Claim, r.ClaimSeq
		}, true},
		{"QoS 1 publish, shared custody with no sequence", func(r *mqttInflightRecord) {
			r.QoS, r.Claim = 1, other
		}, true},
		{"QoS 1 in a PUBREL-ready shape", func(r *mqttInflightRecord) {
			r.QoS, r.State, r.Claim = 1, mqttInflightPubRelReady, other
			r.PubRelDurable, r.PubRelStreamSeq = "PUBREL_QOS1", 9
		}, false},
		{"QoS 1 in a PUBREL phase", func(r *mqttInflightRecord) {
			r.QoS, r.State, r.Claim = 1, mqttInflightPubRelReady, other
			r.PubRelDurable, r.PubRelStreamSeq = "PUBREL_QOS1", 9
			r.ActionToken, r.ActionClaim = nonce, other
		}, false},
		{"QoS 1 carrying an affine reference", func(r *mqttInflightRecord) {
			r.QoS = 1
			r.Q2AffClaim, r.Q2AffCopy = "$MQTT_qos2assign.x", "$MQTT.out.x"
		}, false},
		{"QoS 1 marked retired", func(r *mqttInflightRecord) { r.QoS, r.Retired = 1, true }, false},
		// The LINEAGE is mandatory and is never defaulted to the nonce: a record that
		// predates it would silently acquire a FRESH exchange's value, so an inherited
		// obligation would read as a new one.
		{"absent lineage", func(r *mqttInflightRecord) { r.Lin = _EMPTY_ }, false},
		{"lineage from a foreign alphabet", func(r *mqttInflightRecord) {
			r.Lin = "!!!!!!!!!!!!!!!!!!!!!!"
		}, false},
		{"lineage of the wrong length", func(r *mqttInflightRecord) { r.Lin = "short" }, false},
		// A lineage that DIFFERS from the nonce is the ordinary shape after an inheriting
		// replacement, and it must be legal — refusing it would refuse every heir.
		{"lineage differing from the nonce", func(r *mqttInflightRecord) {
			r.Lin = "yyyyyyyyyyyyyyyyyyyyyy"
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := base()
			tc.build(r)
			err := mqttValidateInflightShape(r)
			if tc.legal && err != nil {
				t.Fatalf("must be legal, refused with: %v", err)
			}
			if !tc.legal && err == nil {
				t.Fatal("must be refused, was accepted")
			}
		})
	}
	if err := mqttValidateInflightShape(nil); err == nil {
		t.Error("a nil record must be refused")
	}
}

// TestInflightRecordDecodeIsAClosedSchema is what replaced five typed fields whose only
// job was to refuse themselves.
//
// The register carried ActionToken, ActionClaim, ActionClaimSeq, PubRelDurable and
// PubRelStreamSeq, and the shape matrix forbade every one of them in `publish` — the only
// state anything writes. So they were declared in order to be rejected. A review round
// named the flaw: that is an INCOMPLETE closed schema. Those five keys were refused while
// any SIXTH plausible control field would have been dropped silently by encoding/json and
// the record read as an ordinary legal register.
//
// The schema is closed at the decoder instead, so the property is about ANY unrecognised
// key rather than the ones someone thought of. The rows below are therefore raw JSON: a
// typed test cannot express a field the struct does not have.
func TestInflightRecordDecodeIsAClosedSchema(t *testing.T) {
	t.Parallel()
	const legal = `{"pi":7,"dur":"$MQTT_msgs_x","seq":41,"qos":2,"state":"publish",` +
		`"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx",` +
		`"claim":"yyyyyyyyyyyyyyyyyyyyyy","acq":0}`

	// The control, first: the legal body decodes. Without it every refusal below would
	// pass against a decoder that refused everything.
	rec, err := mqttDecodeInflightRecord([]byte(legal))
	if err != nil || rec == nil {
		t.Fatalf("the legal body must decode, got %v", err)
	}
	if rec.PI != 7 || rec.Dur != "$MQTT_msgs_x" || rec.Seq != 41 || rec.QoS != 2 ||
		rec.State != mqttInflightPublish || rec.Acq != 0 {
		t.Fatalf("the legal body decoded to the wrong record: %+v", rec)
	}

	// `acq` IS MANDATORY, AND ITS ZERO IS LEGAL — which is why presence is checked by the
	// key walk and not by a typed test after the decode. A body missing it is an
	// unsupported persisted shape and is REFUSED, never read as a fresh allocation:
	// reading absence as zero would hand a record from an unsupported shape a brand-new
	// exchange's transmission identity, and every release bound to that identity would
	// then name the wrong transmission.
	//
	// Paired with M11b. The control above is what makes this row discriminating: the two
	// bodies differ in exactly this one member.
	withoutAcq := `{"pi":7,"dur":"$MQTT_msgs_x","seq":41,"qos":2,"state":"publish",` +
		`"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx",` +
		`"claim":"yyyyyyyyyyyyyyyyyyyyyy"}`
	if got, gerr := mqttDecodeInflightRecord([]byte(withoutAcq)); gerr == nil {
		t.Fatalf("a register with no acq member must be refused, decoded to %+v", got)
	}

	// THE TWO DELETED COMPOSITE STATES are unsupported shapes now, and they are refused
	// even carrying an otherwise valid action triple — the shape that would have made
	// them look most legitimate. Paired with M11.
	activeBody := func(state string) []byte {
		return []byte(`{"pi":7,"dur":"$MQTT_msgs_x","seq":41,"qos":2,"state":"` + state + `",` +
			`"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx",` +
			`"claim":"yyyyyyyyyyyyyyyyyyyyyy","acq":3,` +
			`"preldur":"PUBREL","prelseq":9,` +
			`"action":"zzzzzzzzzzzzzzzzzzzzzz","actclaim":"yyyyyyyyyyyyyyyyyyyyyy"}`)
	}
	// THE CONTROL for the two rows below, and it is what makes them mean anything: the
	// SAME body with a supported state decodes. Without it both refusals would pass
	// against a body that was malformed for some other reason — a mistyped member name,
	// say — which is precisely how a row like this comes to prove nothing.
	if got, gerr := mqttDecodeInflightRecord(activeBody("pubrel_ready")); gerr != nil || got == nil {
		t.Fatalf("the control body must decode, got %v", gerr)
	}
	for _, deleted := range []string{"pubrel_sending", "pubrel_terminal_draining"} {
		if got, gerr := mqttDecodeInflightRecord(activeBody(deleted)); gerr == nil {
			t.Fatalf("the removed state %q must be refused, decoded to %+v", deleted, got)
		}
	}

	inject := func(extra string) []byte {
		return []byte(legal[:len(legal)-1] + "," + extra + "}")
	}
	for _, tc := range []struct {
		name  string
		extra string
	}{
		// The five deleted tags, one per row. Their premise is no longer "the validator
		// forbids this field in this state" — the field does not exist — but "this key is
		// not in the schema".
		{"the action token's tag", `"action":"xxxxxxxxxxxxxxxxxxxxxx"`},
		{"the action claimant's tag", `"action_claim":"yyyyyyyyyyyyyyyyyyyyyy"`},
		{"the action claim sequence's tag", `"action_claimseq":9`},
		{"the canonical PUBREL durable's tag", `"pubrel_dur":"$MQTT_PUBREL_x"`},
		{"the canonical PUBREL sequence's tag", `"pubrel_seq":3`},
		// And the rows the five-field approach could never cover, which is the point.
		{"a control field nobody predicted", `"admission_epoch":4`},
		{"a plausible future phase field", `"phase":"pubrel_ready"`},
		{"a near-miss of a real tag", `"nonce2":"xxxxxxxxxxxxxxxxxxxxxx"`},
		// THE THREE HOLES A REVIEW ROUND FOUND IN THE FIRST VERSION OF THIS BOUNDARY,
		// which used DisallowUnknownFields and Decoder.More. Every one of them decoded to
		// an ordinary legal `publish` record while carrying an unsupported state — the
		// exact reading that licenses a PUBLISH under an identifier already past PUBREC.
		//
		// encoding/json matches member names CASE-INSENSITIVELY, so a case variant of a
		// real field is not an "unknown field" to it. This row's value is UNSUPPORTED and
		// lands after the legal `state`, so under case-folding plus last-value-wins it is
		// the value that would be taken; the paired row below is the direction where the
		// SUPPORTED value wins, which is the one that decodes to a clean record.
		{"a case variant of a real field", `"STATE":"pubrel_ready"`},
		// ...and it permits DUPLICATES with the LAST value winning, so an unsupported
		// value can be smuggled in ahead of a supported one. Injected as a second `state`
		// after the legal one, which is the harder direction: the supported value is
		// first, so a decoder that takes the last value gets the unsupported one.
		{"a duplicate of a real field", `"state":"pubrel_ready"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if r, derr := mqttDecodeInflightRecord(inject(tc.extra)); derr == nil {
				t.Fatalf("an unrecognised key must make the record UNREADABLE, decoded to %+v", r)
			}
		})
	}

	// BOTH orders, for the duplicate and for the case variant, and this is where the
	// discriminating cases are. When the SUPPORTED value comes last, a case-folding or
	// last-value-wins decoder produces a perfectly clean `publish` record and nothing
	// downstream can tell it apart from a legitimate one — so these two bodies are the ones
	// that pass against the weaker boundary, and the rows above are not.
	for _, tc := range []struct{ name, body string }{
		// Both bodies carry a VALID lineage deliberately. Without one they would be refused
		// for the missing lineage instead of for the defect they exist to catch, and would
		// keep passing against a weakened key decoder — the assertion untouched, the test
		// proving nothing.
		{"a duplicate whose SUPPORTED value comes last",
			`{"pi":7,"dur":"d","seq":41,"qos":2,"state":"pubrel_ready","state":"publish",` +
				`"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx"}`},
		{"a case variant whose SUPPORTED value comes last",
			`{"pi":7,"dur":"d","seq":41,"qos":2,"STATE":"pubrel_ready","state":"publish",` +
				`"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx"}`},
	} {
		if r, derr := mqttDecodeInflightRecord([]byte(tc.body)); derr == nil {
			t.Errorf("%s must be refused: it decodes to a record indistinguishable from a "+
				"legitimate one while carrying an unsupported state, got %+v", tc.name, r)
		}
	}

	// TRAILING CONTENT, in all three forms. The first version checked Decoder.More, which
	// answers "is there another element in this array or object" — so it returns false when
	// the next byte is `]` or `}` and a record followed by a stray closing delimiter was
	// ACCEPTED. The boundary now requires the token stream to END.
	for _, tail := range []string{legal, "}", "]", "zzz", " \n\t"} {
		if r, derr := mqttDecodeInflightRecord([]byte(legal + tail)); derr == nil && tail != " \n\t" {
			t.Errorf("a record followed by %q must be refused, decoded to %+v", tail, r)
		}
	}
	// Trailing WHITESPACE is not content and must still decode: refusing it would reject
	// records this broker's own writers could legitimately produce.
	if _, derr := mqttDecodeInflightRecord([]byte(legal + " \n\t")); derr != nil {
		t.Errorf("trailing whitespace is not content: %v", derr)
	}
}

// TestInflightRecordKeySetIsDerivedFromTheStruct pins the one thing that would silently
// re-open the closed schema: the permitted key set is computed from the record's own JSON
// tags, so a field added later is accepted without anyone remembering a list.
//
// The inverse matters more. A hand-maintained list is what put three names that did not
// exist into an AST guard's allow-list elsewhere in this change, and a key set that drifts
// the other way would refuse records the broker itself writes.
func TestInflightRecordKeySetIsDerivedFromTheStruct(t *testing.T) {
	t.Parallel()
	keys := mqttInflightRecordJSONKeys()
	for _, want := range []string{"pi", "dur", "seq", "qos", "nonce", "state"} {
		if _, ok := keys[want]; !ok {
			t.Errorf("the key set omits %q, so a record this broker writes would be refused", want)
		}
	}
	// The five action / canonical-record keys RETURNED with the admission half that
	// produces them, so the key set must admit them — and must admit them under their
	// real tag spellings, which is the half a hand-maintained list gets wrong.
	for _, back := range []string{"action", "actclaim", "actclaimseq", "preldur", "prelseq"} {
		if _, ok := keys[back]; !ok {
			t.Errorf("the key set omits %q, so a record this broker now writes would be refused", back)
		}
	}
	// And nothing else. A key the struct does not declare must never be admitted, which
	// is the closed schema itself.
	for _, never := range []string{"action_claim", "pubrel_dur", "pubrel_seq", "ACTION", "actiontoken"} {
		if _, ok := keys[never]; ok {
			t.Errorf("the key set admits %q, which no struct tag declares", never)
		}
	}
	// Round-trip: every key the record MARSHALS to must be one the walk accepts, or the
	// broker would write records it cannot read back.
	body, err := json.Marshal(freshRegisterLineage(&mqttInflightRecord{
		PI: 7, Dur: "d", Seq: 41, QoS: 2, Nonce: "xxxxxxxxxxxxxxxxxxxxxx",
		State: mqttInflightPublish, Claim: "c", ClaimSeq: 3,
		Q2AffClaim: "a", Q2AffCopy: "b", Retired: true, Cancelled: true,
		ActionToken: "t", ActionClaim: "c", ActionClaimSeq: 3,
		PubRelDurable: "pd", PubRelStreamSeq: 9,
	}))
	if err != nil {
		t.Fatalf("marshal a fully populated record: %v", err)
	}
	var raw map[string]json.RawMessage
	if uerr := json.Unmarshal(body, &raw); uerr != nil {
		t.Fatalf("unmarshal it back to keys: %v", uerr)
	}
	if len(raw) == 0 {
		t.Fatal("a fully populated record marshalled to no keys, so this check proves nothing")
	}
	for k := range raw {
		if _, ok := keys[k]; !ok {
			t.Errorf("the record marshals key %q, which the key walk would refuse", k)
		}
	}
}

// TestInflightSlotPhaseGate pins the phase gate per disposition and per state, because it
// is what stops every path written when a register's life ended at PUBREC from vacating a
// slot mid-exchange.
//
// The unsupported-state refusal is asserted separately because a table full of known-state
// answers can obscure it: a record from elsewhere must authorize no disposition at all.
func TestInflightSlotPhaseGate(t *testing.T) {
	t.Parallel()

	// THE WHOLE MATRIX, one expectation per (disposition, state) pair. Written as a table
	// rather than as loops with exceptions, because the defect this gate exists to stop is
	// a single pair silently reading the wrong way, and a loop with an exception list
	// hides exactly that.
	type want struct {
		publish, publishTerminal, intended, ready, terminal bool
	}
	for _, tc := range []struct {
		name string
		d    mqttInflightDisposition
		w    want
	}{
		{
			// "Never transmitted" is false past PUBREC by construction: the client
			// answered under this identifier.
			name: "delete-never-transmitted", d: mqttSlotDeleteNeverTransmitted,
			w: want{publish: true},
		},
		{
			// Same claim, same refusal.
			name: "mark-cancelled", d: mqttSlotMarkCancelled,
			w: want{publish: true},
		},
		{
			// Source completion settles only a PUBLISH-phase slot. Terminal has a distinct
			// disposition so this path cannot bypass its record-side obligations.
			name: "delete-complete", d: mqttSlotDeleteComplete,
			w: want{publish: true},
		},
		{
			// PUBCOMP plus terminal cleanup's source/record/freshness proofs.
			name: "delete-terminal", d: mqttSlotDeleteTerminal,
			w: want{terminal: true},
		},
		{
			// The mark is about the SOURCE, not the phase.
			name: "mark-retired", d: mqttSlotMarkRetired,
			w: want{publish: true, publishTerminal: true, intended: true, ready: true, terminal: true},
		},
		{
			// An action is legal in EVERY phase now that the token is its own field, so
			// draining is a thing that may happen in any of them. What actually fences a
			// drain is the EXACT token match at the CAS boundary, not this gate.
			name: "drain-action", d: mqttSlotDrainAction,
			w: want{publish: true, publishTerminal: true, intended: true, ready: true, terminal: true},
		},
		{
			// An action is acquired to TRANSMIT, so the two TERMINAL phases are excluded:
			// nothing transmits from them.
			//
			// `pubrel_intended` IS an origin. The live path materializes an owed slot by
			// admitting a delivered record that proves the canonical identity, and that
			// admission installs the tuple and the action in ONE CAS — so refusing the
			// origin refused the arrow, and every exchange whose PUBREL had to be
			// recreated was stranded: admission declined, nothing transmitted, and the
			// client waited for a PUBREL that never came. The exact custody predicate is
			// tested at the CAS boundary.
			name: "acquire-action", d: mqttSlotAcquireAction,
			w: want{publish: true, intended: true, ready: true},
		},
		{
			// Quiescent PHASES, and pubrel_intended is one of them: C-III quantifies
			// custody over every supported quiescent slot, and an owed slot nobody can
			// take custody of can never be materialized. A live TOKEN is withheld by the
			// caller rather than refused here, which keeps the identifier reserved and
			// lets the rest of the session resume.
			name: "transfer-custody", d: mqttSlotTransferCustody,
			w: want{publish: true, intended: true, ready: true},
		},
		{
			// A fresh allocation may replace only a PUBLISH occupant for the same source;
			// replacing a later phase would erase its canonical/action identity.
			name: "replace-publish", d: mqttSlotReplacePublish,
			w: want{publish: true},
		},
		{
			// Every supported state is a legal ORIGIN for some arrow; which arrow is
			// legal is the CAS's business, since only it knows the target.
			name: "advance-phase", d: mqttSlotAdvancePhase,
			w: want{publish: true, publishTerminal: true, intended: true, ready: true, terminal: true},
		},
		{
			// The acknowledged terminal arrow: ONE origin, because a PUBACK ends a PUBLISH
			// phase and nothing else. Its identity and token predicates live at the CAS.
			name: "advance-acknowledged-terminal", d: mqttSlotAdvanceAcknowledgedTerminal,
			w: want{publish: true},
		},
		{
			// The PUBLISH branch of terminal, and ONLY that phase. It may not borrow the
			// PUBREL branch's disposition, whose duplicate scan, canonical acknowledgement
			// and affine disposal all presuppose a canonical record it never has.
			name: "delete-publish-terminal", d: mqttSlotDeletePublishTerminal,
			w: want{publishTerminal: true},
		},
		{
			// Materialization installs the canonical tuple from the two owed phases and
			// nowhere else. Whether an absence must be PROVED first is the caller's
			// obligation, not this gate's.
			name: "rebind-canonical", d: mqttSlotRebindCanonical,
			w: want{intended: true, ready: true},
		},
	} {
		for _, st := range []struct {
			state mqttInflightState
			want  bool
		}{
			{mqttInflightPublish, tc.w.publish},
			{mqttInflightPublishTerminal, tc.w.publishTerminal},
			{mqttInflightPubRelIntended, tc.w.intended},
			{mqttInflightPubRelReady, tc.w.ready},
			{mqttInflightPubRelTerminal, tc.w.terminal},
		} {
			if got := mqttInflightTransitionAllowed(tc.d, st.state); got != st.want {
				t.Errorf("%s from %q: allowed=%v, want %v", tc.name, st.state, got, st.want)
			}
		}
	}

	// AN UNSUPPORTED STATE AUTHORISES NOTHING, for every disposition. This is the clause
	// the per-state arms make easiest to lose — a table full of answers reads as though
	// the refusal came from the table rather than from the guard above it — so it is
	// asserted separately and against every disposition the type has.
	for _, st := range []mqttInflightState{_EMPTY_, "not-a-state", "PUBLISH", "pubrel"} {
		for d := mqttSlotDeleteComplete; d <= mqttSlotAdvancePhase; d++ {
			if mqttInflightTransitionAllowed(d, st) {
				t.Errorf("disposition %v must be refused on unsupported state %q", d, st)
			}
		}
	}
}

// TestPubRelOwnerTriState asserts each parser state SEPARATELY, so a change that folds
// absent into malformed (or either into valid) changes an observable value.
func TestPubRelOwnerTriState(t *testing.T) {
	t.Parallel()
	const nonce = "xxxxxxxxxxxxxxxxxxxxxx"
	for _, tc := range []struct {
		name  string
		hdr   string
		state mqttPubRelOwnerState
	}{
		{"no headers at all", "NATS/1.0\r\n\r\n", mqttPubRelOwnerAbsent},
		{"another header only", "NATS/1.0\r\nNmqtt-PubRel:7\r\n\r\n", mqttPubRelOwnerAbsent},
		{"one valid identity", "NATS/1.0\r\n" + mqttPubRelOwnerHeader + ":" + nonce + "\r\n\r\n",
			mqttPubRelOwnerValid},
		{"empty value", "NATS/1.0\r\n" + mqttPubRelOwnerHeader + ":\r\n\r\n",
			mqttPubRelOwnerMalformed},
		{"wrong length", "NATS/1.0\r\n" + mqttPubRelOwnerHeader + ":short\r\n\r\n",
			mqttPubRelOwnerMalformed},
		{"foreign alphabet", "NATS/1.0\r\n" + mqttPubRelOwnerHeader + ":!!!!!!!!!!!!!!!!!!!!!!\r\n\r\n",
			mqttPubRelOwnerMalformed},
		{"two occurrences", "NATS/1.0\r\n" + mqttPubRelOwnerHeader + ":" + nonce + "\r\n" +
			mqttPubRelOwnerHeader + ":" + nonce + "\r\n\r\n", mqttPubRelOwnerMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mqttParsePubRelOwner([]byte(tc.hdr))
			if got.State != tc.state {
				t.Fatalf("state = %v, want %v (owner %+v)", got.State, tc.state, got)
			}
			if (got.State == mqttPubRelOwnerValid) != got.valid() {
				t.Errorf("valid() disagrees with the state: %+v", got)
			}
		})
	}
}

// TestMarkerLessDrainIntentQuarantineSurvivesSubscribeCycles walks the exact bypass two
// review rounds found, one cycle at a time, because each round's fix opened the next.
//
// A past-PUBREC drain intent with no usable exchange identity is quarantined on restore.
// Round 7 found that adoption erased it: an ordinary QoS>0 re-SUBSCRIBE removed the drain
// from both maps and a later save omitted the intent. Round 8 found that refusing adoption
// merely moved the bypass one cycle out — a fresh consumer under the same sid, then a
// SECOND unsubscribe, staged a new drain into the same map slot and orphaned the
// quarantined one.
//
// So this asserts the property both fixes exist for: the quarantine owns its sid, and no
// subscription cycle can take it away.
func TestMarkerLessDrainIntentQuarantineSurvivesSubscribeCycles(t *testing.T) {
	t.Parallel()
	const sid = "1"
	const dur = "$MQTT_plain_marker_less"

	persisted := &mqttPersistedDrain{
		Cons:    &ConsumerConfig{Durable: dur},
		Subject: "q2/marker/less",
		QoS:     2,
		Started: []mqttPersistedDrainSeq{{
			// Past PUBREC — PubRel with a packet identifier — and NO OwnerNonce, which is a
			// shape this broker cannot write: recordPubRelIntent validates the identity
			// before it saves.
			Seq: 41, PubRel: true, PI: 7,
		}},
	}

	d := mqttDrainFromPersisted(persisted)
	if d == nil {
		t.Fatal("the drain must be RESTORED, not dropped: dropping it accepts the session " +
			"without a drain whose consumer and pending exchange are still stranded")
	}
	st := d.seqs[41]
	if st == nil || !st.intent {
		t.Fatalf("the past-PUBREC intent must be restored, got %+v", st)
	}
	if !st.ownerUnusable {
		t.Fatal("an intent with no usable exchange identity must be quarantined")
	}
	if st.ownerNonce != _EMPTY_ {
		t.Errorf("ownerNonce = %q, want empty", st.ownerNonce)
	}

	sess := &mqttSession{
		drains:     map[string]*mqttDrain{sid: d},
		drainByDur: map[string]*mqttDrain{dur: d},
		cpending:   map[string]map[uint64]uint16{dur: {41: 7}},
	}

	// Cycle one: a re-SUBSCRIBE must NOT adopt the quarantined drain away.
	if sess.adoptDrainLocked(sid) {
		t.Fatal("adoption must be refused while a sequence is quarantined: adopting converts " +
			"the consumer back to a membership and the next save omits the intent")
	}
	if sess.drains[sid] != d {
		t.Fatal("the quarantined drain must still own its sid after a refused adoption")
	}
	if sess.drainByDur[dur] != d {
		t.Fatal("the quarantined drain must still be reachable by durable")
	}

	// Cycle two: a fresh consumer under the same sid, then a SECOND unsubscribe. Staging
	// must not take the sid from the quarantined drain.
	fresh := &ConsumerConfig{Durable: "$MQTT_plain_fresh"}
	sess.cpending[fresh.Durable] = map[uint64]uint16{99: 8}
	if sess.stageDrain(sid, fresh, nil) {
		t.Fatal("staging must be refused while a quarantined drain owns this sid: otherwise the " +
			"new drain takes the map slot and the record, serialized by that map, loses the intent")
	}
	if got := sess.drains[sid]; got != d {
		t.Fatalf("the quarantined drain lost its sid to a later stage: %p vs %p", got, d)
	}
	if sess.drains[sid].seqs[41] == nil || !sess.drains[sid].seqs[41].ownerUnusable {
		t.Fatal("the quarantine must survive the whole cycle")
	}

	// And the record built from this session still carries the intent, which is what a
	// restart reads. Losing it here is what let the source be retransmitted.
	if len(d.startedSeqs()) == 0 {
		t.Fatal("the persisted Started list must still carry the quarantined intent")
	}

	// Cycle three, and the property the first two fixes were symptoms of: the quarantine
	// OWNS its sid, so nothing may provision alongside it either. Round 9 found that the
	// dual drain-and-live-consumer state the refusals left behind cannot be represented by
	// fork repair — mqttGrantFromRecord treats Drains[sid] and Cons[sid] as exclusive and
	// drops the consumer — so a fresh durable's started deliveries would be keyed to a
	// durable the merge omitted.
	if got := sess.drainSidHoldLocked(sid); got != mqttDrainHeldUnusableIdentity {
		t.Fatalf("the sid must report an unusable identity, which is what every occupying "+
			"path asks and what names the operator action, got %v", got)
	}
	if got := sess.drainSidHoldLocked("some-other-sid"); got != mqttDrainNotHeld {
		t.Errorf("an unrelated sid must not be held, got %v", got)
	}
}

// TestDrainHoldIsScopedToItsOwnSequences keeps the property honest in the negative
// direction: a drain carrying NO past-PUBREC intent must not be held, or an ordinary
// re-SUBSCRIBE would be refused and the fix would be worse than the defect.
//
// The fixture's premise CHANGED with F-094 and the change is the point. It used to build
// this "ordinary" drain with a claimed valid intent — {PubRel: true, PI: 7, OwnerNonce} —
// and assert that it adopts. That is exactly the adoption F-094 says must not happen: it
// drops the drain and its whole seqs map, after which the unacknowledged source is re-offered
// on the ordinary path as a fresh PUBLISH of an exchange the client has already answered.
// So the intent case moved to TestAdoptionRefusedWhileADrainCarriesAPastPubRecIntent and
// this test keeps only PUBLISH-phase claims, which is what "ordinary" actually means.
func TestDrainHoldIsScopedToItsOwnSequences(t *testing.T) {
	t.Parallel()
	const sid, dur = "1", "$MQTT_plain_ok"
	d := mqttDrainFromPersisted(&mqttPersistedDrain{
		Cons: &ConsumerConfig{Durable: dur}, QoS: 2,
		Started: []mqttPersistedDrainSeq{
			{Seq: 41, PI: 7}, // started, not past PUBREC
			{Seq: 42, PI: 8},
		},
	})
	if d == nil {
		t.Fatal("a supported drain must restore")
	}
	for _, seq := range []uint64{41, 42} {
		if st := d.seqs[seq]; st == nil || st.intent {
			t.Fatalf("seq %d must restore as a PUBLISH-phase claim, or this test proves "+
				"nothing about the negative direction: %+v", seq, st)
		}
	}
	sess := &mqttSession{
		drains: map[string]*mqttDrain{sid: d}, drainByDur: map[string]*mqttDrain{dur: d},
		cpending: map[string]map[uint64]uint16{dur: {41: 7}},
	}
	if got := sess.drainSidHoldLocked(sid); got != mqttDrainNotHeld {
		t.Fatalf("a drain owing no PUBREL must not be held, got %v", got)
	}
	if !sess.adoptDrainLocked(sid) {
		t.Fatal("an ordinary re-SUBSCRIBE must still adopt its drain")
	}
	if sess.cons[sid] == nil {
		t.Error("adoption must hand the consumer back as an ordinary membership")
	}
}

// TestQuarantineRefusesEverySidOfAWildcardFilter is the paired-SID case, and it exists
// because the single-sid property test did not reach it.
//
// A wildcard such as `foo/#` owns a main sid AND a level-up companion. A per-sid refusal
// processed them independently, so a USABLE main drain was adopted into sess.cons and only
// the companion then failed — and the per-filter failure cleanup does not put an adopted
// drain back, while the persisted record omits a consumer with no membership. The next
// successful save therefore dropped that drain's Started obligations and durable identity.
//
// The property is that the FILTER is the atomicity boundary: if ANY of its sids is
// quarantined, none of them is touched.
func TestQuarantineRefusesEverySidOfAWildcardFilter(t *testing.T) {
	t.Parallel()
	const mainSid = "foo.>"
	companion := mainSid[:len(mainSid)-2] + mqttMultiLevelSidSuffix
	if !mqttNeedSubForLevelUp(mainSid) {
		t.Fatalf("%q must own a level-up companion for this test to mean anything", mainSid)
	}
	sids := mqttUnsubSids(mainSid)
	if len(sids) != 2 || sids[0] != mainSid || sids[1] != companion {
		t.Fatalf("mqttUnsubSids(%q) = %v, want the main sid and its companion", mainSid, sids)
	}

	// A PUBLISH-phase claim, deliberately. Before F-094 this drain carried a valid claimed
	// intent and the test asserted it was NOT quarantined; the widened hold makes every
	// past-PUBREC intent hold its sid, so that fixture would now be held for its own reason
	// and the companion would prove nothing. The subject here is wildcard ATOMICITY — a
	// usable main sid must not be adopted when the companion is refused — so the main drain
	// has to be genuinely unheld.
	usable := mqttDrainFromPersisted(&mqttPersistedDrain{
		Cons: &ConsumerConfig{Durable: "$MQTT_main_ok"}, QoS: 2,
		Started: []mqttPersistedDrainSeq{{Seq: 41, PI: 7}},
	})
	quarantined := mqttDrainFromPersisted(&mqttPersistedDrain{
		Cons: &ConsumerConfig{Durable: "$MQTT_companion_markerless"}, QoS: 2,
		Started: []mqttPersistedDrainSeq{{Seq: 42, PubRel: true, PI: 8}}, // no OwnerNonce
	})
	if usable == nil || quarantined == nil {
		t.Fatal("both drains must restore")
	}
	sess := &mqttSession{
		drains: map[string]*mqttDrain{mainSid: usable, companion: quarantined},
	}

	// The main sid alone looks fine — which is exactly how the partial adoption happened.
	if got := sess.drainSidHoldLocked(mainSid); got != mqttDrainNotHeld {
		t.Fatalf("the main drain owes no PUBREL and must not be held on its own, got %v", got)
	}
	if got := sess.drainSidHoldLocked(companion); got != mqttDrainHeldUnusableIdentity {
		t.Fatalf("the companion drain must report an unusable identity, got %v", got)
	}

	// So the filter-level question is the one that has to be asked, and it must refuse.
	any := false
	for _, sid := range mqttUnsubSids(mainSid) {
		if sess.drainSidHoldLocked(sid) != mqttDrainNotHeld {
			any = true
		}
	}
	if !any {
		t.Fatal("a filter with ANY quarantined sid must be refused as a whole")
	}

	// And nothing was adopted while establishing that: both drains still own their sids.
	if sess.drains[mainSid] != usable || sess.drains[companion] != quarantined {
		t.Fatal("checking must not adopt or move either drain")
	}
	if sess.cons != nil {
		t.Fatalf("no consumer may be installed by the check, got %v", sess.cons)
	}
}

// TestMQTTInflightRecordsAreDecodedThroughOneAPI holds the READ boundary closed.
//
// A holistic round found shape validation added to the recovery scan and nowhere else, so
// mqttSlotAdmitsDisposition, the generic gated reap and the generation teardown all still
// authorized irreversible work — a register delete, a claim release, a consumer removal —
// from a body that was identity-valid and otherwise illegal. The finding named the real
// defect: enumerating the destructive readers never closes, because the next reader is
// always one call site further out.
//
// So the rule is a property of the DECODER, and this guard is what makes it a property
// rather than a convention: exactly one production function may turn `$MQTT_inflight` bytes
// into a record, and it validates. Any new reader inherits the refusal by construction.
//
// The pattern matched is an `mqttInflightRecord` handed to a DECODE call — `Unmarshal` or a
// decoder's `Decode` — as a named variable or inline. That is narrower than "any record
// literal", which would flag every WRITER that fills a record from arguments; a writer is
// governed by the other boundary, storeInflightCAS.
//
// Both call forms are matched because the decoder changed from json.Unmarshal to a
// json.Decoder when the schema was closed with DisallowUnknownFields, and an
// Unmarshal-only guard went instantly VACUOUS — it found zero decodes and its own
// "checks nothing" assertion is what caught that. A guard that silently stops matching is
// worse than no guard, so the seen counter below is load-bearing rather than a nicety.
func TestMQTTInflightRecordsAreDecodedThroughOneAPI(t *testing.T) {
	t.Parallel()
	owners := map[string]struct{}{"mqttDecodeInflightRecord": {}}
	seen := 0
	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, _ := decl.(*ast.FuncDecl)
			if fn == nil || fn.Body == nil {
				continue
			}
			qualified := testQualifiedFuncName(fn)
			_, owner := owners[qualified]

			// Names in THIS function bound to an mqttInflightRecord, so a two-step
			// decode — build the record, unmarshal into it later — is caught as well
			// as an inline one.
			//
			// All four binding forms, and the list is exhaustive rather than
			// representative because a guard that misses one is a guard that permits
			// it. A round-13 finding caught exactly that: only a bare-identifier
			// target was resolved, so `r := mqttInflightRecord{}` followed by
			// `json.Unmarshal(data, &r)` — the most ordinary form there is — passed.
			//
			//	r := mqttInflightRecord{}        // composite literal
			//	r := &mqttInflightRecord{}       // address of one
			//	var r mqttInflightRecord         // typed declaration, no value
			//	r := new(mqttInflightRecord)     // new()
			//	func f(r *mqttInflightRecord)    // a typed PARAMETER
			//
			// The parameter form is the fifth, and a later round found it missing: a helper
			// taking `r *mqttInflightRecord` and unmarshalling into it introduced no local
			// binding at all, so the guard saw nothing. It is the most natural shape for
			// someone factoring a decode out, which makes it the one most likely to be
			// written next.
			bound := map[string]bool{}
			for _, field := range fn.Type.Params.List {
				if !testNamesInflightRecordType(field.Type) {
					continue
				}
				for _, id := range field.Names {
					bound[id.Name] = true
				}
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				var names []ast.Expr
				var values []ast.Expr
				switch d := n.(type) {
				case *ast.AssignStmt:
					names, values = d.Lhs, d.Rhs
				case *ast.ValueSpec:
					for _, id := range d.Names {
						names = append(names, id)
					}
					// `var r mqttInflightRecord` has a TYPE and no value, which is a
					// perfectly good decode target.
					if len(d.Values) == 0 && testNamesInflightRecordType(d.Type) {
						for _, id := range d.Names {
							bound[id.Name] = true
						}
						return true
					}
					values = d.Values
				default:
					return true
				}
				for i, v := range values {
					if i >= len(names) {
						break
					}
					if !testNamesInflightRecord(v) {
						continue
					}
					if id, _ := names[i].(*ast.Ident); id != nil {
						bound[id.Name] = true
					}
				}
				return true
			})

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, _ := n.(*ast.CallExpr)
				// Unmarshal takes (data, target); Decode takes (target) alone.
				if call == nil || len(call.Args) == 0 || len(call.Args) > 2 {
					return true
				}
				sel, _ := call.Fun.(*ast.SelectorExpr)
				if sel == nil {
					return true
				}
				switch sel.Sel.Name {
				case "Unmarshal":
					// Package-qualified: json.Unmarshal(data, target).
					if pkg, _ := sel.X.(*ast.Ident); pkg == nil || pkg.Name != "json" {
						return true
					}
				case "Decode":
					// A decoder method, so the receiver is a value rather than the package
					// name. Not qualified further: any Decode into this record type is a
					// decode of a persisted register whatever the codec.
				default:
					return true
				}
				// The target is resolved THROUGH an address-of, which is what
				// `json.Unmarshal(data, &r)` is: unwrapping first and then asking
				// whether the name is bound is the step the round-13 finding named.
				target := call.Args[len(call.Args)-1]
				if u, _ := target.(*ast.UnaryExpr); u != nil && u.Op == token.AND {
					target = u.X
				}
				id, _ := target.(*ast.Ident)
				if !testNamesInflightRecord(target) && (id == nil || !bound[id.Name]) {
					return true
				}
				seen++
				if !owner {
					t.Errorf("%s: %s decodes persisted bytes into an mqttInflightRecord directly; "+
						"it must go through mqttDecodeInflightRecord, which is the ONE place the "+
						"closed schema and the legal-shape matrix are applied on read",
						fset.Position(call.Pos()), qualified)
				}
				return true
			})
		}
	}
	if seen == 0 {
		t.Error("no register decode was found at all, so this guard checks nothing")
	}
}

// testNamesInflightRecord reports whether an expression YIELDS an mqttInflightRecord: a
// literal, the address of one, or new(mqttInflightRecord).
func testNamesInflightRecord(e ast.Expr) bool {
	if u, _ := e.(*ast.UnaryExpr); u != nil && u.Op == token.AND {
		e = u.X
	}
	if lit, _ := e.(*ast.CompositeLit); lit != nil {
		return testNamesInflightRecordType(lit.Type)
	}
	if call, _ := e.(*ast.CallExpr); call != nil && len(call.Args) == 1 {
		if fn, _ := call.Fun.(*ast.Ident); fn != nil && fn.Name == "new" {
			return testNamesInflightRecordType(call.Args[0])
		}
	}
	return false
}

// testNamesInflightRecordType reports whether a type expression names the record type,
// through a pointer if need be.
func testNamesInflightRecordType(e ast.Expr) bool {
	if st, _ := e.(*ast.StarExpr); st != nil {
		e = st.X
	}
	id, _ := e.(*ast.Ident)
	return id != nil && id.Name == "mqttInflightRecord"
}

// TestStoredPubRelConsumersAllConsultTheVerdict closes the class round 21 found, rather
// than the instance.
//
// scanPubRelRecords deliberately RETURNS every record with its decode verdict attached,
// because the callers' dispositions differ — a reader that would derive something skips an
// unsupported record, while the affine-proof walk and the generation teardown must see what
// they are about to destroy. That design has one failure mode: a caller that reads the
// record's fields and never consults the verdict. Round 21 found exactly that in BOTH affine
// consumers, where a well-formed affine reference on a record whose identifier, owner or
// source correlation was illegal could record PubRecProven and reap an assignment claim.
//
// Four consumers is a small set today, which is precisely when a list becomes a guard: the
// fifth is the one nobody remembers. So the rule is mechanical — a function that ranges over
// this scan's results must mention shapeErr somewhere in its body.
//
// WHAT THIS GUARD DOES NOT CATCH, stated because a guard read as stronger than it is, is
// worse than none: it detects an ABSENT check, not a DISABLED one. A mutation reducing a
// consumer's check to `if false && r.shapeErr != nil` still mentions the field and passes
// here. That mutation is caught by the behavioural tests instead — the two affine routes
// have their own — and deleting the check outright, which is what a new consumer actually
// looks like, is caught here.
func TestStoredPubRelConsumersAllConsultTheVerdict(t *testing.T) {
	t.Parallel()
	seen := 0
	for _, file := range testMQTTProductionFiles(t) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, _ := decl.(*ast.FuncDecl)
			if fn == nil || fn.Body == nil {
				continue
			}
			qualified := testQualifiedFuncName(fn)
			if qualified == "mqttSession.scanPubRelRecords" {
				continue // the producer itself
			}
			calls, consults := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, _ := n.(*ast.SelectorExpr); sel != nil {
					switch sel.Sel.Name {
					case "scanPubRelRecords":
						calls = true
					case "shapeErr":
						consults = true
					}
				}
				return true
			})
			if !calls {
				continue
			}
			seen++
			if !consults {
				t.Errorf("%s: %s consumes scanPubRelRecords without consulting shapeErr; an "+
					"unsupported record's fields may not authorize anything, and the scan returns "+
					"such records deliberately so each caller can apply its OWN disposition",
					fset.Position(fn.Pos()), qualified)
			}
		}
	}
	if seen == 0 {
		t.Error("no consumer of scanPubRelRecords was found at all, so this guard checks nothing")
	}
	if seen < 4 {
		t.Errorf("only %d consumer(s) found; the known set is four — register recovery, drain "+
			"recovery, the affine proof walk and the generation teardown — so the matcher has "+
			"stopped seeing some of them", seen)
	}
}

// A MANDATORY REGISTER MEMBER MAY NOT BE NULL (F-098 compatibility boundary, holistic review
// round 6, finding 5). The presence walk accepted `"acq":null`, and the typed decode then turns
// it into the zero value — which for the transmission counter is a REAL value, the one a fresh
// allocation carries. An unsupported record would be handed a new exchange's identity, and the
// next save would write it back as an explicit zero: migration by normalization.
//
// Explicit numeric zero stays supported, and that pairing is the point: only the unwritable
// spelling is refused.
//
// Mutation M132: accept null again.
func TestInflightRecordRequiresNonNullMandatoryMembers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		body     string
		wantErr  bool
		whyClaim string
	}{
		{
			name:     "a null counter is refused",
			body:     `{"pi":7,"dur":"SRC","seq":41,"qos":2,"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx","claim":"yyyyyyyyyyyyyyyyyyyyyy","claimseq":3,"acq":null,"state":"publish"}`,
			wantErr:  true,
			whyClaim: "null decodes to a counter this broker writes for a fresh allocation",
		},
		{
			name:    "an explicit zero counter is accepted",
			body:    `{"pi":7,"dur":"SRC","seq":41,"qos":2,"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx","claim":"yyyyyyyyyyyyyyyyyyyyyy","claimseq":3,"acq":0,"state":"publish"}`,
			wantErr: false,
		},
		{
			name:     "an absent counter is refused",
			body:     `{"pi":7,"dur":"SRC","seq":41,"qos":2,"nonce":"xxxxxxxxxxxxxxxxxxxxxx","lin":"xxxxxxxxxxxxxxxxxxxxxx","claim":"yyyyyyyyyyyyyyyyyyyyyy","claimseq":3,"state":"publish"}`,
			wantErr:  true,
			whyClaim: "absence and zero are indistinguishable after the typed decode",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := mqttJSONMembersArePresent([]byte(tc.body), mqttInflightRecordRequiredJSONKeys, "register")
			if tc.wantErr && err == nil {
				t.Fatalf("the shape was accepted: %s", tc.whyClaim)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("a supported shape was refused: %v", err)
			}
		})
	}
}
