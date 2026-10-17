package mqtt

import (
	"math"
	"reflect"
	"testing"

	"github.com/nats-io/nuid"
)

// TestSlotReleaseAuthorityPrecedence is C-IV's oracle (F-098).
//
// The contract is a TOTAL FUNCTION with a fixed precedence and a RESERVING DEFAULT, and
// the default is the part that makes it a property rather than the enumeration three
// review rounds dismantled: a shape nobody enumerated falls to the last arm and holds the
// identifier. So this table deliberately includes shapes with no interesting name — they
// exist to prove the default catches them, not because anyone predicted them.
//
// Two rows carry corrections the design consult made to the original candidate, and
// losing either reopens a real hole. They are marked.
func TestSlotReleaseAuthorityPrecedence(t *testing.T) {
	t.Parallel()
	const (
		nonce = "xxxxxxxxxxxxxxxxxxxxxx"
		other = "yyyyyyyyyyyyyyyyyyyyyy"
		tokn  = "zzzzzzzzzzzzzzzzzzzzzz"
		subj  = "$MQTT.inflight.abcdefgh.d0GStWoz33Et7Nq33AW43W.7"
	)
	base := func() *mqttInflightRecord {
		return &mqttInflightRecord{
			SourceAckReply: testInflightAckSubject("$MQTT_msgs_x", 41, 1),
			PI:             7, Dur: "$MQTT_msgs_x", Seq: 41, QoS: 2, State: mqttInflightPublish,
			Nonce: nonce, Lin: nonce, Claim: other, Acq: 3,
		}
	}
	bound := mqttSlotIdentity{subject: subj, nonce: nonce, acq: 3}

	for _, tc := range []struct {
		name   string
		build  func(*mqttSlotEvidence)
		want   mqttSlotAuthority
		reason string
	}{
		{
			name:  "unreadable withholds",
			build: func(ev *mqttSlotEvidence) { ev.readable, ev.rec = false, nil },
			want:  mqttSlotWithhold,
		},
		{
			name:  "an unsupported state withholds",
			build: func(ev *mqttSlotEvidence) { ev.rec.State = "somewhere-else" },
			want:  mqttSlotWithhold,
		},
		{
			name:  "an indeterminate marker withholds",
			build: func(ev *mqttSlotEvidence) { ev.marker = mqttSlotMarkerIndeterminate },
			want:  mqttSlotWithhold,
		},
		{
			// C-I at the authority boundary: B holds evidence naming Acq=k while the
			// occupant is at k+1, because A installed, transmitted and drained in
			// between. Nothing moved between B's read and its write, so no freshness
			// fence catches this — a sequence is not an identity.
			name:  "a stale transmission identity withholds",
			build: func(ev *mqttSlotEvidence) { ev.bound.acq = 2 },
			want:  mqttSlotWithhold,
		},
		{
			name: "ANY live token withholds, over otherwise-valid unsent proof",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.ActionToken, ev.rec.ActionClaim, ev.rec.ActionClaimSeq = tokn, other, 0
				ev.marker = mqttSlotMarkerPresent
			},
			want: mqttSlotWithhold,
		},
		{
			name: "a durable pubrel_terminal phase is terminal authority",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.State = mqttInflightPubRelTerminal
				ev.rec.PubRelDurable, ev.rec.PubRelStreamSeq = "PUBREL", 9
				ev.marker = mqttSlotMarkerAbsent
			},
			want: mqttSlotTerminalAuthority,
		},
		{
			// Arm 3's second member. A rule naming only pubrel_terminal sends an
			// acknowledged QoS 1 exchange to Reserved forever, and at Receive Maximum 1
			// that stops the session receiving anything at all.
			name: "a durable publish_terminal phase is terminal authority",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.State = mqttInflightPublishTerminal
				ev.marker = mqttSlotMarkerAbsent
			},
			want: mqttSlotTerminalAuthority,
		},
		{
			name:  "a present marker on a publish slot is proven-unsent",
			build: func(ev *mqttSlotEvidence) { ev.marker = mqttSlotMarkerPresent },
			want:  mqttSlotProvenUnsentAuthority,
		},
		{
			// RE-EXPRESSED by F-097/F-088 (plan rev 10, WP3 edit 1). This case asserted that a
			// Cancelled bit is proof of non-transmission; that premise is the defect F-088 owns
			// (a cancellation is a local decision that cannot see a PUBLISH already on the wire),
			// and the plan deletes it. With the marker authoritatively ABSENT the exchange is
			// possibly-attempted, whatever the bit says.
			name: "a cancellation on the record is NOT proven-unsent",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.Cancelled, ev.marker = true, mqttSlotMarkerAbsent
			},
			want: mqttSlotReservedAuthority,
		},
		{
			// The genuine never-transmitted cancellation still releases: its marker stands.
			name: "a cancellation whose marker stands is proven-unsent by the marker",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.Cancelled, ev.marker = true, mqttSlotMarkerPresent
			},
			want: mqttSlotProvenUnsentAuthority,
		},
		{
			// L3's crash survivor: publish + Cancelled + no marker + no token. RE-EXPRESSED by
			// F-097/F-088 (plan rev 10, WP3): this shape was freed on the Cancelled bit alone,
			// which is the deleted premise. It is now RESERVED and served instead: the register
			// survives, a late acknowledgement completes the exchange through its ACK route,
			// and the ledger holds identifier and charge until then or discard. (The case that
			// asserted the persisted VoidedRegs verdict as proof is removed with the
			// evidence member it set; the verdict is no longer evidence at all.)
			name: "L3's pure-cancellation survivor is reserved, not freed",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.Cancelled, ev.rec.CancelOrigin = true, mqttCancelOriginOccupant // F-088: provenance
				ev.marker = mqttSlotMarkerAbsent
			},
			want: mqttSlotReservedAuthority,
		},
		{
			// THE SECOND CORRECTION. A PUBLISH can reach the client and be successfully
			// PUBREC'd while the slot still says `publish`, with the retirement recorded
			// and a cancellation surviving over it. Retirement is backed by the client's
			// own PUBREC, so the transmission provably happened; treating either fact as
			// unsent authority frees an outstanding exchange.
			name: "RETIRED DOMINATES a cancellation and reserves",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.Retired, ev.rec.Cancelled = true, true
				ev.rec.CancelOrigin = mqttCancelOriginOccupant // F-088: provenance
				ev.marker = mqttSlotMarkerAbsent
			},
			want: mqttSlotReservedAuthority,
		},
		{
			name: "RETIRED DOMINATES a present marker and reserves",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.Retired, ev.marker = true, mqttSlotMarkerPresent
			},
			want: mqttSlotReservedAuthority,
		},
		{
			// THE DEFAULT, and the reason the contract is a property. A transmitted
			// exchange whose acknowledgement never came back matches no positive arm,
			// and holding its identifier is the safe direction.
			name:  "possibly-attempted reserves",
			build: func(ev *mqttSlotEvidence) { ev.marker = mqttSlotMarkerAbsent },
			want:  mqttSlotReservedAuthority,
		},
		{
			// A past-PUBREC phase is never proven-unsent however the evidence reads:
			// the client answered under this identifier.
			name: "a pubrel_ready slot with a present marker still reserves",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.State = mqttInflightPubRelReady
				ev.rec.PubRelDurable, ev.rec.PubRelStreamSeq = "PUBREL", 9
				ev.marker = mqttSlotMarkerPresent
			},
			want: mqttSlotReservedAuthority,
		},
		{
			// An owed slot is RESERVED for release purposes and that is not a stall:
			// materialization is a separate permission and is not gated on this function.
			name: "an owed pubrel_intended slot reserves",
			build: func(ev *mqttSlotEvidence) {
				ev.rec.State = mqttInflightPubRelIntended
				ev.marker = mqttSlotMarkerAbsent
			},
			want: mqttSlotReservedAuthority,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := mqttSlotEvidence{readable: true, rec: base(), bound: bound, marker: mqttSlotMarkerAbsent}
			tc.build(&ev)
			if got := mqttSlotReleaseAuthority(ev); got != tc.want {
				t.Fatalf("authority = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSlotEvidenceCannotIdentifyATokensOwner is how the "ANY live token, including the
// caller's own" half of arm 2 is actually held (F-098), and it is structural rather than
// behavioural for a reason worth stating.
//
// THE SCHEDULE arm 2 exists to stop: this broker owns a PUBLISH action whose socket write
// is still running; another goroutine observes terminal evidence, or the still-present
// unsent marker, disregards the token BECAUSE IT IS "OURS", and frees the identifier
// while the writer can still transmit.
//
// A table row cannot catch that. mqttSlotReleaseAuthority takes no owner term, so every
// token looks alike to it and a row claiming to test "our own token" tests the foreign
// one twice — which is exactly the hole a first draft of the sibling table had. The
// property that really holds the line is that THE EVIDENCE CANNOT EXPRESS THE EXEMPTION:
// there is no field a caller could set to say "this one is mine, let it through".
//
// So this asserts the shape. Adding such a field — the first move anyone optimizing this
// path would make — fails here, and the reviewer is sent to arm 2's comment.
func TestSlotEvidenceCannotIdentifyATokensOwner(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(mqttSlotEvidence{})
	want := map[string]struct{}{
		// "verdict" is gone with F-088's WP3 edit 1: a void verdict is a decision, not
		// evidence of non-transmission, so the evidence no longer carries it.
		"readable": {}, "rec": {}, "bound": {}, "marker": {},
	}
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if _, ok := want[name]; !ok {
			t.Fatalf("mqttSlotEvidence gained field %q: if it names an action token's OWNER, "+
				"arm 2 can be given an exemption for the caller's own live write, which is "+
				"the release this contract exists to refuse. See mqttSlotReleaseAuthority arm 2",
				name)
		}
		delete(want, name)
	}
	for name := range want {
		t.Fatalf("mqttSlotEvidence lost field %q, so this guard no longer describes it", name)
	}
}

// TestInflightSlotTransmissionCounterIsMonotone is C-I's monotonicity oracle (F-098).
//
// `Acq` is what makes an acknowledgement bound to one transmission undischargeable by a
// later one, and that only holds if the counter NEVER GOES BACKWARDS. A drain in
// particular must not reset it: a reset hands the next transmission an identity an
// earlier one's evidence already names, and every release bound to that identity then
// discharges the wrong exchange — which is the staleness the whole contract exists to
// refuse, reintroduced by a single line in a mutator.
//
// The rule is enforced in ONE clause at `casInflightSlot`, the door every slot rewrite
// passes through: a rewrite either leaves the counter alone or advances it by exactly
// one. That single test covers a decrease, a jump, and overflow — MaxUint64 wraps to
// zero, which is a decrease.
//
// Every row drives the REAL CAS against a stored record, so a mutation of the guard is
// observable as a landed or refused write rather than as a predicate's return value.
func TestInflightSlotTransmissionCounterIsMonotone(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-acq-monotone")
	dur := e.sess.idHash + "_" + nuid.Next()

	// A DISTINCT IDENTIFIER PER ROW, so no row has to delete another's record and each
	// starts from a known counter on a subject nothing else has touched.
	seed := func(t *testing.T, pi uint16, acq uint64) (*mqttInflightSlotRef, uint64) {
		t.Helper()
		subject := mqttInflightSubject(e.sess.idHash, e.sess.gen, pi)
		rec := freshRegisterLineage(&mqttInflightRecord{
			State: mqttInflightPublish, PI: pi, Dur: dur, Seq: 41, QoS: 2,
			Nonce: mqttNewInflightNonce(), Acq: acq,
		})
		seq, err := e.asm.jsa.storeInflightCAS(subject, 0, rec)
		if err != nil {
			t.Fatalf("seed at acq=%d: %v", acq, err)
		}
		ref := mqttSlotRefFromRecord(subject, seq, rec)
		if ref == nil {
			t.Fatal("the seeded record must yield a slot reference")
		}
		return ref, seq
	}

	row := 0
	for _, tc := range []struct {
		name string
		from uint64
		set  func(*mqttInflightRecord)
		want bool // the CAS must land
	}{
		{
			// THE POSITIVE CONTROL, and without it every refusal below would pass
			// against a CAS that refused everything.
			name: "an advance of exactly one lands",
			from: 3, set: func(r *mqttInflightRecord) { r.Acq++ }, want: true,
		},
		{
			// An ordinary mark or custody write touches the counter not at all, and
			// must not be refused for it.
			name: "leaving the counter alone lands",
			from: 3, set: func(r *mqttInflightRecord) { r.Retired = true }, want: true,
		},
		{
			name: "a RESET is refused — this is the drain mutation",
			from: 3, set: func(r *mqttInflightRecord) { r.Acq = 0 }, want: false,
		},
		{
			name: "any decrease is refused",
			from: 3, set: func(r *mqttInflightRecord) { r.Acq = 2 }, want: false,
		},
		{
			name: "a JUMP is refused: a mutator counted an acquisition nobody made",
			from: 3, set: func(r *mqttInflightRecord) { r.Acq += 2 }, want: false,
		},
		{
			// OVERFLOW IS REFUSED RATHER THAN WRAPPED. Wrapping produces the same
			// identity collision by another route, and there is no correct value to
			// write, so the rewrite fails and the identifier stays reserved.
			name: "overflow is refused, not wrapped",
			from: math.MaxUint64, set: func(r *mqttInflightRecord) { r.Acq++ }, want: false,
		},
	} {
		row++
		pi := uint16(row)
		t.Run(tc.name, func(t *testing.T) {
			ref, seededSeq := seed(t, pi, tc.from)
			_, ok := e.sess.casInflightSlot(e.c, *ref, mqttInflightPublish, mqttSlotAdvancePhase, tc.set)
			if ok != tc.want {
				t.Fatalf("CAS landed=%v, want %v", ok, tc.want)
			}
			cur, smsg := e.loadRegister(t, pi)
			if cur == nil || smsg == nil {
				t.Fatal("the record must survive either outcome")
			}
			if !tc.want {
				// THE RECORD IS UNCHANGED, asserted as well as the refusal: a refusal
				// that nonetheless moved the record would satisfy the boolean above and
				// still have written the identity this guard exists to protect.
				if cur.Acq != tc.from || smsg.Sequence != seededSeq {
					t.Fatalf("a refused rewrite moved the record: acq=%d seq=%d, want acq=%d seq=%d",
						cur.Acq, smsg.Sequence, tc.from, seededSeq)
				}
			}
		})
	}
}
