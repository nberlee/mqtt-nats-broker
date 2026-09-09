package mqtt

import (
	"errors"
	"strings"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

// Shorthands so the matrix below reads as the contract's table does. They are the only
// place the test names the production constants; every row is a literal.
var (
	obsCaptured = func(o mqttMarkerOutcome) mqttMarkerObservation {
		return mqttMarkerObservation{prov: mqttMarkerCaptured, outcome: o, seq: 41}
	}
	obsReloaded = func(o mqttMarkerOutcome) mqttMarkerObservation {
		seq := uint64(0)
		if o == mqttMarkerMatch || o == mqttMarkerUnattributed {
			seq = 57
		}
		return mqttMarkerObservation{prov: mqttMarkerReloaded, outcome: o, seq: seq}
	}
	obsNoneHeld = mqttMarkerObservation{prov: mqttMarkerNoneHeld}
	regOf       = func(k mqttRegisterKind) mqttRegisterObservation {
		switch k {
		case mqttRegisterUnobserved, mqttRegisterAbsent, mqttRegisterIndeterminate, mqttRegisterIneligible:
			return mqttRegisterObservation{kind: k}
		}
		return mqttRegisterObservation{kind: k, seq: 9}
	}
	// A resume reread that found the same exchange at the sequence the walk saw, or moved.
	regRereadSame  = mqttRegisterObservation{kind: mqttRegisterSameExchange, seq: 9, snapshotSeq: 9}
	regRereadMoved = mqttRegisterObservation{kind: mqttRegisterSameExchange, seq: 12, snapshotSeq: 9}
)

// TestMQTTMarkerObservationMatrix is the decision table, one subtest per row, written out
// as LITERALS rather than derived from the code's own table: a test that iterates the
// production rows proves nothing against a wrong row. Every row of the F-072 contract is
// here, including the ones a shortcut would fold — 24a (absence completes a reap under any
// acquisition), 29 (a reload-acquired present marker is never reaped), 20a (a sender that
// wrote nothing and holds nothing owes nothing) — and the type rejections: "record a
// transmission" takes no absence and no indeterminate, because the sender performs no read.
func TestMQTTMarkerObservationMatrix(t *testing.T) {
	// NOT parallel at the top level: the rejection and hook subtests install the
	// process-global disposition hook. The rows run in parallel among themselves.

	derive := func(resume bool) mqttMarkerContext {
		return mqttMarkerContext{op: mqttMarkerOpDeriveSendState, resume: resume}
	}
	replace := func(gate bool) mqttMarkerContext {
		return mqttMarkerContext{op: mqttMarkerOpReplaceOccupant, gatePassed: gate}
	}
	record := mqttMarkerContext{op: mqttMarkerOpRecordIntent}
	evidence := mqttMarkerContext{op: mqttMarkerOpCleanAfterEvidence}
	reap := mqttMarkerContext{op: mqttMarkerOpReap}

	rows := []struct {
		row  string
		ctx  mqttMarkerContext
		reg  mqttRegisterObservation
		mk   mqttMarkerObservation
		want mqttMarkerDisposition
	}{
		// derive send state, store path
		{"1", derive(false), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerMatch), mqttMarkerInstallUnsent},
		{"2", derive(false), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerAbsent), mqttMarkerWithhold},
		{"3", derive(false), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerUnattributed), mqttMarkerWithhold},
		{"3", derive(false), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerIndeterminate), mqttMarkerWithhold},
		// derive send state, resume, marker present against the walk snapshot
		{"4", derive(true), regOf(mqttRegisterSnapshot), obsCaptured(mqttMarkerMatch), mqttMarkerInstallUnsent},
		{"4a", derive(true), regOf(mqttRegisterSnapshot), obsCaptured(mqttMarkerUnattributed), mqttMarkerWithhold},
		{"4a", derive(true), regOf(mqttRegisterSnapshot), obsReloaded(mqttMarkerIndeterminate), mqttMarkerWithhold},
		// derive send state, resume, marker absent: the reread replaces the snapshot
		{"5", derive(true), regRereadSame, obsReloaded(mqttMarkerAbsent), mqttMarkerInstallTransmitted},
		{"6", derive(true), regRereadMoved, obsReloaded(mqttMarkerAbsent), mqttMarkerReclassify},
		{"7", derive(true), regOf(mqttRegisterAbsent), obsReloaded(mqttMarkerAbsent), mqttMarkerReclassify},
		{"8", derive(true), regOf(mqttRegisterDifferentExchange), obsReloaded(mqttMarkerAbsent), mqttMarkerReclassify},
		{"9", derive(true), regOf(mqttRegisterUnreadable), obsReloaded(mqttMarkerAbsent), mqttMarkerWithhold},
		{"9", derive(true), regOf(mqttRegisterIndeterminate), obsReloaded(mqttMarkerAbsent), mqttMarkerWithhold},
		// row 14: an ineligible register derives nothing from any marker, including none
		{"14", derive(true), regOf(mqttRegisterIneligible), obsCaptured(mqttMarkerMatch), mqttMarkerWithhold},
		{"14", derive(true), regOf(mqttRegisterIneligible), obsReloaded(mqttMarkerAbsent), mqttMarkerWithhold},
		{"14", derive(true), regOf(mqttRegisterIneligible), mqttMarkerObservation{}, mqttMarkerWithhold},
		// replace an occupant
		{"15", replace(true), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerMatch), mqttMarkerReplacePair},
		{"16", replace(true), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerAbsent), mqttMarkerReplaceRegisterOnly},
		{"17", replace(true), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerUnattributed), mqttMarkerWithhold},
		{"17", replace(true), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerIndeterminate), mqttMarkerWithhold},
		// row 18 precedes 17: the refused gate refuses whatever the marker says
		{"18", replace(false), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerMatch), mqttMarkerRefuse},
		{"18", replace(false), regOf(mqttRegisterOurs), obsReloaded(mqttMarkerUnattributed), mqttMarkerRefuse},
		{"18", replace(false), regOf(mqttRegisterOurs), mqttMarkerObservation{}, mqttMarkerRefuse},
		// record intent, BEFORE the write: {captured, none held}; there is no "wrote" input
		{"19", record, regOf(mqttRegisterUnobserved), obsCaptured(mqttMarkerMatch), mqttMarkerDeleteMarker},
		{"19a", record, regOf(mqttRegisterUnobserved), obsNoneHeld, mqttMarkerPreserveMarker},
		// clean after protocol evidence: the register is not a condition
		{"21", evidence, regOf(mqttRegisterUnobserved), obsCaptured(mqttMarkerMatch), mqttMarkerDeleteMarker},
		{"22", evidence, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerMatch), mqttMarkerDeleteMarker},
		{"22a", evidence, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerAbsent), mqttMarkerPreserveMarker},
		{"23", evidence, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerUnattributed), mqttMarkerRefuse},
		{"23", evidence, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerIndeterminate), mqttMarkerRefuse},
		// reap, captured before the sibling read
		{"24", reap, regOf(mqttRegisterAbsent), obsCaptured(mqttMarkerMatch), mqttMarkerDeleteMarker},
		{"24", reap, regOf(mqttRegisterAbsent), obsCaptured(mqttMarkerUnattributed), mqttMarkerDeleteMarker},
		{"25", reap, regOf(mqttRegisterDifferentExchange), obsCaptured(mqttMarkerMatch), mqttMarkerDeleteMarker},
		{"26", reap, regOf(mqttRegisterSameExchange), obsCaptured(mqttMarkerMatch), mqttMarkerPreserveMarker},
		{"26a", reap, regOf(mqttRegisterSameExchange), obsCaptured(mqttMarkerUnattributed), mqttMarkerPreserveMarker},
		{"26a", reap, regOf(mqttRegisterDifferentExchange), obsCaptured(mqttMarkerUnattributed), mqttMarkerPreserveMarker},
		{"27", reap, regOf(mqttRegisterUnreadable), obsCaptured(mqttMarkerMatch), mqttMarkerPreserveMarker},
		{"27", reap, regOf(mqttRegisterUnreadable), obsCaptured(mqttMarkerUnattributed), mqttMarkerPreserveMarker},
		{"28", reap, regOf(mqttRegisterIndeterminate), obsCaptured(mqttMarkerMatch), mqttMarkerPreserveMarker},
		{"28", reap, regOf(mqttRegisterIndeterminate), obsCaptured(mqttMarkerUnattributed), mqttMarkerPreserveMarker},
		// row 24a: absence is accepted under ANY acquisition and ANY sibling — nothing to delete
		{"24a", reap, regOf(mqttRegisterAbsent), obsReloaded(mqttMarkerAbsent), mqttMarkerPreserveMarker},
		{"24a", reap, regOf(mqttRegisterSameExchange), obsReloaded(mqttMarkerAbsent), mqttMarkerPreserveMarker},
		{"24a", reap, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerAbsent), mqttMarkerPreserveMarker},
		{"24a", reap, regOf(mqttRegisterIndeterminate), obsReloaded(mqttMarkerAbsent), mqttMarkerPreserveMarker},
		// row 29: reload-acquired and present or indeterminate is never reaped
		{"29", reap, regOf(mqttRegisterAbsent), obsReloaded(mqttMarkerMatch), mqttMarkerRefuse},
		{"29", reap, regOf(mqttRegisterAbsent), obsReloaded(mqttMarkerUnattributed), mqttMarkerRefuse},
		{"29", reap, regOf(mqttRegisterAbsent), obsReloaded(mqttMarkerIndeterminate), mqttMarkerRefuse},
		{"29", reap, regOf(mqttRegisterDifferentExchange), obsReloaded(mqttMarkerMatch), mqttMarkerRefuse},
		{"29", reap, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerIndeterminate), mqttMarkerRefuse},
	}
	// The rows run inside one group, so the group returns only when every row has fired;
	// the subtests after it install the process-wide disposition hook and must not see a
	// row's verdict.
	t.Run("rows", func(t *testing.T) {
		for _, tc := range rows {
			name := "row " + tc.row + " " + tc.mk.prov.String() + " " + tc.mk.outcome.String()
			t.Run(name, func(t *testing.T) {
				v, err := mqttDecideMarker(tc.ctx, tc.reg, tc.mk)
				if err != nil {
					t.Fatalf("row %s: unexpected rejection: %v", tc.row, err)
				}
				if v.disposition != tc.want || v.row != tc.row {
					t.Fatalf("row %s: want %s, got %s from row %s", tc.row, tc.want, v.disposition, v.row)
				}
			})
		}
	})

	// The dispositions are DISTINGUISHABLE: nine values, nine names, none the zero value.
	// A boundary that collapsed to "may act" (M52) fails here before it fails any row.
	t.Run("dispositions are distinct", func(t *testing.T) {
		all := []mqttMarkerDisposition{
			mqttMarkerInstallUnsent, mqttMarkerInstallTransmitted, mqttMarkerReclassify,
			mqttMarkerWithhold, mqttMarkerReplacePair, mqttMarkerReplaceRegisterOnly,
			mqttMarkerDeleteMarker, mqttMarkerPreserveMarker, mqttMarkerRefuse,
		}
		seen := map[mqttMarkerDisposition]string{}
		names := map[string]struct{}{}
		for _, d := range all {
			if d == mqttMarkerDispositionUnset {
				t.Fatalf("a disposition is the zero value")
			}
			if prev, dup := seen[d]; dup {
				t.Fatalf("dispositions %s and %s share a value", prev, d)
			}
			seen[d] = d.String()
			if _, dup := names[d.String()]; dup || d.String() == "unset" {
				t.Fatalf("disposition name %q is not unique", d.String())
			}
			names[d.String()] = struct{}{}
		}
	})

	// Type rejections: observations that are not inputs of the operation. Each is an
	// error, never a Refuse, so a caller cannot mistake its own programming error for a
	// stream state it has a lifecycle for.
	rejections := []struct {
		name string
		ctx  mqttMarkerContext
		reg  mqttRegisterObservation
		mk   mqttMarkerObservation
	}{
		{"record intent rejects an absent marker", record, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerAbsent)},
		{"record intent rejects an indeterminate marker", record, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerIndeterminate)},
		{"record intent rejects a reload-acquired marker", record, regOf(mqttRegisterUnobserved), obsReloaded(mqttMarkerMatch)},
		{"record intent takes no register", record, regOf(mqttRegisterOurs), obsCaptured(mqttMarkerMatch)},
		{"clean after evidence has no none-held row", evidence, regOf(mqttRegisterUnobserved), obsNoneHeld},
		{"clean after evidence takes no register", evidence, regOf(mqttRegisterSameExchange), obsCaptured(mqttMarkerMatch)},
		{"reap never completes on none held alone", reap, regOf(mqttRegisterAbsent), obsNoneHeld},
		{"reap revalidates by a read, not a snapshot", reap, regOf(mqttRegisterSnapshot), obsCaptured(mqttMarkerMatch)},
		{"an absent marker is not read against the walk snapshot", derive(true), regOf(mqttRegisterSnapshot), obsReloaded(mqttMarkerAbsent)},
		{"a reread is entered only on an absent marker", derive(true), regRereadSame, obsCaptured(mqttMarkerMatch)},
		{"the store path derives from the acknowledged register", derive(false), regOf(mqttRegisterSnapshot), obsReloaded(mqttMarkerMatch)},
		{"derive send state takes no entry property", derive(false), regOf(mqttRegisterOurs), obsNoneHeld},
		{"replacement takes no entry property", replace(true), regOf(mqttRegisterOurs), obsNoneHeld},
		{"none held carries no outcome", record, regOf(mqttRegisterUnobserved), mqttMarkerObservation{prov: mqttMarkerNoneHeld, outcome: mqttMarkerAbsent}},
		{"a capture was present", reap, regOf(mqttRegisterAbsent), mqttMarkerObservation{prov: mqttMarkerCaptured, outcome: mqttMarkerAbsent}},
		{"a present marker has a sequence", reap, regOf(mqttRegisterAbsent), mqttMarkerObservation{prov: mqttMarkerCaptured, outcome: mqttMarkerMatch}},
		{"no provenance", reap, regOf(mqttRegisterAbsent), mqttMarkerObservation{outcome: mqttMarkerMatch, seq: 3}},
		{"unknown operation", mqttMarkerContext{}, regOf(mqttRegisterAbsent), obsCaptured(mqttMarkerMatch)},
	}
	for _, tc := range rejections {
		t.Run(tc.name, func(t *testing.T) {
			fired := false
			restore := setMQTTTestMarkerDispositionHook(func(string, mqttMarkerVerdict) { fired = true })
			defer restore()
			v, err := mqttDecideMarker(tc.ctx, tc.reg, tc.mk)
			if err == nil {
				t.Fatalf("must be rejected as a type error, got %s from row %s", v.disposition, v.row)
			}
			if !errors.Is(err, errMarkerObservationIll) {
				t.Fatalf("rejection has the wrong class: %v", err)
			}
			if fired {
				t.Fatal("a rejected observation reached no disposition and must not report one")
			}
		})
	}

	// The hook reports the verdict to the caller tag it was given, which is what the
	// caller-attributed behavioural oracles rest on.
	t.Run("the disposition hook carries the caller", func(t *testing.T) {
		var gotCaller string
		var got mqttMarkerVerdict
		restore := setMQTTTestMarkerDispositionHook(func(caller string, v mqttMarkerVerdict) {
			gotCaller, got = caller, v
		})
		defer restore()
		ctx := record
		ctx.caller = mqttMarkerCallerSender
		if _, err := mqttDecideMarker(ctx, regOf(mqttRegisterUnobserved), obsNoneHeld); err != nil {
			t.Fatal(err)
		}
		if gotCaller != mqttMarkerCallerSender || got.disposition != mqttMarkerPreserveMarker || got.row != "19a" {
			t.Fatalf("hook saw caller %q verdict %+v", gotCaller, got)
		}
	})
}

// TestMQTTMarkerDecodeIsAClosedSchema pins the marker's decoder to the register decoder's
// property: exact-case keys, no duplicates, no unknown members, no trailing content, a
// well-formed nonce — and that the two classes never decode as each other.
func TestMQTTMarkerDecodeIsAClosedSchema(t *testing.T) {
	t.Parallel()
	nonce := strings.Repeat("x", mqttInflightNonceLen)
	legal, err := mqttBuildInflightMarker(nonce)
	if err != nil {
		t.Fatalf("a well-formed nonce must build: %v", err)
	}
	if string(legal) != `{"nonce":"`+nonce+`"}` {
		t.Fatalf("the marker body is %s", legal)
	}
	m, err := mqttDecodeInflightMarker(legal)
	if err != nil || m == nil || m.Nonce != nonce {
		t.Fatalf("the legal body must decode, got %+v %v", m, err)
	}
	if _, err := mqttBuildInflightMarker("short"); err == nil {
		t.Fatal("a malformed nonce must not build a marker")
	}

	for _, tc := range []struct{ name, body string }{
		{"an unknown member", `{"nonce":"` + nonce + `","pi":7}`},
		{"a case variant of the real member", `{"nonce":"` + nonce + `","NONCE":"` + nonce + `"}`},
		{"a case variant alone", `{"NONCE":"` + nonce + `"}`},
		{"a duplicate member", `{"nonce":"` + nonce + `","nonce":"` + nonce + `"}`},
		{"trailing content", `{"nonce":"` + nonce + `"}}`},
		{"trailing content after a scalar", `{"nonce":"` + nonce + `"} x`},
		{"no nonce", `{}`},
		{"an empty nonce", `{"nonce":""}`},
		{"a nonce of the wrong length", `{"nonce":"` + nonce[:10] + `"}`},
		{"a nonce out of alphabet", `{"nonce":"` + strings.Repeat("!", mqttInflightNonceLen) + `"}`},
		{"a nonce of the wrong type", `{"nonce":7}`},
		{"not an object", `["` + nonce + `"]`},
		// The sibling's body: a register is not a marker.
		{"a register body", `{"pi":7,"dur":"$MQTT_msgs_x","seq":41,"qos":2,"state":"publish","nonce":"` + nonce + `"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, derr := mqttDecodeInflightMarker([]byte(tc.body)); derr == nil {
				t.Fatalf("must be unattributed, decoded to %+v", got)
			}
		})
	}

	// And a marker is not a register: the register decoder refuses the marker body.
	t.Run("a marker body is not a register", func(t *testing.T) {
		t.Parallel()
		if r, derr := mqttDecodeInflightRecord(legal); derr == nil {
			t.Fatalf("the register decoder must refuse a marker body, decoded to %+v", r)
		}
	})
}

// TestMQTTInflightWalkFilterExcludesMarkers pins the two subject classes apart at every
// seam: the register walk filter does not match a marker, the marker walk filter matches
// only markers of that incarnation, and each parser refuses the other's subject.
func TestMQTTInflightWalkFilterExcludesMarkers(t *testing.T) {
	t.Parallel()
	idHash := strings.Repeat("h", sysHashLen)
	gen := strings.Repeat("g", mqttSessGenLen)
	other := strings.Repeat("o", mqttSessGenLen)
	reg := mqttInflightSubject(idHash, gen, 7)
	mk := mqttInflightMarkerSubject(idHash, gen, 7)
	if mk != reg+".unsent" {
		t.Fatalf("marker subject is %q", mk)
	}
	regFilter := mqttInflightWalkFilter(idHash, gen)
	mkFilter := mqttInflightMarkerWalkFilter(idHash, gen)

	for _, tc := range []struct {
		name    string
		filter  string
		subject string
		want    bool
	}{
		{"register filter matches a register", regFilter, reg, true},
		{"register filter does not match a marker", regFilter, mk, false},
		{"marker filter matches a marker", mkFilter, mk, true},
		{"marker filter does not match a register", mkFilter, reg, false},
		{"marker filter is bound to the incarnation", mkFilter, mqttInflightMarkerSubject(idHash, other, 7), false},
		{"marker filter is bound to the client", mkFilter, mqttInflightMarkerSubject(strings.Repeat("o", sysHashLen), gen, 7), false},
		{"the sweep's prefix sees both", mqttInflightStreamSubjectPrefix + ">", mk, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := natsserver.SubjectsCollide(tc.filter, tc.subject); got != tc.want {
				t.Fatalf("%q against %q: %v", tc.filter, tc.subject, got)
			}
		})
	}

	t.Run("parsers refuse each other's class", func(t *testing.T) {
		t.Parallel()
		if n, err := mqttParseInflightSubject(mk); err == nil {
			t.Fatalf("the register parser must refuse a marker subject, got %+v", n)
		}
		if n, err := mqttParseInflightMarkerSubject(reg); err == nil {
			t.Fatalf("the marker parser must refuse a register subject, got %+v", n)
		}
		n, err := mqttParseInflightMarkerSubject(mk)
		if err != nil {
			t.Fatalf("the marker parser must accept its own subject: %v", err)
		}
		if n.idHash != idHash || n.gen != gen || n.pi != 7 || n.subject != reg {
			t.Fatalf("the marker parser names the wrong register: %+v", n)
		}
		if mqttInflightMarkerSubjectFor(n) != mk {
			t.Fatalf("the register name does not derive its marker subject")
		}
		for _, bad := range []string{
			reg + ".UNSENT", reg + ".unsent.x", reg + "..unsent",
			mqttInflightStreamSubjectPrefix + idHash + "." + gen + ".01.unsent",
			mqttInflightStreamSubjectPrefix + idHash + "." + gen + ".0.unsent",
			"unsent",
		} {
			if n, err := mqttParseInflightMarkerSubject(bad); err == nil {
				t.Fatalf("%q must not parse as a marker, got %+v", bad, n)
			}
		}
	})
}
