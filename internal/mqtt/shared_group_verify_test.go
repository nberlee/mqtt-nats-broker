package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nuid"
)

// Every ConsumerConfig field carries exactly one disposition, derived by REFLECTION rather
// than listed by hand, so a dependency bump that adds a field breaks the bijection by name
// instead of silently widening what a present group durable may carry.
//
// This proves every field is CLASSIFIED. It does not prove each is ENFORCED — that is
// TestSharedGroupConfigPerFieldEnforcement below, and the two are separate on purpose: a
// field can be correctly dispositioned and never compared.
func TestSharedGroupConfigFieldCoverage(t *testing.T) {
	t.Parallel()

	typ := reflect.TypeOf(ConsumerConfig{})
	seen := make(map[string]struct{}, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		seen[name] = struct{}{}
		rule, ok := sharedGroupConfigRules[name]
		if !ok {
			t.Errorf("ConsumerConfig.%s has no disposition in sharedGroupConfigRules. "+
				"Add one: a new field is refused by the whole-struct comparison, and this table "+
				"is where the reason it is refused (or accepted) gets written down", name)
			continue
		}
		if rule.reason == "" {
			t.Errorf("ConsumerConfig.%s is dispositioned with no reason", name)
		}
	}
	for name := range sharedGroupConfigRules {
		if _, ok := seen[name]; !ok {
			t.Errorf("sharedGroupConfigRules names %q, which is not a ConsumerConfig field", name)
		}
	}
}

// What the SERVER sets that the request did not, measured against a real create rather
// than asserted from memory.
//
// This is the oracle for the sharedGroupServerNormalized rows. It fails in both
// directions: a row whose expected value is not what nats-server actually produces, and a
// field the server normalizes that carries no row. Without it, the expected values in
// sharedGroupExpectedConfig are a remembered list, which is exactly the failure mode the
// whole-struct comparison exists to avoid.
func TestSharedGroupServerNormalization(t *testing.T) {
	t.Parallel()

	e := testNewInflightEnv(t, "client-shared-normalization")
	const group, subject = "g", "foo"

	req := buildSharedGroupConsumerConfig(group, subject, 30*time.Second, 1024, false)
	if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
		Stream: mqttStreamName,
		Config: req,
	}); err != nil {
		t.Fatalf("createDurableConsumer: %v", err)
	}

	_, _, durable := mqttSharedNames(group, subject)
	ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, durable)
	if err != nil || ci == nil || ci.Config == nil {
		t.Fatalf("lookupConsumer: info=%v err=%v", ci, err)
	}

	// Which fields the server moved, by reflection over the live config against the request.
	live := reflect.ValueOf(*ci.Config)
	sent := reflect.ValueOf(req)
	typ := live.Type()
	moved := map[string]struct{}{}
	for i := 0; i < typ.NumField(); i++ {
		if !reflect.DeepEqual(live.Field(i).Interface(), sent.Field(i).Interface()) {
			moved[typ.Field(i).Name] = struct{}{}
		}
	}

	// Both server-side kinds are legitimate destinations for a field the server moved.
	// They are not interchangeable — Normalized carries ONE expected value and is compared
	// against it, Owned has no predictable value and is constrained a different way — but
	// either is an accounted-for answer to "the server moved this".
	declared := map[string]struct{}{}
	for name, rule := range sharedGroupConfigRules {
		if rule.kind == sharedGroupServerNormalized || rule.kind == sharedGroupServerOwned {
			declared[name] = struct{}{}
		}
	}

	for name := range moved {
		if _, ok := declared[name]; !ok {
			t.Errorf("nats-server moved ConsumerConfig.%s (request %v -> live %v) but it is dispositioned "+
				"neither sharedGroupServerNormalized nor sharedGroupServerOwned. Give it whichever fits — "+
				"Normalized if it has one predictable value, Owned if it does not and can be constrained "+
				"another way — or the comparison will refuse every healthy group durable",
				name,
				sent.FieldByName(name).Interface(), live.FieldByName(name).Interface())
		}
	}
	for name := range declared {
		if _, ok := moved[name]; !ok {
			t.Errorf("ConsumerConfig.%s is dispositioned as server-set, but the server did not move it. "+
				"A field the server leaves alone must be pinned, or its expected value is unconstrained "+
				"prose", name)
		}
	}

	// The bridge writes no consumer metadata, and a freshly created durable is the cleanest
	// place to assert it: whatever is there came from the server.
	if err := sharedGroupMetadataIsServerOnly(ci.Config.Metadata); err != nil {
		t.Errorf("a freshly created group durable already carries non-server metadata: %v", err)
	}

	// And the expected VALUES are what the server actually produced — the half that stops
	// a normalized row becoming a free pass.
	want := sharedGroupExpectedConfig(group, subject, ci.Config)
	if !reflect.DeepEqual(*ci.Config, want) {
		t.Errorf("a freshly created group durable does not match its own expected configuration.\n got: %+v\nwant: %+v",
			*ci.Config, want)
	}
}

// The batched consumer lookup must be able to say three different things, and an earlier
// encoding could only say two.
//
// mqttClassifyConsumerInfoResponse is tested DIRECTLY rather than through any caller
// because the defect it fixes lives entirely in decoding one response: a success carrying
// a nil ConsumerInfo used to be encoded exactly like an authoritative not-found, so
// "the consumer is not there" and "the reply established nothing" were indistinguishable.
// The in-flight source cache turns the first into mqttInflightSourceLost, a terminal
// verdict, so the collapse concluded unrecoverability from a reply that proved nothing.
//
// A caller-level test cannot pin this: an integration fixture that stubs the batch never
// exercises the decode, and both classifications refuse, so a refusal-only assertion
// passes either way. This is the test the encoding mutation cannot get past.
func TestMQTTClassifyConsumerInfoResponse(t *testing.T) {
	t.Parallel()

	usable := &ConsumerInfo{Name: "dur", Config: &ConsumerConfig{Durable: "dur"}}

	for _, tc := range []struct {
		name        string
		resp        *JSApiConsumerInfoResponse
		wantInfo    *ConsumerInfo
		wantSettled bool
		why         string
	}{
		{
			name:        "authoritative not found",
			resp:        &JSApiConsumerInfoResponse{ApiResponse: ApiResponse{Error: ApiErrors[JSConsumerNotFoundErr]}},
			wantInfo:    nil,
			wantSettled: true,
			why:         "not-found is the one error that PROVES absence, which is what makes it the one verdict the CONNECT pre-pass may repair (and the post-pass still treats as terminal)",
		},
		{
			name:        "success carrying a nil ConsumerInfo",
			resp:        &JSApiConsumerInfoResponse{},
			wantInfo:    nil,
			wantSettled: false,
			why:         "a success that says nothing is NOT an absence; encoding it as one is the defect this function exists for",
		},
		{
			name:        "consumer read but carrying a nil Config",
			resp:        &JSApiConsumerInfoResponse{ConsumerInfo: &ConsumerInfo{Name: "dur"}},
			wantInfo:    &ConsumerInfo{Name: "dur"},
			wantSettled: true,
			why:         "the consumer WAS read, so the lookup is settled; whether a nil Config is usable is the caller's question, not the decode's",
		},
		{
			name:        "an error that is not not-found",
			resp:        &JSApiConsumerInfoResponse{ApiResponse: ApiResponse{Error: ApiErrors[JSStreamNotFoundErr]}},
			wantInfo:    nil,
			wantSettled: false,
			why:         "any other error is a failed read, which establishes nothing",
		},
		{
			name:        "a usable consumer",
			resp:        &JSApiConsumerInfoResponse{ConsumerInfo: usable},
			wantInfo:    usable,
			wantSettled: true,
			why:         "the ordinary case",
		},
		{
			name:        "no response at all",
			resp:        nil,
			wantInfo:    nil,
			wantSettled: false,
			why:         "nothing came back, so nothing is established",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			info, settled := mqttClassifyConsumerInfoResponse(tc.resp)
			if settled != tc.wantSettled {
				t.Fatalf("settled=%v, want %v: %s", settled, tc.wantSettled, tc.why)
			}
			if (info == nil) != (tc.wantInfo == nil) {
				t.Fatalf("info=%v, want nil==%v: %s", info, tc.wantInfo == nil, tc.why)
			}
			if info != nil && tc.wantInfo != nil && info.Name != tc.wantInfo.Name {
				t.Fatalf("info.Name=%q, want %q", info.Name, tc.wantInfo.Name)
			}
		})
	}
}

// A shared group consumer config persisted in Cons is a shape this broker never writes:
// buildPersistedSessionLocked skips every entry whose DeliverGroup is non-empty, because
// SharedSubs is the sole authority. Under the compatibility boundary such a record is
// REFUSED at every ingress rather than accommodated.
//
// One case per WIRING POINT, and each needs a POSITIVE oracle. "The call returned an
// error" is not one: three of these ingresses continue past a bad record by design, so a
// test that only checked a return value would pass with the refusal removed. What each
// case asserts is the destructive thing that did NOT happen.
func testStoreSharedConsRecord(t *testing.T, e *testInflightEnv, clientID string) {
	t.Helper()
	_, _, durable := mqttSharedNames("grp", "topic")
	ps := &mqttPersistedSession{
		Origin: e.asm.jsa.id,
		ID:     clientID,
		Gen:    mqttNewSessGen(),
		Cons: map[string]*ConsumerConfig{
			"$share/grp/topic": {Durable: durable, DeliverGroup: "$mqttshare.x", DeliverSubject: "$MQTT.qsub.x"},
		},
		ExpiryInterval: 1,
		DisconnectedAt: 1,
	}
	body, err := json.Marshal(ps)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := e.asm.jsa.storeSessionMsg(e.asm.domainTk, getHash(clientID), 0, body); err != nil {
		t.Fatalf("storing the unsupported record: %v", err)
	}
}

func TestSharedConsInRecordIsRefusedAtEveryIngress(t *testing.T) {
	t.Parallel()

	t.Run("the record predicate itself", func(t *testing.T) {
		t.Parallel()
		_, _, groupDur := mqttSharedNames("g", "t")

		for _, tc := range []struct {
			name string
			sid  string
			cc   *ConsumerConfig
			want bool
			why  string
		}{
			{
				name: "a plain consumer",
				sid:  "plain/topic",
				cc:   &ConsumerConfig{Durable: "idhash_nuid"},
				want: false,
				why:  "the ordinary shape, which must keep working",
			},
			{
				name: "a non-empty DeliverGroup",
				sid:  "plain/topic",
				cc:   &ConsumerConfig{Durable: "d", DeliverGroup: "q"},
				want: true,
				why:  "the obvious mark",
			},
			{
				name: "a shared SID with an EMPTY DeliverGroup",
				sid:  "$share/g/t",
				cc:   &ConsumerConfig{Durable: "d"},
				want: true,
				why: "the hole a DeliverGroup-only guard leaves: clear() decides what is " +
					"session-private by DeliverGroup alone, so this entry would go into the " +
					"DIRECT deletion list and teardown would delete a consumer it does not own",
			},
			{
				name: "the level-up companion SID with an empty DeliverGroup",
				sid:  "$share/g/t" + mqttMultiLevelSidSuffix,
				cc:   &ConsumerConfig{Durable: "d"},
				want: true,
				why:  "the companion is a shared consumer in its own right",
			},
			{
				name: "a group DURABLE with an empty DeliverGroup and a plain SID",
				sid:  "plain/topic",
				cc:   &ConsumerConfig{Durable: groupDur},
				want: true,
				why: "mqttSharedNames is the single source of that prefix, so no other " +
					"consumer class can carry it — the durable alone identifies the shape",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				ps := &mqttPersistedSession{Cons: map[string]*ConsumerConfig{tc.sid: tc.cc}}
				_, found := mqttSharedConsInRecord(ps)
				if found != tc.want {
					t.Fatalf("found=%v, want %v: %s", found, tc.want, tc.why)
				}
			})
		}
	})

	// The destructive consequence of the empty-DeliverGroup shape, end to end: without the
	// refusal, record-driven expiry hands this entry to clear(), which reads it as
	// session-private and deletes the group durable outright.
	t.Run("record-driven expiry refuses an empty-DeliverGroup shared entry", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "ingress-emptydg-owner")
		const cid = "ingress-emptydg"
		_, _, groupDur := mqttSharedNames("grp", "topic")
		ps := &mqttPersistedSession{
			Origin:         e.asm.jsa.id,
			ID:             cid,
			Gen:            mqttNewSessGen(),
			SharedSubs:     map[string]byte{"$share/grp/topic": 1},
			Cons:           map[string]*ConsumerConfig{"$share/grp/topic": {Durable: groupDur}},
			ExpiryInterval: 1,
			DisconnectedAt: 1,
		}
		body, err := json.Marshal(ps)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, err := e.asm.jsa.storeSessionMsg(e.asm.domainTk, getHash(cid), 0, body); err != nil {
			t.Fatalf("store: %v", err)
		}
		before := testLoadRawSessionRecord(t, e, cid)

		e.asm.expireSessionFromRecord(e.s, getHash(cid))

		after := testLoadRawSessionRecord(t, e, cid)
		if after == nil {
			t.Fatal("expiry DELETED a record carrying an unsupported shared shape, which means " +
				"clear() also ran over its Cons and deleted the group durable")
		}
		if before.Sequence != after.Sequence || !bytes.Equal(before.Data, after.Data) {
			t.Fatalf("expiry rewrote the record (seq %d -> %d)", before.Sequence, after.Sequence)
		}
	})

	// INGRESS 1 - the CONNECT restore prologue. Its oracle is direct: the restore fails, so
	// no session is ever built from the record. This also covers the overdue/retired branch,
	// which builds its transient session from the same ps behind this check.
	t.Run("CONNECT restore", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "ingress-connect-owner")
		const cid = "ingress-connect"
		testStoreSharedConsRecord(t, e, cid)
		sess, _, err := e.asm.createOrRestoreSession(cid, e.s.getOpts())
		if err == nil {
			t.Fatalf("the restore accepted an unsupported record shape; session=%v", sess)
		}
		if !strings.Contains(err.Error(), "shared group consumer configuration") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	// INGRESS 2 - record-driven expiry. Its oracle is that the record is left UNCHANGED
	// and UNCLAIMED: this path claims, rewrites and deletes what it acts on, so "it did not
	// crash" proves nothing and only the record's own survival does.
	t.Run("record-driven expiry", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "ingress-expiry-owner")
		const cid = "ingress-expiry"
		testStoreSharedConsRecord(t, e, cid)
		before := testLoadRawSessionRecord(t, e, cid)

		e.asm.expireSessionFromRecord(e.s, getHash(cid))

		after := testLoadRawSessionRecord(t, e, cid)
		if after == nil {
			t.Fatal("record-driven expiry DELETED a record whose shape this broker cannot interpret")
		}
		if !bytes.Equal(before.Data, after.Data) || before.Sequence != after.Sequence {
			t.Fatalf("record-driven expiry rewrote the unsupported record (seq %d -> %d): refusing "+
				"means leaving it exactly as found, since a rewrite normalizes a format nothing here "+
				"understands", before.Sequence, after.Sequence)
		}
		var ps mqttPersistedSession
		if err := json.Unmarshal(after.Data, &ps); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if ps.Teardown != nil {
			t.Fatal("record-driven expiry CLAIMED a teardown on an unsupported record")
		}
	})

	// INGRESS 3 - the session-expiry sweep. Its oracle is that no DisconnectedAt stamp is
	// written: that stamp is a full record write, and writing one normalizes the shape.
	t.Run("session-expiry sweep", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "ingress-sweep-owner")
		const cid = "ingress-sweep"
		_, _, durable := mqttSharedNames("grp", "topic")
		ps := &mqttPersistedSession{
			Origin: e.asm.jsa.id,
			ID:     cid,
			Gen:    mqttNewSessGen(),
			Cons: map[string]*ConsumerConfig{
				"$share/grp/topic": {Durable: durable, DeliverGroup: "$mqttshare.x"},
			},
			ExpiryInterval: 60,
			// DisconnectedAt deliberately ZERO: that is the state whose stamp the sweep
			// would write, so it is the only shape in which the refusal is observable.
		}
		body, err := json.Marshal(ps)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, err := e.asm.jsa.storeSessionMsg(e.asm.domainTk, getHash(cid), 0, body); err != nil {
			t.Fatalf("store: %v", err)
		}
		before := testLoadRawSessionRecord(t, e, cid)

		e.asm.sweepSessionExpiries(e.s)

		after := testLoadRawSessionRecord(t, e, cid)
		if after == nil {
			t.Fatal("the expiry sweep deleted an unsupported record")
		}
		var got mqttPersistedSession
		if err := json.Unmarshal(after.Data, &got); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got.DisconnectedAt != 0 {
			t.Fatalf("the expiry sweep STAMPED DisconnectedAt=%d on an unsupported record, which is a "+
				"full record write and normalizes the shape", got.DisconnectedAt)
		}
		if before.Sequence != after.Sequence {
			t.Fatalf("the expiry sweep rewrote the record (seq %d -> %d)", before.Sequence, after.Sequence)
		}
	})

	// INGRESS 4 - the orphan-consumer sweep. Its oracle is an UNRELATED orphan that the
	// scan would have deleted had it continued. Without that, "the sweep did not delete the
	// shared durable" is satisfied by a sweep that simply skipped the record, which is the
	// behaviour the refusal must NOT have: this walk deletes by absence of a reference, so a
	// record it cannot account for has to abort the whole pass.
	t.Run("orphan-consumer sweep aborts", func(t *testing.T) {
		t.Parallel()
		e := testNewInflightEnv(t, "ingress-orphan-owner")
		const cid = "ingress-orphan"

		orphan := mqttSharedConsumerDurablePrefix + "unreferencedorphan"
		if _, err := e.asm.jsa.createDurableConsumer(&CreateConsumerRequest{
			Stream: mqttStreamName,
			Config: ConsumerConfig{
				Durable: orphan, DeliverSubject: mqttSubPrefix + "orphan",
				DeliverGroup: "$mqttshare.orphan", AckPolicy: AckExplicit,
			},
		}); err != nil {
			t.Fatalf("creating the unrelated orphan: %v", err)
		}
		testStoreSharedConsRecord(t, e, cid)

		// Age every candidate past the sweep's grace window, or nothing is eligible and
		// the assertion below would hold for the wrong reason.
		e.asm.sweepOrphanedConsumersBefore(e.s, time.Now().Add(2*mqttOrphanSweepGrace))

		if ci, err := e.asm.jsa.lookupConsumer(mqttStreamName, orphan); err != nil || ci == nil {
			t.Fatalf("the unrelated orphan was DELETED (%v), so the sweep continued past a record "+
				"it cannot interpret instead of aborting: it deletes by absence of a reference, and "+
				"a record it cannot read may be the very thing referencing a candidate", err)
		}
	})

	// INGRESS 5 - the common validator, driven through the reader that makes it DESTRUCTIVE.
	//
	// discardAbsentWinnerClaim's call to mqttValidateSessionRecord is exactly what
	// authorizes advanceQ2AffClaim(Discarded=true), so the oracle is the claim surviving:
	// still present, still claimed, not discarded. An earlier version of this case called
	// the validator directly on an in-memory struct and asserted an error — which a
	// holistic review correctly rejected, because a regression that removed the validator
	// call, or moved it AFTER the transition, would leave that green while an unsupported
	// record discarded a live assignment.
	//
	// The control arm matters as much: the same fixture with a plain Cons entry MUST reach
	// the discard, or "deferred" proves nothing about the shared entry.
	t.Run("the common validator, via discardAbsentWinnerClaim", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name        string
			cons        map[string]*ConsumerConfig
			wantOutcome mqttQoS2SweepOutcome
			wantDiscard bool
		}{
			{
				name:        "a shared group consumer in Cons defers the discard",
				cons:        map[string]*ConsumerConfig{"$share/g/t": {Durable: "d", DeliverGroup: "q"}},
				wantOutcome: mqttQoS2SweepDeferred,
				wantDiscard: false,
			},
			{
				name:        "control: an interpretable record reaches the discard",
				cons:        map[string]*ConsumerConfig{"plain": {Durable: "d"}},
				wantOutcome: mqttQoS2SweepClaimed,
				wantDiscard: true,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				e := testNewInflightEnv(t, "ingress-discard-"+tc.name[:6])
				// The record's ID must HASH BACK to the claim's winner, or
				// mqttValidateSessionRecord refuses on the identity check and both arms
				// defer for a reason that has nothing to do with the shared entry. The
				// control arm below is what caught exactly that.
				winnerID := "discard-winner-" + tc.name
				idHash := getHash(winnerID)
				gen := nuid.Next()
				const groupDur, sseq = "$MQTT_SHARED_ingressabc", uint64(7)

				subject := mqttQ2AffClaimSubject(groupDur, sseq)
				claim := testValidQ2AffClaim(idHash, gen, groupDur, sseq)
				if _, err := e.asm.storeQ2AffClaimCAS(subject, 0, claim); err != nil {
					t.Fatalf("store claim: %v", err)
				}

				// The winner's record: interpretable except (in the first arm) for the one
				// unsupported entry, and at a DIFFERENT affine generation, which is the
				// state that would otherwise authorize the discard.
				e.asm.mu.Lock()
				e.asm.testSessionLoad = func(string) (*mqttPersistedSession, uint64, error) {
					return &mqttPersistedSession{
						ID:       winnerID,
						Gen:      mqttNewSessGen(),
						Q2AffGen: nuid.Next(),
						Cons:     tc.cons,
					}, 1, nil
				}
				e.asm.mu.Unlock()
				t.Cleanup(func() {
					e.asm.mu.Lock()
					e.asm.testSessionLoad = nil
					e.asm.mu.Unlock()
				})

				got := e.asm.discardAbsentWinnerClaim(e.s, &mqttQ2AffInvEntry{
					subject: subject, claim: claim, stored: time.Now(),
				})
				if got != tc.wantOutcome {
					t.Fatalf("outcome = %v, want %v", got, tc.wantOutcome)
				}

				cur, _, err := e.asm.loadQ2AffClaim(subject)
				if err != nil {
					t.Fatalf("load claim: %v", err)
				}
				if tc.wantDiscard {
					// Reaching the transition shows up EITHER as a discarded claim or as no
					// claim at all: advanceQ2AffClaim is followed by reapQ2AffClaim, which
					// removes the record outright when the source can no longer be
					// re-offered — which is the case for a synthetic source. What must not
					// happen is the claim surviving as a live, undiscarded one.
					if cur != nil && !cur.Discarded {
						t.Fatalf("the control arm did not reach the discard (%+v): if an interpretable "+
							"record cannot get there, the deferral in the other arm proves nothing", cur)
					}
					return
				}
				if cur == nil {
					t.Fatal("the claim is gone: an unsupported record authorized a reap")
				}
				if cur.Discarded {
					t.Fatal("an unsupported record authorized Discarded=true on a live assignment")
				}
				if cur.Phase != mqttQ2AffClaimed {
					t.Fatalf("the claim moved out of claimed: %+v", cur)
				}
			})
		}
	})
}

// testLoadRawSessionRecord reads a client's session record as stored, so a test can prove a
// refusing path left it byte-for-byte alone.
func testLoadRawSessionRecord(t *testing.T, e *testInflightEnv, clientID string) *StoredMsg {
	t.Helper()
	smsg, err := e.asm.jsa.loadSessionMsg(e.asm.domainTk, getHash(clientID))
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("loading the session record: %v", err)
		}
		return nil
	}
	return smsg
}

// Every pinned and server-normalized field is actually ENFORCED, not merely classified.
//
// TestSharedGroupConfigFieldCoverage proves each field has a disposition; this proves the
// comparison acts on it. The two are genuinely different, and a holistic review caught the
// gap: an earlier comment here promised this test by name while it did not exist, so a
// field could stay dispositioned while an implementation change quietly stopped comparing
// it, and the bijection would still pass.
//
// Driven against a SYNTHETIC ConsumerInfo rather than a live consumer, because a live one
// cannot isolate every field: JetStream rejects AckNone alongside a positive MaxAckPending,
// and Name cannot be varied independently of Durable. The baseline is a real canonical
// config, cloned once per field and perturbed in exactly one place, with the perturbation
// asserted to have taken effect before the refusal is required.
func TestSharedGroupConfigPerFieldEnforcement(t *testing.T) {
	t.Parallel()

	const group, subject = "g", "foo"
	base := buildSharedGroupConsumerConfig(group, subject, 30*time.Second, 1024, false)
	base.Name = base.Durable
	base.MaxDeliver = -1
	if err := sharedGroupConfigUsable(&base, group, subject, 0); err != nil {
		t.Fatalf("the canonical baseline must itself be accepted, got: %v", err)
	}

	typ := reflect.TypeOf(base)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		rule, ok := sharedGroupConfigRules[f.Name]
		if !ok {
			continue // the bijection test owns this failure
		}
		if rule.kind != sharedGroupPinned && rule.kind != sharedGroupServerNormalized {
			continue // variable and server-owned fields are bounded, not pinned
		}
		t.Run(f.Name, func(t *testing.T) {
			t.Parallel()
			mutated := base
			v := reflect.ValueOf(&mutated).Elem().Field(i)
			if !perturbConsumerField(v) {
				// FATAL, not a skip. A skipped subtest is a GREEN run, so an unknown kind
				// would silently leave a pinned field unproven — which is the exact shape
				// of hole this matrix exists to close.
				t.Fatalf("no perturbation defined for %s of kind %s: add one, or this field is "+
					"dispositioned but never proven enforced", f.Name, f.Type)
			}
			if reflect.DeepEqual(v.Interface(), reflect.ValueOf(base).Field(i).Interface()) {
				t.Fatalf("the perturbation of %s did not change it, so this case proves nothing", f.Name)
			}
			if err := sharedGroupConfigUsable(&mutated, group, subject, 0); err == nil {
				t.Fatalf("ConsumerConfig.%s is dispositioned %v but a deviation in it was ACCEPTED. "+
					"A classified field that is never compared is exactly the hole the whole-struct "+
					"comparison is supposed to close", f.Name, rule.kind)
			}
		})
	}
}

// perturbConsumerField changes v to something a canonical group durable would not carry,
// and reports whether it knew how. The caller makes an unknown kind FATAL: a skip is a
// green run, so "unproven" and "proven" would be indistinguishable in the output.
func perturbConsumerField(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "-perturbed")
	case reflect.Bool:
		v.SetBool(!v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(v.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(v.Float() + 1)
	case reflect.Slice:
		v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem())))
	case reflect.Map:
		if v.IsNil() {
			v.Set(reflect.MakeMap(v.Type()))
		}
		v.SetMapIndex(reflect.ValueOf("perturbed").Convert(v.Type().Key()),
			reflect.Zero(v.Type().Elem()))
	case reflect.Pointer:
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		} else {
			v.Set(reflect.Zero(v.Type()))
		}
	default:
		return false
	}
	return true
}

// The two variable fields that carry bounds are bounded at the edges, not merely non-zero.
// "Variable" must not come to mean "unchecked".
func TestSharedGroupVariableFieldBounds(t *testing.T) {
	t.Parallel()

	const group, subject = "g", "foo"
	mk := func(ackWait time.Duration, maxAck int) *ConsumerConfig {
		c := buildSharedGroupConsumerConfig(group, subject, ackWait, maxAck, false)
		c.Name, c.MaxDeliver = c.Durable, -1
		return &c
	}

	for _, tc := range []struct {
		name       string
		cfg        *ConsumerConfig
		streamMax  int
		wantAccept bool
	}{
		{"canonical", mk(30*time.Second, 1024), 0, true},
		{"zero ack_wait", mk(0, 1024), 0, false},
		{"negative ack_wait", mk(-time.Second, 1024), 0, false},
		{"zero max_ack_pending", mk(30*time.Second, 0), 0, false},
		{"negative max_ack_pending", mk(30*time.Second, -1), 0, false},
		{"max_ack_pending at the limit", mk(30*time.Second, mqttMaxAckTotalLimit), 0, true},
		{"max_ack_pending above the limit", mk(30*time.Second, mqttMaxAckTotalLimit+1), 0, false},
		{"max_ack_pending at the stream's limit", mk(30*time.Second, 100), 100, true},
		{"max_ack_pending above the stream's limit", mk(30*time.Second, 101), 100, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := sharedGroupConfigUsable(tc.cfg, group, subject, tc.streamMax)
			if tc.wantAccept && err != nil {
				t.Fatalf("must be accepted, got: %v", err)
			}
			if !tc.wantAccept && err == nil {
				t.Fatal("must be refused, was accepted")
			}
		})
	}
}

// The two dispositions the pinned matrix deliberately skips need their own policy oracles,
// or "closed by construction" is only true of two thirds of the table.
//
// A holistic review made the point: skipping member-variable and server-owned fields leaves
// regressions alive in both directions — one that stops accepting a legitimate alternate
// MemoryStorage, and one that turns the metadata constraint into a free pass — while the
// documentation goes on claiming the whole struct is closed.
func TestSharedGroupNonPinnedDispositionsHaveOracles(t *testing.T) {
	t.Parallel()

	const group, subject = "g", "foo"
	canonical := func(memoryStorage bool) *ConsumerConfig {
		c := buildSharedGroupConsumerConfig(group, subject, 30*time.Second, 1024, memoryStorage)
		c.Name, c.MaxDeliver = c.Durable, -1
		return &c
	}

	// MEMBER-VARIABLE, accept direction. Both MemoryStorage values are ones this broker
	// produces, so a group created by a member running consumer_memory_storage must be
	// joinable. Only the accept direction is meaningful here: the field is unbounded by
	// design, and asserting a rejection would contradict that.
	t.Run("either MemoryStorage is accepted", func(t *testing.T) {
		t.Parallel()
		for _, ms := range []bool{false, true} {
			if err := sharedGroupConfigUsable(canonical(ms), group, subject, 0); err != nil {
				t.Fatalf("MemoryStorage=%v must be accepted — another member's broker may run it: %v", ms, err)
			}
		}
	})

	// SERVER-OWNED. The constraint is not "ignore Metadata" but "nothing beyond the three
	// NAMED server keys", so both directions matter.
	t.Run("server metadata is accepted", func(t *testing.T) {
		t.Parallel()
		cfg := canonical(false)
		cfg.Metadata = map[string]string{
			JSRequiredLevelMetadataKey: "0",
			JSServerVersionMetadataKey: "2.14.5",
			JSServerLevelMetadataKey:   "4",
		}
		if err := sharedGroupConfigUsable(cfg, group, subject, 0); err != nil {
			t.Fatalf("the server's own metadata keys must be accepted, or every healthy group "+
				"durable is refused: %v", err)
		}
	})

	t.Run("a foreign metadata key is refused", func(t *testing.T) {
		t.Parallel()
		cfg := canonical(false)
		cfg.Metadata = map[string]string{
			JSServerVersionMetadataKey: "2.14.5",
			"someone.elses.key":        "x",
		}
		if err := sharedGroupConfigUsable(cfg, group, subject, 0); err == nil {
			t.Fatal("a metadata key this broker never writes was ACCEPTED: the server-owned " +
				"disposition would then be a free pass rather than a constraint")
		}
	})

	// And every field in the table has SOME executed oracle — pinned and normalized through
	// the matrix above, variable and server-owned through this test. A disposition with no
	// oracle anywhere is a classification nobody checks.
	t.Run("every disposition kind is exercised somewhere", func(t *testing.T) {
		t.Parallel()
		seen := map[sharedGroupFieldKind]int{}
		for _, rule := range sharedGroupConfigRules {
			seen[rule.kind]++
		}
		for _, k := range []sharedGroupFieldKind{
			sharedGroupPinned, sharedGroupServerNormalized,
			sharedGroupMemberVariable, sharedGroupServerOwned,
		} {
			if seen[k] == 0 {
				t.Errorf("no field carries disposition %v; if a kind has become unused, delete it "+
					"rather than leaving an unexercised category in the table", k)
			}
		}
	})
}

// The resume verification never issues an unbounded consumer lookup.
//
// A holistic review called this a blocker, and the reason is correctness before it is
// resource use. A session's shared filter count is client-controlled (MaxSubs defaults to
// unlimited), one batched lookup shares a single timeout, and an indeterminate verdict
// REFUSES — so with one unbounded call a single lost response makes every durable in the
// session indeterminate, and a legitimately large session fails closed on every reconnect,
// permanently. Every other resume-time batch in this package is capped at
// mqttInflightLoadBatch for the same reason.
//
// The batch size can only be observed at the seam: the verdict map looks identical either
// way, which is exactly why this went unnoticed until review.
func TestSharedGroupLookupBatchIsBounded(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-batch-bound")

	// More targets than one batch may carry, so a single unbounded call is distinguishable
	// from correct chunking.
	const n = mqttInflightLoadBatch + 37
	targets := make([]mqttSharedGroupTarget, 0, n)
	for i := 0; i < n; i++ {
		subject := "bound/topic" + strconv.Itoa(i)
		_, _, durable := mqttSharedNames("bg", subject)
		targets = append(targets, mqttSharedGroupTarget{
			sid: "$share/bg/" + subject, group: "bg", subject: subject, durable: durable,
		})
	}

	var sizes []int
	e.asm.mu.Lock()
	e.asm.testSharedGroupLookupBatch = func(sz int) { sizes = append(sizes, sz) }
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testSharedGroupLookupBatch = nil
		e.asm.mu.Unlock()
	})

	verdicts := e.asm.classifySharedGroups(targets, mqttSharedGroupVerdictIsTerminal)

	if len(sizes) == 0 {
		t.Fatal("no lookup was issued, so this proves nothing about the batch size")
	}
	for i, sz := range sizes {
		if sz > mqttInflightLoadBatch {
			t.Fatalf("lookup %d requested %d durables, above the %d bound: one reconnect can then "+
				"build an arbitrarily large request set, and one lost response makes the whole "+
				"session indeterminate — which refuses", i, sz, mqttInflightLoadBatch)
		}
	}
	// The FIRST request is a full chunk, not the whole set. That is what distinguishes
	// chunking from a single unbounded call: with n deliberately above the bound, an
	// unbounded implementation would ask for all n at once.
	//
	// Note what is NOT asserted: that the chunks cover every target. These durables do not
	// exist, so the first chunk's first verdict is a refusal and the pass stops there by
	// design — one refusal already refuses the CONNECT (F-077 later made an authoritatively
	// ABSENT verdict repairable instead, which is a different arm and does not reach this
	// batching rule) — so any further lookup is work whose
	// result cannot change the outcome. An earlier version of this test required full
	// coverage and had to change when that rule landed; the discriminating property, and
	// the one M21 violates, is the request SIZE.
	if sizes[0] != mqttInflightLoadBatch {
		t.Fatalf("the first lookup requested %d durables from a set of %d, want a full chunk of %d",
			sizes[0], n, mqttInflightLoadBatch)
	}
	// Every target still receives a verdict, whether it was looked up or judged
	// indeterminate on the way out: a caller reading a missing entry as "nothing to check"
	// is the failure this whole change exists to prevent.
	if len(verdicts) != n {
		t.Fatalf("got %d verdicts for %d targets", len(verdicts), n)
	}
}

// A failed lookup chunk STOPS the verification; it does not walk the remaining chunks.
//
// The second holistic finding on this seam, and the reason is a stall the client controls.
// Each batched lookup carries its own timeout, so continuing after a transport failure
// would cost one timeout PER REMAINING CHUNK on a session whose filter count is its own
// (MaxSubs defaults to unlimited). In the POST-pass that is worse than latency: this
// connection's shared queue interest is already installed, so QoS 0 messages selected for
// it are dropped behind the restore gate for the whole stall.
//
// And continuing buys nothing — one indeterminate filter already refuses the whole CONNECT,
// so later chunks cannot rescue it. Every target must still receive a verdict, because a
// caller reading a missing entry as "fine" is the failure mode this whole change exists to
// prevent.
func TestSharedGroupLookupStopsAtTheFirstFailedChunk(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-batch-stop")

	const n = 3*mqttInflightLoadBatch + 11
	targets := make([]mqttSharedGroupTarget, 0, n)
	for i := 0; i < n; i++ {
		subject := "stop/topic" + strconv.Itoa(i)
		_, _, durable := mqttSharedNames("sg", subject)
		targets = append(targets, mqttSharedGroupTarget{
			sid: "$share/sg/" + subject, group: "sg", subject: subject, durable: durable,
		})
	}

	var calls int
	e.asm.mu.Lock()
	e.asm.testSharedGroupLookupBatch = func(int) { calls++ }
	e.asm.testSharedGroupLookupErr = func(int) error { return fmt.Errorf("injected transport failure") }
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testSharedGroupLookupBatch, e.asm.testSharedGroupLookupErr = nil, nil
		e.asm.mu.Unlock()
	})

	verdicts := e.asm.classifySharedGroups(targets, mqttSharedGroupVerdictIsTerminal)

	if calls != 1 {
		t.Fatalf("issued %d lookups after the first one failed, want exactly 1: each carries its own "+
			"timeout, so a client-sized session would stall the CONNECT once per chunk while its "+
			"already-installed queue interest drops QoS 0 deliveries", calls)
	}
	if len(verdicts) != n {
		t.Fatalf("got %d verdicts for %d targets: every target must be judged, since a missing "+
			"entry reads as 'nothing to check'", len(verdicts), n)
	}
	for sid, v := range verdicts {
		if !errors.Is(v.err, errSharedGroupIndeterminate) {
			t.Fatalf("sid %q got verdict %v, want indeterminate: a failed lookup establishes nothing, "+
				"and an unproven absence must never be reported as one", sid, v.err)
		}
	}
}

// A refusal that arrives as a RESPONSE — no transport error at all — stops the pass too.
//
// This is the third and last cut of the same seam, and the one that finally generalised it.
// Round 4 blocked on an unbounded batch; round 5 on continuing after a transport failure;
// round 6 on continuing after a perfectly transported response that happened to classify as
// a refusal. Each earlier fix special-cased the failure it had just been shown.
//
// There is no special case now: any verdict carrying an error is terminal for THIS BATCHING
// RULE, because one
// refused filter already refuses the whole CONNECT, so every later lookup is work whose
// result cannot change the outcome — while each one carries its own timeout and the
// post-pass runs with this connection's queue interest already installed, dropping QoS 0.
func TestSharedGroupLookupStopsAtAResponseLevelRefusal(t *testing.T) {
	t.Parallel()
	e := testNewInflightEnv(t, "client-batch-resp")

	const n = 2*mqttInflightLoadBatch + 5
	targets := make([]mqttSharedGroupTarget, 0, n)
	for i := 0; i < n; i++ {
		subject := "resp/topic" + strconv.Itoa(i)
		_, _, durable := mqttSharedNames("rs", subject)
		targets = append(targets, mqttSharedGroupTarget{
			sid: "$share/rs/" + subject, group: "rs", subject: subject, durable: durable,
		})
	}

	var calls int
	e.asm.mu.Lock()
	e.asm.testSharedGroupLookupBatch = func(int) { calls++ }
	// Deliberately NOT testSharedGroupLookupErr: the lookup succeeds at the transport
	// level and the refusal comes from classifying one response. That is precisely the
	// case the transport-error-only early return walked straight past.
	e.asm.testSharedGroupLookupFail = func(string) error { return errSharedGroupIndeterminate }
	e.asm.mu.Unlock()
	t.Cleanup(func() {
		e.asm.mu.Lock()
		e.asm.testSharedGroupLookupBatch, e.asm.testSharedGroupLookupFail = nil, nil
		e.asm.mu.Unlock()
	})

	verdicts := e.asm.classifySharedGroups(targets, mqttSharedGroupVerdictIsTerminal)

	if calls != 1 {
		t.Fatalf("issued %d lookups after a response-level refusal, want exactly 1: the outcome was "+
			"already terminal, so the rest are round trips the client can lengthen at will while "+
			"installed queue interest drops QoS 0 deliveries", calls)
	}
	if len(verdicts) != n {
		t.Fatalf("got %d verdicts for %d targets: every target must be judged", len(verdicts), n)
	}
	for sid, v := range verdicts {
		if v.err == nil {
			t.Fatalf("sid %q was accepted while the pass was abandoning: an unexamined target must "+
				"never come back usable", sid)
		}
	}
}
