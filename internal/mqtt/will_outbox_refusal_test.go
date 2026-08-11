//go:build mqttport_fulltests

// Refusal of unsupported delayed-Will outbox records (F-058).
//
// $MQTT_willq carries no released format contract, so a record that is not one
// this broker can have written is refused at every decode boundary rather than
// migrated into a bucket that cannot be arbitrated. Refusal is read-only: the
// record is never claimed, published, cancelled or deleted, which is why every
// assertion below observes the STORED BYTES. Observing through loadWillOutbox
// would be vacuous — it is one of the boundaries that now errors on exactly
// these records, so "no record came back" would hold whether the mechanism works
// or not.
//
// Every fail-closed assertion is paired with a VALID sibling in the same pass, so
// "the invalid record was untouched" can never be satisfied by "nothing ran".

package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// seedWillRecordRaw stores raw bytes as the record on an explicit subject with a
// create-only CAS, for fixtures a valid mqttWillOutbox value cannot express: an
// undecodable body, or a body whose identities do not name its own subject.
func seedWillRecordRaw(t testing.TB, asm *mqttAccountSessionManager, subject string, data []byte) uint64 {
	t.Helper()
	bb := strings.Builder{}
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":0")
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	msg := append([]byte(bb.String()), data...)
	resp, err := asm.jsa.storeMsg(subject, hdr, msg)
	if err != nil {
		t.Fatalf("seed raw will record on %q: %v", subject, err)
	}
	return resp.Sequence
}

// rawWillBytes returns the stored body of the current record on subject, or nil
// when the subject holds no record. This is the only honest way to observe a
// refused record: it never decodes it.
func rawWillBytes(t testing.TB, asm *mqttAccountSessionManager, subject string) []byte {
	t.Helper()
	smsg, err := asm.jsa.loadLastMsgFor(mqttWillQStreamName, subject)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("raw load of %q: %v", subject, err)
		}
		return nil
	}
	return smsg.Data
}

// requireWillBytesUnchanged asserts the record on subject is byte-identical to
// want: not advanced to publishing, not cancelled, not deleted.
func requireWillBytesUnchanged(t testing.TB, asm *mqttAccountSessionManager, subject string, want []byte) {
	t.Helper()
	got := rawWillBytes(t, asm, subject)
	if got == nil {
		t.Fatalf("refused record on %q was DELETED; it must be left in place", subject)
	}
	if string(got) != string(want) {
		t.Fatalf("refused record on %q was MUTATED:\n got %s\nwant %s", subject, got, want)
	}
}

// testMQTTCountWillRecordsForClient returns the number of outbox records on one
// client's filter, read raw so a refused record still counts.
func testMQTTCountWillRecordsForClient(t testing.TB, asm *mqttAccountSessionManager, cidHash string) int {
	t.Helper()
	n := 0
	for seq := uint64(1); ; {
		smsg, err := asm.jsa.loadNextMsgFromSeq(mqttWillQStreamName, mqttWillQClientFilter(asm.domainTk, cidHash), seq)
		if err != nil {
			break
		}
		seq = smsg.Sequence + 1
		n++
	}
	return n
}

// dueWillRecord is a well-formed, due, pending record: the shape every variant
// below starts from and then breaks in exactly one way, so each variant isolates
// the invariant named in its case name.
func dueWillRecord(asm *mqttAccountSessionManager, cidHash, willID string) *mqttWillOutbox {
	return &mqttWillOutbox{
		WillID: willID, ClientIDHash: cidHash, Gen: mqttNewSessGen(),
		Topic: "will/refusal", Msg: []byte(willID), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	}
}

// openWillPublishRecorder installs a non-blocking publish seam that records every
// publication by WillID without performing one, and returns a reader for the
// counts. Unlike the parking gate, nothing has to be released.
func openWillPublishRecorder(asm *mqttAccountSessionManager) (func(willID string) int, func()) {
	var mu sync.Mutex
	counts := map[string]int{}
	setWillPublishHook(asm, func(rec *mqttWillOutbox) error {
		mu.Lock()
		counts[rec.WillID]++
		mu.Unlock()
		return nil
	})
	count := func(willID string) int {
		mu.Lock()
		defer mu.Unlock()
		return counts[willID]
	}
	return count, func() { setWillPublishHook(asm, nil) }
}

// refusedWillFixture is one unsupported record: the subject it is stored on, the
// exact bytes stored, and the WillID the claim scan would publish under if the
// refusal failed.
type refusedWillFixture struct {
	name    string
	subject string
	data    []byte
	willID  string // _EMPTY_ when the body has none to publish under
}

// seedRefusedWillFixtures stores one record per unsupported shape and returns
// them. Each lives on its own client hash (except the deliberate mismatch), so no
// two fixtures interact.
func seedRefusedWillFixtures(t *testing.T, asm *mqttAccountSessionManager, state mqttWillState, prep func(*mqttWillOutbox)) []refusedWillFixture {
	t.Helper()
	// Each entry breaks exactly one invariant of dueWillRecord.
	breaks := []struct {
		name string
		// mutate breaks the record; it returns the subject to store it on, or
		// _EMPTY_ to use the subject the record's own identities name.
		mutate func(rec *mqttWillOutbox, cidHash string) string
	}{
		{"missing-generation", func(rec *mqttWillOutbox, _ string) string {
			rec.Gen = _EMPTY_
			return _EMPTY_
		}},
		{"out-of-alphabet-generation", func(rec *mqttWillOutbox, _ string) string {
			rec.Gen = strings.Repeat("!", mqttSessGenLen)
			return _EMPTY_
		}},
		{"zero-origin-sequence", func(rec *mqttWillOutbox, _ string) string {
			rec.OriginSessionSeq = 0
			return _EMPTY_
		}},
		{"malformed-origin-epoch", func(rec *mqttWillOutbox, _ string) string {
			rec.OriginSessEpoch = mqttSessEpochNUIDPrefix + strings.Repeat("!", 22)
			return _EMPTY_
		}},
		{"foreign-identity-on-another-clients-subject", func(rec *mqttWillOutbox, cidHash string) string {
			// A fully well-formed body — only its BINDING is wrong. Fenced with the
			// foreign client's watermark and session state, then claimed and published
			// on this client's subject, if the binding were not checked.
			subject := mqttWillQSubject(asm.domainTk, cidHash, rec.WillID)
			rec.ClientIDHash = getHash("refusal-foreign-owner")
			return subject
		}},
		{"dotted-will-id", func(rec *mqttWillOutbox, _ string) string {
			// Exactly bound to its own subject, and visible to the account filter
			// (<domainTk>>) — but its extra token makes it invisible to the
			// reconnect-cancel filter (<cidHash>.*), so a reconnect entitled to
			// suppress it could not reach it.
			rec.WillID = "dotted.willid"
			return _EMPTY_
		}},
		{"unknown-state", func(rec *mqttWillOutbox, _ string) string {
			rec.State = mqttWillState("migrating")
			return _EMPTY_
		}},
	}
	out := make([]refusedWillFixture, 0, len(breaks)+1)
	for i, b := range breaks {
		cidHash := getHash(fmt.Sprintf("refusal-%s-%d", b.name, i))
		willID := fmt.Sprintf("refusal-w%d", i)
		rec := dueWillRecord(asm, cidHash, willID)
		rec.State = state
		if prep != nil {
			prep(rec)
		}
		subject := b.mutate(rec, cidHash)
		if subject == _EMPTY_ {
			subject = mqttWillQSubject(asm.domainTk, rec.ClientIDHash, rec.WillID)
		}
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal %s fixture: %v", b.name, err)
		}
		seedWillRecordRaw(t, asm, subject, data)
		out = append(out, refusedWillFixture{name: b.name, subject: subject, data: data, willID: rec.WillID})
	}
	// An undecodable body. Kept to pin the refusal diagnostic path, NOT as evidence
	// for the validation call: json.Unmarshal rejects it with or without validation.
	cidHash := getHash("refusal-undecodable")
	subject := mqttWillQSubject(asm.domainTk, cidHash, "refusal-undecodable-w")
	data := []byte(`{"wid":"refusal-undecodable-w","state":`)
	seedWillRecordRaw(t, asm, subject, data)
	out = append(out, refusedWillFixture{name: "undecodable-body", subject: subject, data: data})
	return out
}

// TestMQTTWillOutboxScanRefusesUnsupportedRecords proves the claim scan refuses
// every unsupported shape of a DUE PENDING record: none is claimed, none is
// published, and none is deleted. The valid sibling is seeded LAST, so its firing
// proves the scan walked past every refused record rather than stopping at one.
func TestMQTTWillOutboxScanRefusesUnsupportedRecords(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	published, restore := openWillPublishRecorder(asm)
	defer restore()

	refused := seedRefusedWillFixtures(t, asm, mqttWillPending, nil)

	// The control: a well-formed due record with no watermark and no session
	// record, so the fence proceeds and it fires.
	okHash := getHash("refusal-control-pub")
	okRec := dueWillRecord(asm, okHash, "refusal-control-w")
	okSubject, _ := seedWillRecord(t, asm, okRec)

	// The control fires and its record drains, leaving EXACTLY the refused ones.
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if n := published(okRec.WillID); n != 1 {
			return fmt.Errorf("control Will published %d times, want 1", n)
		}
		if b := rawWillBytes(t, asm, okSubject); b != nil {
			return fmt.Errorf("control record not drained yet")
		}
		return nil
	})
	// Two further scan intervals: a refusal that merely arrives late is not a
	// refusal.
	time.Sleep(3 * mqttWillClaimInterval)

	// Per variant first, so a failure names the invariant that stopped holding;
	// the account-wide count then catches anything seeded but not enumerated.
	for _, f := range refused {
		t.Run(f.name, func(t *testing.T) {
			requireWillBytesUnchanged(t, asm, f.subject, f.data)
			if f.willID != _EMPTY_ {
				if n := published(f.willID); n != 0 {
					t.Fatalf("refused Will %q was published %d times", f.willID, n)
				}
			}
		})
	}
	if n := testMQTTCountWillRecords(t, asm); n != len(refused) {
		t.Fatalf("outbox holds %d records, want the %d refused ones", n, len(refused))
	}
}

// TestMQTTWillOutboxScanRefusesUnsupportedExpiredClaim proves the crashed-publisher
// recovery path refuses the same records: an unsupported PUBLISHING record whose
// lease has expired is never reclaimed, while a valid expired-lease sibling is.
func TestMQTTWillOutboxScanRefusesUnsupportedExpiredClaim(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	published, restore := openWillPublishRecorder(asm)
	defer restore()

	expired := func(rec *mqttWillOutbox) {
		rec.ClaimOwner = "dead-node"
		rec.ClaimUntil = time.Now().Add(-time.Minute).UnixNano()
	}
	refused := seedRefusedWillFixtures(t, asm, mqttWillPublishing, expired)

	okHash := getHash("reclaim-control-pub")
	okRec := dueWillRecord(asm, okHash, "reclaim-control-w")
	okRec.State = mqttWillPublishing
	expired(okRec)
	okSubject, _ := seedWillRecord(t, asm, okRec)

	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if n := published(okRec.WillID); n != 1 {
			return fmt.Errorf("control Will republished %d times, want 1", n)
		}
		if b := rawWillBytes(t, asm, okSubject); b != nil {
			return fmt.Errorf("control record not drained yet")
		}
		return nil
	})
	time.Sleep(3 * mqttWillClaimInterval)

	for _, f := range refused {
		t.Run(f.name, func(t *testing.T) {
			requireWillBytesUnchanged(t, asm, f.subject, f.data)
			if f.willID != _EMPTY_ {
				if n := published(f.willID); n != 0 {
					t.Fatalf("refused Will %q was republished %d times", f.willID, n)
				}
			}
		})
	}
}

// TestMQTTWillOutboxScanNeverReapsUnsupportedTerminalRecord proves the reaper is
// refused too. A terminal-SHAPED unsupported record is left in place: deleting it
// would be a mutation of state this broker has just declared it cannot interpret,
// and unlike a retained-message cache entry there is no second copy to re-derive
// it from. A valid done sibling is reaped in the same pass.
func TestMQTTWillOutboxScanNeverReapsUnsupportedTerminalRecord(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	refused := seedRefusedWillFixtures(t, asm, mqttWillDone, nil)

	okHash := getHash("reap-control-pub")
	okRec := dueWillRecord(asm, okHash, "reap-control-w")
	okRec.State = mqttWillDone
	okSubject, _ := seedWillRecord(t, asm, okRec)

	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if b := rawWillBytes(t, asm, okSubject); b != nil {
			return fmt.Errorf("valid done record not reaped yet")
		}
		return nil
	})
	time.Sleep(3 * mqttWillClaimInterval)

	for _, f := range refused {
		t.Run(f.name, func(t *testing.T) {
			requireWillBytesUnchanged(t, asm, f.subject, f.data)
		})
	}
}

// TestMQTTWillOutboxCancelRefusesUnsupportedRecords proves the reconnect cancel
// pass refuses them as well: an unsupported record is never CAS-transitioned to
// cancelled, and — because no fire path will act on it either — its presence does
// not make the pass unresolved. The pass must still return nil, or every reconnect
// of that client would log an unresolved cancellation forever.
func TestMQTTWillOutboxCancelRefusesUnsupportedRecords(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	// One client hash for the whole pass: the cancel filter is per client, so the
	// refused records and the control must share it. FireAt is far in the future so
	// the claim loop leaves every record pending and the cancel is the only writer.
	cidHash := getHash("cancel-refusal-pub")
	far := time.Now().Add(time.Hour).UnixNano()

	type seeded struct {
		name    string
		subject string
		data    []byte
	}
	var refused []seeded
	seed := func(name string, mutate func(rec *mqttWillOutbox) string) {
		rec := dueWillRecord(asm, cidHash, "cancel-"+name)
		rec.FireAt = far
		subject := mutate(rec)
		if subject == _EMPTY_ {
			subject = mqttWillQSubject(asm.domainTk, rec.ClientIDHash, rec.WillID)
		}
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatalf("marshal %s fixture: %v", name, err)
		}
		seedWillRecordRaw(t, asm, subject, data)
		refused = append(refused, seeded{name, subject, data})
	}
	seed("missing-generation", func(rec *mqttWillOutbox) string {
		rec.Gen = _EMPTY_
		return _EMPTY_
	})
	seed("zero-origin-sequence", func(rec *mqttWillOutbox) string {
		rec.OriginSessionSeq = 0
		return _EMPTY_
	})
	seed("foreign-identity-on-another-clients-subject", func(rec *mqttWillOutbox) string {
		subject := mqttWillQSubject(asm.domainTk, cidHash, rec.WillID)
		rec.ClientIDHash = getHash("cancel-refusal-foreign")
		return subject
	})

	okRec := dueWillRecord(asm, cidHash, "cancel-control-w")
	okRec.FireAt = far
	okSubject, _ := seedWillRecord(t, asm, okRec)

	// The unscoped reconnect cancel: entitled to every incarnation's records.
	if err := asm.cancelWillOutbox(s, cidHash); err != nil {
		t.Fatalf("cancel pass must resolve: a refused record is not an unresolved pending one: %v", err)
	}

	// The control proves the pass ran over this client's filter at all.
	cur, _, err := asm.jsa.loadWillOutbox(asm.domainTk, okSubject)
	if err != nil || cur == nil || cur.State != mqttWillCancelled {
		t.Fatalf("valid sibling must be cancelled, got rec=%v err=%v", cur, err)
	}
	for _, f := range refused {
		t.Run(f.name, func(t *testing.T) {
			requireWillBytesUnchanged(t, asm, f.subject, f.data)
		})
	}
}

// TestMQTTWillOutboxLoadRefusesUnsupportedRecord proves the point loader reports an
// unsupported record as an ERROR, never as a record and never as absent. Both
// fixtures are well-formed JSON broken only semantically: an undecodable one would
// make this vacuous, since json.Unmarshal supplies the error either way.
func TestMQTTWillOutboxLoadRefusesUnsupportedRecord(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	far := time.Now().Add(time.Hour).UnixNano()
	load := func(name string, mutate func(rec *mqttWillOutbox)) (*mqttWillOutbox, error) {
		cidHash := getHash("load-refusal-" + name)
		rec := dueWillRecord(asm, cidHash, "load-"+name)
		rec.FireAt = far
		mutate(rec)
		subject := mqttWillQSubject(asm.domainTk, rec.ClientIDHash, rec.WillID)
		data, merr := json.Marshal(rec)
		if merr != nil {
			t.Fatalf("marshal %s fixture: %v", name, merr)
		}
		seedWillRecordRaw(t, asm, subject, data)
		got, _, err := asm.jsa.loadWillOutbox(asm.domainTk, subject)
		return got, err
	}

	for _, tc := range []struct {
		name   string
		mutate func(rec *mqttWillOutbox)
	}{
		{"missing-generation", func(rec *mqttWillOutbox) { rec.Gen = _EMPTY_ }},
		{"unknown-state", func(rec *mqttWillOutbox) { rec.State = mqttWillState("migrating") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := load(tc.name, tc.mutate)
			if err == nil {
				t.Fatalf("loader accepted an unsupported record: %+v", got)
			}
			if got != nil {
				t.Fatalf("loader returned a record alongside the error: %+v", got)
			}
		})
	}

	// The control: a well-formed record still loads, so the arm above is a refusal
	// and not a loader that has stopped returning anything.
	okRec, err := load("control", func(rec *mqttWillOutbox) {})
	if err != nil || okRec == nil {
		t.Fatalf("a valid record must still load: rec=%v err=%v", okRec, err)
	}
}

// TestMQTTWillOutboxEnqueueRefusesUnsupportedRecord proves the writer refuses too:
// a record this broker would itself refuse to read is never persisted, because
// persisting one converts a sacrificed Will Delay into permanent loss plus a
// permanent operator diagnostic. The Will takes the existing immediate-publish
// fail-safe instead.
//
// Unreachable from production callers (all pass a live session's generation), so
// this drives the invariant arm directly.
func TestMQTTWillOutboxEnqueueRefusesUnsupportedRecord(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	var mu sync.Mutex
	var publishedTopics []string
	setWillPublishHook(asm, func(rec *mqttWillOutbox) error {
		mu.Lock()
		publishedTopics = append(publishedTopics, rec.Topic)
		mu.Unlock()
		return nil
	})
	defer setWillPublishHook(asm, nil)

	cidHash := getHash("enqueue-refusal-pub")
	will := &mqttWill{topic: []byte("will/enqueue-refusal"), message: []byte("failsafe")}
	// _EMPTY_ generation: the F-058 invariant, breaking nothing else.
	err := asm.enqueueDelayedWill(s, cidHash, _EMPTY_, will, nil, _EMPTY_,
		time.Now().Add(time.Hour).UnixNano(), 1, asm.sessStreamEpoch)
	if err != nil {
		t.Fatalf("the fail-safe publish must succeed: %v", err)
	}

	if n := testMQTTCountWillRecordsForClient(t, asm, cidHash); n != 0 {
		t.Fatalf("an unsupported record was persisted: %d records on the client's filter", n)
	}
	mu.Lock()
	got := append([]string(nil), publishedTopics...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "will/enqueue-refusal" {
		t.Fatalf("want exactly one immediate fail-safe publication of the Will, got %v", got)
	}

	// The control: the same call with a valid generation DOES persist, so the
	// assertion above is a refusal and not an enqueue that never works here.
	okHash := getHash("enqueue-control-pub")
	if err := asm.enqueueDelayedWill(s, okHash, mqttNewSessGen(), will, nil, _EMPTY_,
		time.Now().Add(time.Hour).UnixNano(), 1, asm.sessStreamEpoch); err != nil {
		t.Fatalf("valid enqueue: %v", err)
	}
	if n := testMQTTCountWillRecordsForClient(t, asm, okHash); n != 1 {
		t.Fatalf("valid enqueue must persist exactly one record, got %d", n)
	}
	mu.Lock()
	after := len(publishedTopics)
	mu.Unlock()
	if after != 1 {
		t.Fatalf("a confirmed enqueue must NOT also publish immediately: %d publications", after)
	}
}

// TestMQTTWillOutboxEnqueueExhaustionAfterLandedWriteDuplicates pins what the OTHER
// caller of publishWillFailSafe really guarantees, because it is weaker than the
// writer-side refusal above and an earlier draft of that helper's comment claimed
// they were the same.
//
// Retry exhaustion means UNCONFIRMED, not proven absent. A create can land while its
// acknowledgement is lost; if every bounded re-read is ALSO indeterminate, the loop
// exits into the fail-safe with a durable pending record still in place. The Will is
// then published immediately AND fires again from the record. That is the outbox's
// sanctioned at-least-once duplicate reached by a second route, and the alternative —
// staying silent about an enqueue that may never have landed — is loss.
//
// Pre-existing behavior; this test exists so the guarantee is written down and cannot
// be quietly upgraded to "no record can exist here". A test that only forced the
// store to fail would never see it: the record has to LAND for the duplicate to be
// possible, which is what the store seam does here.
func TestMQTTWillOutboxEnqueueExhaustionAfterLandedWriteDuplicates(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	var mu sync.Mutex
	var published int
	setWillPublishHook(asm, func(rec *mqttWillOutbox) error {
		mu.Lock()
		published++
		mu.Unlock()
		return nil
	})
	defer setWillPublishHook(asm, nil)

	// The first store LANDS for real and then reports a lost ack; every later
	// create-only attempt loses to the record it wrote. So a durable pending record
	// exists from attempt 0 onwards.
	var landed atomic.Bool
	setWillEnqueueStoreHook(asm, func(subject string, expectSeq uint64, rec *mqttWillOutbox) (uint64, error) {
		if !landed.Load() {
			if _, err := asm.jsa.storeWillOutboxCAS(subject, expectSeq, rec); err != nil {
				return 0, err
			}
			landed.Store(true)
		}
		return 0, fmt.Errorf("injected lost ack")
	})
	defer setWillEnqueueStoreHook(asm, nil)

	// And every ambiguity re-read is indeterminate, so the loop can never CONFIRM the
	// record it actually wrote and runs out of attempts.
	restoreLoad := setMQTTTestLoadLastMsgHook(func(stream, subject string) error {
		if stream == mqttWillQStreamName {
			return fmt.Errorf("injected indeterminate re-read")
		}
		return nil
	})
	defer restoreLoad()

	cidHash := getHash("exhaust-landed-pub")
	will := &mqttWill{topic: []byte("will/exhaust-landed"), message: []byte("dup")}
	if err := asm.enqueueDelayedWill(s, cidHash, mqttNewSessGen(), will, nil, _EMPTY_,
		time.Now().Add(time.Hour).UnixNano(), 1, asm.sessStreamEpoch); err != nil {
		t.Fatalf("the fail-safe publish must succeed: %v", err)
	}
	restoreLoad()

	if !landed.Load() {
		t.Fatal("the store seam never landed a record; the duplicate window was not exercised")
	}
	// Published immediately, WHILE a durable pending record survives: exactly the
	// combination the helper's contract must not deny.
	mu.Lock()
	n := published
	mu.Unlock()
	if n != 1 {
		t.Fatalf("want one immediate fail-safe publication, got %d", n)
	}
	if got := testMQTTCountWillRecordsForClient(t, asm, cidHash); got != 1 {
		t.Fatalf("want the landed pending record to survive the fail-safe, got %d records", got)
	}
}
