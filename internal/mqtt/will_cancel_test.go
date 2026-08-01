//go:build mqttport_fulltests

package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// setWillCancelWatermarkLoadHook installs (or clears) the instance-scoped
// watermark-load seam on asm.
func setWillCancelWatermarkLoadHook(asm *mqttAccountSessionManager, fn func(cidHash string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus)) {
	asm.mu.Lock()
	asm.testCancelWatermarkLoad = fn
	asm.mu.Unlock()
}

// setWillCancelWatermarkStoreHook installs (or clears) the instance-scoped
// watermark-store (advance-only) CAS seam on asm.
func setWillCancelWatermarkStoreHook(asm *mqttAccountSessionManager, fn func(subject string, expectSeq uint64, wm *mqttWillCancelWatermark) (uint64, error)) {
	asm.mu.Lock()
	asm.testCancelWatermarkStore = fn
	asm.mu.Unlock()
}

// setRollbackSessionStoreHook installs (or clears) the instance-scoped
// rollback-only session-store seam on asm.
func setRollbackSessionStoreHook(asm *mqttAccountSessionManager, fn func(ps *mqttPersistedSession, idHash string, seq uint64) error) {
	asm.mu.Lock()
	asm.testRollbackSessionStore = fn
	asm.mu.Unlock()
}

// setRollbackSessionLoadHook installs (or clears) the instance-scoped
// rollback-only session-load seam on asm.
func setRollbackSessionLoadHook(asm *mqttAccountSessionManager, fn func(cidHash string) (*mqttPersistedSession, uint64, error)) {
	asm.mu.Lock()
	asm.testRollbackSessionLoad = fn
	asm.mu.Unlock()
}

// seedWatermark stores a watermark record directly on asm's $MQTT_willcancel
// stream (create-only CAS), for tests that need a marker without a live CONNECT.
func seedWatermark(t testing.TB, asm *mqttAccountSessionManager, wm *mqttWillCancelWatermark) uint64 {
	t.Helper()
	subject := mqttWillCancelSubject(asm.domainTk, wm.ClientIDHash)
	seq, err := asm.jsa.storeWillCancelCAS(subject, 0, wm)
	if err != nil {
		t.Fatalf("seed watermark: %v", err)
	}
	return seq
}

// seedWatermarkRaw stores an arbitrary payload at cidHash's watermark subject
// (create-only CAS), for corrupt/cross-client marker tests.
func seedWatermarkRaw(t testing.TB, asm *mqttAccountSessionManager, cidHash string, payload []byte) uint64 {
	t.Helper()
	subject := mqttWillCancelSubject(asm.domainTk, cidHash)
	var bb bytes.Buffer
	bb.WriteString(hdrLine)
	bb.WriteString(JSExpectedLastSubjSeq)
	bb.WriteString(":0")
	bb.WriteString(CR_LF)
	bb.WriteString(CR_LF)
	hdr := bb.Len()
	bb.Write(payload)
	resp, err := asm.jsa.storeMsg(subject, hdr, bb.Bytes())
	if err != nil {
		t.Fatalf("seed raw watermark: %v", err)
	}
	return resp.Sequence
}

// TestMQTTWillOutboxWatermarkMonotonicCAS proves the advance never lowers a
// watermark: after advancing to 100, an out-of-order advance to 90 is a no-op.
func TestMQTTWillOutboxWatermarkMonotonicCAS(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	cid := getHash("wm-mono")
	if err := asm.advanceWillCancelWatermark(cid, 100); err != nil {
		t.Fatalf("advance 100: %v", err)
	}
	if err := asm.advanceWillCancelWatermark(cid, 90); err != nil {
		t.Fatalf("advance 90: %v", err)
	}
	wm, _, status := asm.loadWillCancelWatermark(cid)
	if status != mqttWillCancelValid {
		t.Fatalf("status = %v, want valid", status)
	}
	if wm.SessionSeq != 100 {
		t.Fatalf("SessionSeq = %d, want 100 (monotonic; 90 must not lower it)", wm.SessionSeq)
	}
	if wm.SessEpoch != asm.sessStreamEpoch {
		t.Fatalf("epoch = %q, want current %q", wm.SessEpoch, asm.sessStreamEpoch)
	}
}

// TestMQTTWillOutboxWatermarkAmbiguousAck proves an advance whose CAS store lands
// but returns an error is confirmed by a re-read rather than failing the CONNECT.
func TestMQTTWillOutboxWatermarkAmbiguousAck(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	cid := getHash("wm-amb")
	// The store lands the real write, then reports an (ambiguous) error.
	setWillCancelWatermarkStoreHook(asm, func(subject string, expect uint64, wm *mqttWillCancelWatermark) (uint64, error) {
		_, _ = asm.jsa.storeWillCancelCAS(subject, expect, wm)
		return 0, fmt.Errorf("injected ambiguous store error")
	})
	defer setWillCancelWatermarkStoreHook(asm, nil)

	if err := asm.advanceWillCancelWatermark(cid, 50); err != nil {
		t.Fatalf("advance must succeed via ambiguous-write confirmation, got %v", err)
	}
	setWillCancelWatermarkStoreHook(asm, nil)
	wm, _, status := asm.loadWillCancelWatermark(cid)
	if status != mqttWillCancelValid || wm.SessionSeq != 50 {
		t.Fatalf("re-read = (%v, seq %v), want valid seq 50", status, wmSeq(wm))
	}
}

// TestMQTTWillOutboxWatermarkFailsWhenUnconfirmable proves a store failure that
// does NOT land (nothing durable) surfaces an error so the CONNECT is rejected.
func TestMQTTWillOutboxWatermarkFailsWhenUnconfirmable(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	cid := getHash("wm-fail")
	var calls int32
	setWillCancelWatermarkStoreHook(asm, func(subject string, expect uint64, wm *mqttWillCancelWatermark) (uint64, error) {
		atomic.AddInt32(&calls, 1)
		return 0, fmt.Errorf("injected hard store error")
	})
	defer setWillCancelWatermarkStoreHook(asm, nil)

	if err := asm.advanceWillCancelWatermark(cid, 5); err == nil {
		t.Fatal("advance must fail when the store never lands")
	}
	if n := atomic.LoadInt32(&calls); n != mqttWillCancelAdvanceAttempts {
		t.Fatalf("store attempts = %d, want %d (bounded retry)", n, mqttWillCancelAdvanceAttempts)
	}
}

// TestMQTTWillOutboxWatermarkSelfHealsCorrupt proves a corrupt/cross-client marker
// is authoritatively CAS-overwritten by the next advance (self-heal).
func TestMQTTWillOutboxWatermarkSelfHealsCorrupt(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	// (a) Malformed JSON marker: load classifies corrupt; advance repairs it.
	cid := getHash("wm-corrupt")
	seedWatermarkRaw(t, asm, cid, []byte("{not-json"))
	if _, _, st := asm.loadWillCancelWatermark(cid); st != mqttWillCancelCorrupt {
		t.Fatalf("malformed marker status = %v, want corrupt", st)
	}
	if err := asm.advanceWillCancelWatermark(cid, 7); err != nil {
		t.Fatalf("advance over corrupt marker: %v", err)
	}
	if wm, _, st := asm.loadWillCancelWatermark(cid); st != mqttWillCancelValid || wm.SessionSeq != 7 {
		t.Fatalf("after repair = (%v, seq %v), want valid seq 7", st, wmSeq(wm))
	}

	// (b) Cross-client poisoning: a valid record carrying a FOREIGN cid at this
	// client's subject is corrupt (cid mismatch), never suppresses, and is repaired.
	cid2 := getHash("wm-foreign")
	foreign := &mqttWillCancelWatermark{ClientIDHash: getHash("someone-else"), SessionSeq: 999, SessEpoch: asm.sessStreamEpoch}
	b, _ := json.Marshal(foreign)
	seedWatermarkRaw(t, asm, cid2, b)
	if _, _, st := asm.loadWillCancelWatermark(cid2); st != mqttWillCancelCorrupt {
		t.Fatalf("foreign-cid marker status = %v, want corrupt", st)
	}
	if err := asm.advanceWillCancelWatermark(cid2, 3); err != nil {
		t.Fatalf("advance over foreign marker: %v", err)
	}
	if wm, _, st := asm.loadWillCancelWatermark(cid2); st != mqttWillCancelValid || wm.ClientIDHash != cid2 || wm.SessionSeq != 3 {
		t.Fatalf("after repair = (%v, cid %q, seq %v), want valid own-cid seq 3", st, wmCid(wm), wmSeq(wm))
	}
}

// TestMQTTWillOutboxWatermarkStaleEpochFallbackThenOverwrite proves a marker from a
// different incarnation classifies stale-epoch (fall back to the session-record
// fence) and is overwritten with the current epoch by the next advance.
func TestMQTTWillOutboxWatermarkStaleEpochFallbackThenOverwrite(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	cid := getHash("wm-stale")
	seedWatermark(t, asm, &mqttWillCancelWatermark{ClientIDHash: cid, SessionSeq: 500, SessEpoch: mqttGenSessEpoch()})
	if _, _, st := asm.loadWillCancelWatermark(cid); st != mqttWillCancelStaleEpoch {
		t.Fatalf("status = %v, want stale-epoch", st)
	}
	if err := asm.advanceWillCancelWatermark(cid, 2); err != nil {
		t.Fatalf("advance over stale-epoch marker: %v", err)
	}
	if wm, _, st := asm.loadWillCancelWatermark(cid); st != mqttWillCancelValid || wm.SessEpoch != asm.sessStreamEpoch || wm.SessionSeq != 2 {
		t.Fatalf("after overwrite = (%v, epoch %q, seq %v), want valid current-epoch seq 2", st, wmEpoch(wm), wmSeq(wm))
	}
}

// TestMQTTWillOutboxWatermarkOutageAbortsScanAfterOneLoad (plan test 17): when the
// marker stream is unavailable, a single scan over many due Wills incurs at most ONE
// indeterminate watermark load (it aborts after the first, leaving every record
// pending) rather than one timeout per due record.
func TestMQTTWillOutboxWatermarkOutageAbortsScanAfterOneLoad(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	// Marker load is a transient/global failure for every client. Installed BEFORE
	// seeding so no concurrent background scan can fall back and fire a record: every
	// scan (background or explicit) aborts on the first indeterminate load.
	var loads int32
	setWillCancelWatermarkLoadHook(asm, func(cidHash string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus) {
		atomic.AddInt32(&loads, 1)
		return nil, 0, mqttWillCancelIndeterminate
	})
	defer setWillCancelWatermarkLoadHook(asm, nil)

	// Seed several due records for distinct clients (all with valid native origins).
	const n = 5
	var subjects []string
	for i := 0; i < n; i++ {
		subj, _ := seedWillRecord(t, asm, &mqttWillOutbox{
			WillID: fmt.Sprintf("outage-w%d", i), ClientIDHash: getHash(fmt.Sprintf("outage-pub%d", i)),
			Topic: "will/outage", Msg: []byte("later"), QoS: 0,
			FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
			OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
		})
		subjects = append(subjects, subj)
	}

	// A single explicit scan must abort after the FIRST record's marker load.
	before := atomic.LoadInt32(&loads)
	asm.scanWillOutbox(s)
	if delta := atomic.LoadInt32(&loads) - before; delta != 1 {
		t.Fatalf("one scan performed %d marker loads over %d due records, want exactly 1 (abort-after-one)", delta, n)
	}

	// Every record remains pending (deferred, never fired or cancelled).
	for _, subj := range subjects {
		rec, _, err := asm.jsa.loadWillOutbox(subj)
		if err != nil || rec == nil || rec.State != mqttWillPending {
			got := "nil"
			if rec != nil {
				got = string(rec.State)
			}
			t.Fatalf("record %s not pending after marker-outage scan: state=%s err=%v", subj, got, err)
		}
	}
}

// TestValidateWillCancelStream exercises every validateWillCancelStream branch
// deterministically against synthetic StreamInfo values — including the
// retention, mirror, sources, and explicit-replica-mismatch checks that a
// single-node manager bring-up cannot reach.
func TestValidateWillCancelStream(t *testing.T) {
	const domainTk = ""
	wantSubj := mqttWillCancelSubjectPrefix + domainTk + ">"
	base := func() *StreamInfo {
		return &StreamInfo{Config: StreamConfig{
			Subjects:   []string{wantSubj},
			Storage:    FileStorage,
			Retention:  LimitsPolicy,
			Discard:    DiscardOld,
			MaxMsgsPer: 1,
		}}
	}
	// Conformant with no explicit replica requirement passes.
	if err := validateWillCancelStream(base(), domainTk, 0); err != nil {
		t.Fatalf("conformant stream must validate, got %v", err)
	}
	// Conformant with a matching explicit replica requirement passes.
	si := base()
	si.Config.Replicas = 3
	if err := validateWillCancelStream(si, domainTk, 3); err != nil {
		t.Fatalf("matching replicas must validate, got %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(si *StreamInfo)
		want   int // wantReplicas passed to the validator
	}{
		{"nil", func(si *StreamInfo) {}, 0}, // replaced below
		{"wrongsubject", func(si *StreamInfo) { si.Config.Subjects = []string{"bogus.>"} }, 0},
		{"twosubjects", func(si *StreamInfo) { si.Config.Subjects = []string{wantSubj, "extra.>"} }, 0},
		{"memstorage", func(si *StreamInfo) { si.Config.Storage = MemoryStorage }, 0},
		{"interestretention", func(si *StreamInfo) { si.Config.Retention = InterestPolicy }, 0},
		{"discardnew", func(si *StreamInfo) { si.Config.Discard = DiscardNew }, 0},
		{"maxmsgsper", func(si *StreamInfo) { si.Config.MaxMsgsPer = 2 }, 0},
		{"maxmsgs", func(si *StreamInfo) { si.Config.MaxMsgs = 10 }, 0},
		{"maxbytes", func(si *StreamInfo) { si.Config.MaxBytes = 1 << 20 }, 0},
		{"maxage", func(si *StreamInfo) { si.Config.MaxAge = time.Hour }, 0},
		{"mirror", func(si *StreamInfo) { si.Config.Mirror = &StreamSource{Name: mqttSessStreamName} }, 0},
		{"sources", func(si *StreamInfo) { si.Config.Sources = []*StreamSource{{Name: mqttSessStreamName}} }, 0},
		{"replicamismatch", func(si *StreamInfo) { si.Config.Replicas = 1 }, 3},
		{"maxmsgsize", func(si *StreamInfo) { si.Config.MaxMsgSize = 1024 }, 0},
		{"noack", func(si *StreamInfo) { si.Config.NoAck = true }, 0},
		{"sealed", func(si *StreamInfo) { si.Config.Sealed = true }, 0},
		{"subjecttransform", func(si *StreamInfo) {
			si.Config.SubjectTransform = &SubjectTransformConfig{Source: "a", Destination: "b"}
		}, 0},
		{"republish", func(si *StreamInfo) { si.Config.RePublish = &RePublish{Source: "a", Destination: "b"} }, 0},
		{"allowmsgcounter", func(si *StreamInfo) { si.Config.AllowMsgCounter = true }, 0},
		{"allowrollup", func(si *StreamInfo) { si.Config.AllowRollup = true }, 0},
		{"asyncpersist", func(si *StreamInfo) { si.Config.PersistMode = AsyncPersistMode }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var si *StreamInfo
			if tc.name != "nil" {
				si = base()
				tc.mutate(si)
			}
			if err := validateWillCancelStream(si, domainTk, tc.want); err == nil {
				t.Fatalf("expected validation failure for %q", tc.name)
			}
		})
	}

	// The audited NON-prerequisites must be ACCEPTED: no operation ever deletes
	// or purges on this stream (a watermark is permanent), so a delete marker can
	// never be created either. Without these rows the audit is unfalsifiable in
	// the over-strict direction — a future copy-paste from a deleting stream's
	// validator would lock conforming deployments out.
	for _, tc := range []struct {
		name   string
		mutate func(si *StreamInfo)
	}{
		{"denydelete is not a prerequisite", func(si *StreamInfo) { si.Config.DenyDelete = true }},
		{"denypurge is not a prerequisite", func(si *StreamInfo) { si.Config.DenyPurge = true }},
		{"subjectdeletemarkerttl is not a prerequisite", func(si *StreamInfo) { si.Config.SubjectDeleteMarkerTTL = time.Minute }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			si := base()
			tc.mutate(si)
			if err := validateWillCancelStream(si, domainTk, 0); err != nil {
				t.Fatalf("%s, but it was rejected: %v", tc.name, err)
			}
		})
	}
}

// rollbackObs captures a single rollbackAcceptedSession outcome for assertions.
type rollbackObs struct {
	esCNil bool
	result rollbackResult
}

// captureRollback installs testRollbackDone on asm, returning a channel that
// receives one observation per rollback (es.c nil-ness sampled under es.mu).
func captureRollback(asm *mqttAccountSessionManager) chan rollbackObs {
	ch := make(chan rollbackObs, 4)
	asm.mu.Lock()
	asm.testRollbackDone = func(es *mqttSession, result rollbackResult) {
		es.mu.Lock()
		cNil := es.c == nil
		es.mu.Unlock()
		ch <- rollbackObs{esCNil: cNil, result: result}
	}
	asm.mu.Unlock()
	return ch
}

// failWatermarkStoreFor makes asm's advance CAS store fail for exactly one client's
// subject, leaving every other client's advance (including the test subscriber's own
// CONNECT) working. land==true performs the real store first (an ambiguous landed
// write) before returning the error.
func failWatermarkStoreFor(asm *mqttAccountSessionManager, cidHash string, land bool) {
	target := mqttWillCancelSubject(asm.domainTk, cidHash)
	setWillCancelWatermarkStoreHook(asm, func(subject string, expect uint64, wm *mqttWillCancelWatermark) (uint64, error) {
		if subject != target {
			return asm.jsa.storeWillCancelCAS(subject, expect, wm)
		}
		if land {
			_, _ = asm.jsa.storeWillCancelCAS(subject, expect, wm)
		}
		return 0, fmt.Errorf("injected watermark store failure")
	})
}

// testSessionPresent reports whether an in-memory session exists for clientID.
func testSessionPresent(asm *mqttAccountSessionManager, clientID string) bool {
	asm.mu.RLock()
	defer asm.mu.RUnlock()
	_, ok := asm.sessions[clientID]
	return ok
}

// testLoadPersisted returns the durable session record for cidHash (nil if absent).
func testLoadPersisted(t testing.TB, asm *mqttAccountSessionManager, cidHash string) *mqttPersistedSession {
	t.Helper()
	smsg, err := asm.jsa.loadSessionMsg(asm.domainTk, cidHash)
	if err != nil {
		if isErrorOtherThan(err, JSNoMessageFoundErr) {
			t.Fatalf("load persisted session: %v", err)
		}
		return nil
	}
	ps := &mqttPersistedSession{}
	if err := json.Unmarshal(smsg.Data, ps); err != nil {
		t.Fatalf("decode persisted session: %v", err)
	}
	return ps
}

// testExpectNoConnAck asserts the CONNECT was rejected: the socket closes without a
// CONNACK (a read returns an error before any packet arrives).
func testExpectNoConnAck(t *testing.T, c net.Conn, r *mqttReader) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var buf [64]byte
	if n, err := r.reader.Read(buf[:]); err == nil {
		t.Fatalf("expected the CONNECT to be rejected (no CONNACK), but %d bytes arrived", n)
	}
	_ = c.SetReadDeadline(time.Time{})
}

// awaitRollback waits for one rollback observation.
func awaitRollback(t *testing.T, ch chan rollbackObs) rollbackObs {
	t.Helper()
	select {
	case obs := <-ch:
		return obs
	case <-time.After(5 * time.Second):
		t.Fatal("rollback transaction never completed")
		return rollbackObs{}
	}
}

// TestMQTTWillOutboxWatermarkConnectFailureRollback (plan test 8-general + 8d finite
// branch): a watermark-store failure with a HEALTHY session store rejects the
// CONNECT and upholds the unconditional guarantees — no CONNACK, es.c == nil,
// in-memory session removed, result applied/superseded — then a later CONNECT
// recovers. The resolved expiry is finite, so the rollback re-saves a disconnected
// record (never a falsely-connected one).
func TestMQTTWillOutboxWatermarkConnectFailureRollback(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wm8g"
	cidHash := getHash(clientID)
	obs := captureRollback(asm)
	failWatermarkStoreFor(asm, cidHash, false)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(120)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testExpectNoConnAck(t, c, r)

	got := awaitRollback(t, obs)
	if !got.esCNil {
		t.Error("es.c must be nil after rollback")
	}
	if got.result == rollbackIndeterminate {
		t.Errorf("with a healthy session store the result must be applied/superseded, got indeterminate")
	}
	if testSessionPresent(asm, clientID) {
		t.Error("in-memory session must be removed after rollback")
	}
	if ps := testLoadPersisted(t, asm, cidHash); ps != nil && ps.DisconnectedAt == 0 {
		t.Error("finite-expiry rollback must leave a disconnected record, not a connected one")
	}

	// Recovery: clear the injection; a fresh CONNECT now succeeds and advances.
	setWillCancelWatermarkStoreHook(asm, nil)
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(120)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	if _, _, st := asm.loadWillCancelWatermark(cidHash); st != mqttWillCancelValid {
		t.Fatalf("watermark must be valid after recovery CONNECT, got %v", st)
	}
}

// TestMQTTWillOutboxWatermarkConnectFailureZeroExpiryDeletes (plan test 8d zero
// branch): a resumed CONNECT with zero session expiry that fails the watermark
// performs a CONFIRMED delete of the candidate record (it ends at connection close),
// leaving the durable record absent.
func TestMQTTWillOutboxWatermarkConnectFailureZeroExpiryDeletes(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wm8dzero"
	cidHash := getHash(clientID)
	obs := captureRollback(asm)
	failWatermarkStoreFor(asm, cidHash, false)

	// cleanStart=false (resume) with no session-expiry property => expiry 0.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: false}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testExpectNoConnAck(t, c, r)

	got := awaitRollback(t, obs)
	if !got.esCNil {
		t.Error("es.c must be nil after rollback")
	}
	if testSessionPresent(asm, clientID) {
		t.Error("in-memory session must be removed after rollback")
	}
	if ps := testLoadPersisted(t, asm, cidHash); ps != nil {
		t.Errorf("zero-expiry rollback must delete the record, still present: disconnected=%d", ps.DisconnectedAt)
	}
}

// TestMQTTWillOutboxWatermarkConnectFailureAmbiguousLanded (plan test 8b): the
// watermark store lands durably but is unconfirmable. The CONNECT is still rejected
// (no CONNACK), yet the marker is durably present (suppressing the prior Will), and a
// later CONNECT succeeds.
func TestMQTTWillOutboxWatermarkConnectFailureAmbiguousLanded(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wm8b"
	cidHash := getHash(clientID)
	obs := captureRollback(asm)

	// Load returns absent for the FIRST call (so a store is attempted), then
	// indeterminate (so neither the store loop nor watermarkAtLeast can confirm the
	// landed write). The store lands the real write, then errors.
	var loadCalls int32
	setWillCancelWatermarkLoadHook(asm, func(cid string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus) {
		if cid != cidHash {
			return nil, 0, mqttWillCancelAbsent
		}
		if atomic.AddInt32(&loadCalls, 1) == 1 {
			return nil, 0, mqttWillCancelAbsent
		}
		return nil, 0, mqttWillCancelIndeterminate
	})
	failWatermarkStoreFor(asm, cidHash, true) // land the write, then error

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(120)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testExpectNoConnAck(t, c, r)
	awaitRollback(t, obs)

	// The landed marker is durably present (clear the seams and read for real).
	setWillCancelWatermarkLoadHook(asm, nil)
	setWillCancelWatermarkStoreHook(asm, nil)
	if wm, _, st := asm.loadWillCancelWatermark(cidHash); st != mqttWillCancelValid || wm.SessionSeq == 0 {
		t.Fatalf("ambiguous write must have landed durably, got status %v seq %v", st, wmSeq(wm))
	}

	// A later CONNECT still succeeds.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(120)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
}

// TestMQTTWillOutboxWatermarkConnectFailureIndeterminate (plan test 8c): with the
// rollback persistence AND its confirming re-read both failing, the result is
// classified rollbackIndeterminate; the unconditional guarantees still hold (no
// CONNACK, es.c == nil, in-memory session removed) but the persisted record is NOT
// required to be non-connected.
func TestMQTTWillOutboxWatermarkConnectFailureIndeterminate(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wm8c"
	cidHash := getHash(clientID)
	obs := captureRollback(asm)
	failWatermarkStoreFor(asm, cidHash, false)
	setRollbackSessionStoreHook(asm, func(ps *mqttPersistedSession, idHash string, seq uint64) error {
		return fmt.Errorf("injected rollback store failure")
	})
	setRollbackSessionLoadHook(asm, func(cid string) (*mqttPersistedSession, uint64, error) {
		return nil, 0, fmt.Errorf("injected rollback load failure")
	})
	defer setRollbackSessionStoreHook(asm, nil)
	defer setRollbackSessionLoadHook(asm, nil)

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(120)}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testExpectNoConnAck(t, c, r)

	got := awaitRollback(t, obs)
	if got.result != rollbackIndeterminate {
		t.Errorf("result = %v, want rollbackIndeterminate", got.result)
	}
	if !got.esCNil {
		t.Error("es.c must be nil even on the indeterminate path")
	}
	if testSessionPresent(asm, clientID) {
		t.Error("in-memory session must be removed even on the indeterminate path")
	}
}

// TestMQTTWillOutboxWatermarkRollbackNeverExpireCancelsTimer (plan test 8d,
// finite->never transition): a rollback whose authoritative re-read reports a
// never-expire disconnected record must reconcile the expiry timer by CANCELLING a
// stale finite timer (a never-expire session does not expire), not by re-arming it.
func TestMQTTWillOutboxWatermarkRollbackNeverExpireCancelsTimer(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	cidHash := getHash("wm8dnever")
	// Pre-arm a stale finite timer as if a prior finite generation had scheduled it.
	asm.mu.Lock()
	asm.scheduleSessionExpiryLocked(s, cidHash, time.Hour)
	asm.mu.Unlock()
	if testMQTTPendingExpiryTimer(asm, cidHash) == nil {
		t.Fatal("precondition: finite timer must be armed")
	}

	setRollbackSessionLoadHook(asm, func(string) (*mqttPersistedSession, uint64, error) {
		return &mqttPersistedSession{DisconnectedAt: time.Now().Unix(), ExpiryInterval: mqttSessionNeverExpire}, 999, nil
	})
	defer setRollbackSessionLoadHook(asm, nil)
	setRollbackSessionStoreHook(asm, func(*mqttPersistedSession, string, uint64) error { return nil })
	defer setRollbackSessionStoreHook(asm, nil)

	es := &mqttSession{id: "wm8dnever", idHash: cidHash, jsa: &asm.jsa, c: &client{}}
	asm.rollbackAcceptedSession(s, es, cidHash, 998, mqttSessionNeverExpire)

	if testMQTTPendingExpiryTimer(asm, cidHash) != nil {
		t.Fatal("never-expire rollback must cancel the stale finite timer")
	}
}

// TestMQTTWillOutboxWatermarkPhase2ShutdownStopsScan (plan test 17, shutdown half):
// an ACTIVE multi-record scan stops BETWEEN records once shutdown Phase 2 signals
// the claim loops. Phase 2 (closeWillDispatch, closing willClaimStopCh) is entered
// from inside the fence while the first record is being processed; the loop-top stop
// check must then return before the remaining records are walked (marker-loaded).
func TestMQTTWillOutboxWatermarkPhase2ShutdownStopsScan(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	var loads int32
	// First record: signal Phase 2, then return a valid current-epoch marker that
	// cancels this record (a clean terminal, no worker dispatch). Any later record
	// (which must not be reached) would return absent.
	setWillCancelWatermarkLoadHook(asm, func(cid string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus) {
		if atomic.AddInt32(&loads, 1) == 1 {
			s.closeWillDispatch() // idempotent; the deferred full shutdown is a no-op
			return &mqttWillCancelWatermark{ClientIDHash: cid, SessionSeq: 999, SessEpoch: asm.sessStreamEpoch}, 1, mqttWillCancelValid
		}
		return nil, 0, mqttWillCancelAbsent
	})
	defer setWillCancelWatermarkLoadHook(asm, nil)

	const n = 4
	var subjects []string
	for i := 0; i < n; i++ {
		subj, _ := seedWillRecord(t, asm, &mqttWillOutbox{
			WillID: fmt.Sprintf("p2-w%d", i), ClientIDHash: getHash(fmt.Sprintf("p2-pub%d", i)),
			Topic: "will/p2", Msg: []byte("x"), QoS: 0,
			FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
			OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
		})
		subjects = append(subjects, subj)
	}

	asm.scanWillOutbox(s)

	// Exactly one record was processed; the loop-top willClaimStopCh check stopped the
	// scan before walking (marker-loading) the rest.
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Fatalf("scan performed %d marker loads; an active scan must stop after the first record on Phase-2 shutdown", got)
	}
	// The remaining records were never claimed (still pending).
	pendingRest := 0
	for _, subj := range subjects {
		if rec, _, err := asm.jsa.loadWillOutbox(subj); err == nil && rec != nil && rec.State == mqttWillPending {
			pendingRest++
		}
	}
	if pendingRest < n-1 {
		t.Fatalf("only %d of the trailing records remain pending; the scan walked past the stop signal", pendingRest)
	}
}

// TestMQTTWillOutboxWatermarkRealConnectAdvancesAndSuppresses (plan tests 3/6, real
// workflow): a real CONNECT (and reconnect) advances the durable marker on the
// production CONNECT path — no seam — and that real marker then suppresses an
// older-generation Will via the claimant fence.
func TestMQTTWillOutboxWatermarkRealConnectAdvancesAndSuppresses(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wmreal"
	cidHash := getHash(clientID)

	// First real CONNECT advances the marker on the production path.
	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(120)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r1)
	wm1, _, st1 := asm.loadWillCancelWatermark(cidHash)
	if st1 != mqttWillCancelValid || wm1.SessionSeq == 0 || wm1.SessEpoch != asm.sessStreamEpoch {
		t.Fatalf("CONNECT must advance a valid current-epoch marker, got status=%v wm=%+v", st1, wm1)
	}
	firstSeq := wm1.SessionSeq
	c1.Close()

	// A real reconnect advances it strictly further.
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: false, props: mqttV5ConnPropsSessionExpiry(120)}, o.MQTT.Host, o.MQTT.Port)
	defer c2.Close()
	testMQTTReadConnAckV5(t, r2)
	wm2, _, st2 := asm.loadWillCancelWatermark(cidHash)
	if st2 != mqttWillCancelValid || wm2.SessionSeq <= firstSeq {
		t.Fatalf("reconnect must advance the marker beyond %d, got status=%v seq=%v", firstSeq, st2, wmSeq(wm2))
	}

	// An older-generation Will (origin below the marker) is suppressed by the real
	// marker — no load seam involved.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmrealsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmreal", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmreal-w", ClientIDHash: cidHash, Topic: "will/wmreal", Msg: []byte("suppressed"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})
	expectWillSuppressed(t, asm, subject, rs)
}

// TestMQTTWillOutboxWatermarkSuppressesAfterRealFiniteDisconnect (plan test 3, real
// workflow): a real reconnect advances the marker to a newer generation, that newer
// generation then FINITE-disconnects (its session record persists with
// DisconnectedAt > 0), and — with no subsequent reconnect to repair anything — an
// older-generation Will is still suppressed by the surviving watermark. The numeric
// session-record fence alone would fire here (a newer-but-disconnected record fires
// like an absent one), so suppression proves the durable marker is doing the work.
func TestMQTTWillOutboxWatermarkSuppressesAfterRealFiniteDisconnect(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wmfindisc"
	cidHash := getHash(clientID)

	// Generation 1: real CONNECT with finite expiry advances the marker to S1.
	c1, r1 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r1)
	wm1, _, st1 := asm.loadWillCancelWatermark(cidHash)
	if st1 != mqttWillCancelValid || wm1.SessionSeq == 0 {
		t.Fatalf("gen1 CONNECT must advance a valid marker, got status=%v wm=%+v", st1, wm1)
	}
	s1 := wm1.SessionSeq
	c1.Close()

	// Generation 2: real reconnect (resume) advances the marker strictly beyond S1...
	c2, r2 := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: false, props: mqttV5ConnPropsSessionExpiry(300)}, o.MQTT.Host, o.MQTT.Port)
	testMQTTReadConnAckV5(t, r2)
	wm2, _, st2 := asm.loadWillCancelWatermark(cidHash)
	if st2 != mqttWillCancelValid || wm2.SessionSeq <= s1 {
		t.Fatalf("gen2 reconnect must advance the marker beyond %d, got status=%v seq=%v", s1, st2, wmSeq(wm2))
	}
	s2 := wm2.SessionSeq
	// ...then FINITE-disconnects: the session record persists with DisconnectedAt > 0.
	c2.Close()
	checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
		ps := testLoadPersisted(t, asm, cidHash)
		if ps == nil {
			return fmt.Errorf("session record must persist under finite expiry")
		}
		if ps.DisconnectedAt == 0 {
			return fmt.Errorf("session record not yet finite-disconnected")
		}
		return nil
	})

	// No reconnect repairs anything: the marker still sits at gen2's S2.
	if wm, _, st := asm.loadWillCancelWatermark(cidHash); st != mqttWillCancelValid || wm.SessionSeq != s2 {
		t.Fatalf("marker must remain at gen2 seq %d after finite disconnect, got status=%v seq=%v", s2, st, wmSeq(wm))
	}

	// An older-generation Will (origin S1 < marker S2) is suppressed, even though the
	// live session record is disconnected (which the session-record fence would fire).
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmfindiscsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmfindisc", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmfindisc-w", ClientIDHash: cidHash, Topic: "will/wmfindisc", Msg: []byte("suppressed"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: s1, OriginSessEpoch: asm.sessStreamEpoch,
	})
	expectWillSuppressed(t, asm, subject, rs)
}

// TestMQTTWillOutboxWatermarkSurvivesSessionDeletion (plan tests 4/6, real
// workflow): a real clean-start zero-expiry CONNECT/disconnect DELETES the session
// record, but the durable marker must survive that lifecycle (nothing on the
// disconnect/deletion path may remove it) and still suppress an older Will with no
// session record present — the case a lifecycle bug that deleted the watermark would
// silently break.
func TestMQTTWillOutboxWatermarkSurvivesSessionDeletion(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wmdel"
	cidHash := getHash(clientID)

	awaitSessionDeleted := func() {
		checkFor(t, 5*time.Second, 25*time.Millisecond, func() error {
			_, err := asm.jsa.loadSessionMsg(asm.domainTk, cidHash)
			if err != nil && !isErrorOtherThan(err, JSNoMessageFoundErr) {
				return nil // not-found: record deleted
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("session record still present")
		})
	}

	// Two clean-start zero-expiry CONNECT/disconnect cycles: each deletes the session
	// record and advances the durable marker (so it ends strictly above sequence 1).
	var markerSeq uint64
	for i := 0; i < 2; i++ {
		c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
		testMQTTReadConnAckV5(t, r)
		wm, _, st := asm.loadWillCancelWatermark(cidHash)
		if st != mqttWillCancelValid || wm.SessionSeq <= markerSeq {
			t.Fatalf("CONNECT %d must advance the marker beyond %d, got status=%v seq=%v", i, markerSeq, st, wmSeq(wm))
		}
		markerSeq = wm.SessionSeq
		c.Close()
		awaitSessionDeleted()
	}
	if markerSeq < 2 {
		t.Fatalf("marker must be >= 2 to test an older origin, got %d", markerSeq)
	}

	// The marker SURVIVES the session deletion, unchanged.
	wm2, _, st2 := asm.loadWillCancelWatermark(cidHash)
	if st2 != mqttWillCancelValid || wm2.SessionSeq != markerSeq {
		t.Fatalf("marker must survive session deletion unchanged, got status=%v seq=%v (want %d)", st2, wmSeq(wm2), markerSeq)
	}

	// With no session record, the surviving marker alone suppresses an older Will
	// (origin strictly below the marker).
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmdelsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmdel", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmdel-w", ClientIDHash: cidHash, Topic: "will/wmdel", Msg: []byte("suppressed"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})
	expectWillSuppressed(t, asm, subject, rs)
}

// TestMQTTWillOutboxWatermarkConnectFailurePriorWillFires (plan test 8a, scoped):
// single replica, prior Will pre-enqueued, no remote observer, no live displaced
// client; a definite watermark no-write failure ⇒ the prior Will still fires. (Not a
// general multi-replica guarantee.)
func TestMQTTWillOutboxWatermarkConnectFailurePriorWillFires(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	clientID := "wm8a"
	cidHash := getHash(clientID)
	failWatermarkStoreFor(asm, cidHash, false) // definite no-write failure

	// Subscriber for the prior Will's topic.
	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wm8asub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wm8a", opts: 0}})

	// Prior Will, due ~1.2s out so the failing CONNECT's rollback (a confirmed delete
	// of its own candidate record) completes before the record becomes due.
	seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wm8a-w", ClientIDHash: cidHash, Topic: "will/wm8a", Msg: []byte("fires"), QoS: 0,
		FireAt: time.Now().Add(1200 * time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// The failing CONNECT (clean start, zero expiry) deletes its candidate record on
	// rollback, so no watermark and no connected record suppress the prior Will.
	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: clientID, cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testExpectNoConnAck(t, c, r)

	testMQTTReadPublishV5(t, rs, "will/wm8a", []byte("fires"))
}

// TestMQTTWillOutboxWatermarkConnectFailureStaleReconcileWorker (plan test 8e): a
// finite rollback arms the expiry timer from the authoritative record and advances
// the local reconcile watermark; a stale remote-expiry worker parked on an older
// observation must then reject its snapshot and leave the rollback's timer intact.
func TestMQTTWillOutboxWatermarkConnectFailureStaleReconcileWorker(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	id := "wm8e"
	cidHash := getHash(id)
	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	// A real session record so the parked worker's authoritative load succeeds and it
	// parks at the decision seam (rather than returning early on not-found).
	planted := testMQTTPlantSessionRecord(t, js, asm.domainTk, id, &mqttPersistedSession{
		DisconnectedAt: time.Now().Unix() - 1, ExpiryInterval: 3600,
	})
	release, done := testMQTTStartParkedReconcile(t, asm, s, cidHash, planted)

	// Seam the rollback's re-read/store so the finite branch arms a timer without a
	// real CONNECT. The re-read reports a finite disconnected record at a newer seq.
	candidate := planted + 1000
	now := time.Now().Unix()
	setRollbackSessionLoadHook(asm, func(string) (*mqttPersistedSession, uint64, error) {
		return &mqttPersistedSession{DisconnectedAt: now, ExpiryInterval: 3600}, candidate + 1, nil
	})
	defer setRollbackSessionLoadHook(asm, nil)
	setRollbackSessionStoreHook(asm, func(*mqttPersistedSession, string, uint64) error { return nil })
	defer setRollbackSessionStoreHook(asm, nil)

	es := &mqttSession{id: id, idHash: cidHash, jsa: &asm.jsa, c: &client{}}
	asm.mu.Lock()
	asm.sessions[es.id] = es
	asm.sessByHash[es.idHash] = es
	asm.mu.Unlock()

	asm.rollbackAcceptedSession(s, es, cidHash, candidate, 3600)

	es.mu.Lock()
	cNil := es.c == nil
	es.mu.Unlock()
	if !cNil {
		t.Fatal("es.c must be nil after rollback")
	}
	if testMQTTPendingExpiryTimer(asm, cidHash) == nil {
		t.Fatal("finite rollback must arm the expiry timer from the authoritative record")
	}

	// Release the stale worker: its observation (planted) is now below the advanced
	// reconcile watermark, so it must reject and NOT disturb the armed timer.
	close(release)
	<-done
	if testMQTTPendingExpiryTimer(asm, cidHash) == nil {
		t.Fatal("stale reconcile worker wrongly cancelled the rollback's armed timer")
	}
}

func wmSeq(wm *mqttWillCancelWatermark) interface{} {
	if wm == nil {
		return "nil"
	}
	return wm.SessionSeq
}
func wmCid(wm *mqttWillCancelWatermark) string {
	if wm == nil {
		return "nil"
	}
	return wm.ClientIDHash
}
func wmEpoch(wm *mqttWillCancelWatermark) string {
	if wm == nil {
		return "nil"
	}
	return wm.SessEpoch
}

// expectWillSuppressed asserts the seeded outbox record at subject is cancelled
// (or cancelled-then-reaped) by the claimant fence, and never delivered.
func expectWillSuppressed(t *testing.T, asm *mqttAccountSessionManager, subject string, rs *mqttReader) {
	t.Helper()
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		rec, _, err := asm.jsa.loadWillOutbox(subject)
		if err != nil {
			return err
		}
		if rec == nil {
			return nil // cancelled then reaped
		}
		if rec.State != mqttWillCancelled {
			return fmt.Errorf("want cancelled (or reaped), got %q", rec.State)
		}
		return nil
	})
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxWatermarkSuppressesDisconnectedGeneration proves the watermark
// alone suppresses a prior generation's Will even when NO current session record
// exists (administrative loss / origin replica loss) — the residual the old
// session-record fence could not close. (Plan tests 3, 4, 6.)
func TestMQTTWillOutboxWatermarkSuppressesDisconnectedGeneration(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	pubHash := getHash("wmsup-pub")
	// A durable watermark at seq 100 (current epoch), but no session record at all:
	// the old fence would proceed (fire); the watermark must cancel.
	seedWatermark(t, asm, &mqttWillCancelWatermark{ClientIDHash: pubHash, SessionSeq: 100, SessEpoch: asm.sessStreamEpoch})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmsupsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmsup", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmsup-w", ClientIDHash: pubHash, Topic: "will/wmsup", Msg: []byte("suppressed"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch, // older than the watermark (100)
	})

	expectWillSuppressed(t, asm, subject, rs)
}

// TestMQTTWillOutboxWatermarkLegitNewerWillFires proves the replacement
// generation's OWN delayed Will (a newer origin sequence within the same epoch)
// still fires: watermark.SessionSeq is not greater than the Will's origin, so the
// fence falls back and (absent session record) proceeds. (Plan test 5.)
func TestMQTTWillOutboxWatermarkLegitNewerWillFires(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	pubHash := getHash("wmfire-pub")
	seedWatermark(t, asm, &mqttWillCancelWatermark{ClientIDHash: pubHash, SessionSeq: 100, SessEpoch: asm.sessStreamEpoch})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmfiresub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmfire", opts: 0}})

	seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmfire-w", ClientIDHash: pubHash, Topic: "will/wmfire", Msg: []byte("fire"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 101, OriginSessEpoch: asm.sessStreamEpoch, // newer than the watermark (100): its own Will
	})

	testMQTTReadPublishV5(t, rs, "will/wmfire", []byte("fire"))
}

// TestMQTTWillOutboxWatermarkCancelsForeignEpochWill proves a Will from a different
// incarnation (OriginSessEpoch != current) is suppressed by a valid current-epoch
// watermark regardless of the numeric sequences (epoch is an identity, not an
// ordering). (Plan test 13b.)
func TestMQTTWillOutboxWatermarkCancelsForeignEpochWill(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	pubHash := getHash("wmxepoch-pub")
	// Current-epoch watermark at a LOW sequence; the Will carries a foreign epoch and
	// a much HIGHER origin sequence. Numeric comparison would let it fire; epoch
	// identity must cancel it.
	seedWatermark(t, asm, &mqttWillCancelWatermark{ClientIDHash: pubHash, SessionSeq: 1, SessEpoch: asm.sessStreamEpoch})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmxepochsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmxepoch", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmxepoch-w", ClientIDHash: pubHash, Topic: "will/wmxepoch", Msg: []byte("suppressed"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 999, OriginSessEpoch: mqttGenSessEpoch(), // a different incarnation
	})

	expectWillSuppressed(t, asm, subject, rs)
}

// TestMQTTWillOutboxWatermarkMalformedEpochCharsetRejected proves the epoch
// validator rejects a body of the correct length whose characters are outside the
// NUID base-62 alphabet, and that such a Will DEFERS (never cross-incarnation
// cancels) even against a valid current-epoch watermark. Guards the length-only
// validation gap.
func TestMQTTWillOutboxWatermarkMalformedEpochCharsetRejected(t *testing.T) {
	bad := mqttSessEpochNUIDPrefix + strings.Repeat("!", 22) // 22 chars, out of alphabet
	if mqttValidSessEpoch(bad) {
		t.Fatalf("epoch %q must be invalid (out-of-alphabet body)", bad)
	}
	// Also reject an in-length body with a single stray byte, and accept a real epoch.
	if mqttValidSessEpoch(mqttSessEpochNUIDPrefix + "0123456789012345678_1") {
		t.Fatal("epoch with a stray '_' must be invalid")
	}
	if !mqttValidSessEpoch(mqttGenSessEpoch()) {
		t.Fatal("a freshly generated epoch must be valid")
	}

	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	pubHash := getHash("wmbadcs-pub")
	seedWatermark(t, asm, &mqttWillCancelWatermark{ClientIDHash: pubHash, SessionSeq: 1, SessEpoch: asm.sessStreamEpoch})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmbadcssub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmbadcs", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmbadcs-w", ClientIDHash: pubHash, Topic: "will/wmbadcs", Msg: []byte("deferred"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 5, OriginSessEpoch: bad, // length-valid but out-of-alphabet: must defer, not cancel
	})

	time.Sleep(600 * time.Millisecond)
	rec, _, err := asm.jsa.loadWillOutbox(subject)
	if err != nil || rec == nil || rec.State != mqttWillPending {
		got := "nil"
		if rec != nil {
			got = string(rec.State)
		}
		t.Fatalf("want malformed-epoch record still pending (deferred), got state=%s err=%v", got, err)
	}
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxWatermarkInvalidOriginDefers proves a Will with a zero origin
// sequence or an empty/malformed origin epoch is never compared through the
// watermark and stays pending (deferred), never fired or cancelled. (Plan test 12.)
func TestMQTTWillOutboxWatermarkInvalidOriginDefers(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	pubHash := getHash("wminv-pub")
	// A valid current-epoch watermark exists, but the record's own origin identity is
	// invalid (empty epoch), so fence step 1 defers before any comparison.
	seedWatermark(t, asm, &mqttWillCancelWatermark{ClientIDHash: pubHash, SessionSeq: 100, SessEpoch: asm.sessStreamEpoch})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wminvsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wminv", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wminv-w", ClientIDHash: pubHash, Topic: "will/wminv", Msg: []byte("deferred"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 5, OriginSessEpoch: "", // invalid origin identity
	})

	// Stays pending (deferred): neither fired nor cancelled.
	time.Sleep(600 * time.Millisecond)
	rec, _, err := asm.jsa.loadWillOutbox(subject)
	if err != nil || rec == nil || rec.State != mqttWillPending {
		got := "nil"
		if rec != nil {
			got = string(rec.State)
		}
		t.Fatalf("want invalid-origin record still pending, got state=%s err=%v", got, err)
	}
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxWatermarkCorruptMarkerDefers proves a corrupt watermark for the
// Will's own client defers only that record (never fires or cancels it) until a
// CONNECT repairs the marker. (Plan test 7, corrupt branch.)
func TestMQTTWillOutboxWatermarkCorruptMarkerDefers(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	pubHash := getHash("wmcorruptfence-pub")
	seedWatermarkRaw(t, asm, pubHash, []byte("{garbage"))

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmcorruptfencesub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmcf", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmcf-w", ClientIDHash: pubHash, Topic: "will/wmcf", Msg: []byte("later"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	// While the marker is corrupt the record stays pending; nothing is delivered.
	time.Sleep(600 * time.Millisecond)
	if rec, _, err := asm.jsa.loadWillOutbox(subject); err != nil || rec == nil || rec.State != mqttWillPending {
		got := "nil"
		if rec != nil {
			got = string(rec.State)
		}
		t.Fatalf("want record pending while marker corrupt, got state=%s err=%v", got, err)
	}
	testMQTTExpectNothing(t, rs)
}

// TestMQTTWillOutboxWatermarkIndeterminateAborts proves a transient marker-load
// failure returns willScanAbort: the record stays pending and nothing is delivered
// until the load recovers. (Plan test 7, indeterminate branch.)
func TestMQTTWillOutboxWatermarkIndeterminateAborts(t *testing.T) {
	o := testMQTTDefaultOptionsV5()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)

	pubHash := getHash("wmindet-pub")
	var loads int32
	// Fail the load ONLY for the publisher's marker; the subscriber's own CONNECT
	// advance must still see a normal (absent) load so it can connect.
	setWillCancelWatermarkLoadHook(asm, func(cidHash string) (*mqttWillCancelWatermark, uint64, mqttWillCancelStatus) {
		if cidHash != pubHash {
			return nil, 0, mqttWillCancelAbsent
		}
		atomic.AddInt32(&loads, 1)
		return nil, 0, mqttWillCancelIndeterminate
	})

	cs, rs := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "wmindetsub", cleanStart: true}, o.MQTT.Host, o.MQTT.Port)
	defer cs.Close()
	testMQTTReadConnAckV5(t, rs)
	testMQTTSubV5(t, cs, rs, 1, []mqttV5SubFilter{{topic: "will/wmindet", opts: 0}})

	subject, _ := seedWillRecord(t, asm, &mqttWillOutbox{
		WillID: "wmindet-w", ClientIDHash: pubHash, Topic: "will/wmindet", Msg: []byte("deferred"), QoS: 0,
		FireAt: time.Now().Add(-time.Millisecond).UnixNano(), State: mqttWillPending,
		OriginSessionSeq: 1, OriginSessEpoch: asm.sessStreamEpoch,
	})

	time.Sleep(600 * time.Millisecond)
	if rec, _, err := asm.jsa.loadWillOutbox(subject); err != nil || rec == nil || rec.State != mqttWillPending {
		got := "nil"
		if rec != nil {
			got = string(rec.State)
		}
		t.Fatalf("want record pending while marker load indeterminate, got state=%s err=%v", got, err)
	}
	testMQTTExpectNothing(t, rs)
	if atomic.LoadInt32(&loads) == 0 {
		t.Fatal("expected the fence to attempt the watermark load")
	}

	// Recover: the marker is absent, so the fence falls back and (no session record)
	// the deferred Will now fires — proving it was deferred, never lost.
	setWillCancelWatermarkLoadHook(asm, nil)
	testMQTTReadPublishV5(t, rs, "will/wmindet", []byte("deferred"))
}
