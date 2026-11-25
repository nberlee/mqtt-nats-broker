package mqtt

import (
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

// What the steps from an absent session record to its 0x89 disposal log (issue #190).
//
// Every route to that disposal reports one vocabulary line, so a production occurrence could
// not say which route ran or what the store answered. The steps that decide it now log their
// observations: the read that arms the ownership question, the deleted-record retirement's
// standing, and the bound probe's rejection and readback. These tests assert the CONTENT of
// those lines, because a line that names the wrong route or the wrong standing is worse than
// none.
//
// None of these tests is parallel: several install process-global hooks.

// captureLog routes this env's server log into a capture, which the client logger shares.
func (e *testRejectionEnv) captureLog() *logCapture {
	lc := &logCapture{}
	e.s.log.Store(slog.New(lc))
	return lc
}

// linesWith returns every captured line containing all of parts, in log order.
func (lc *logCapture) linesWith(parts ...string) []string {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	var out []string
	for _, line := range lc.lines {
		matched := true
		for _, p := range parts {
			if !strings.Contains(line, p) {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, line)
		}
	}
	return out
}

// indexOf is the position of the first captured line containing all of parts, or -1.
func (lc *logCapture) indexOf(parts ...string) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for i, line := range lc.lines {
		matched := true
		for _, p := range parts {
			if !strings.Contains(line, p) {
				matched = false
				break
			}
		}
		if matched {
			return i
		}
	}
	return -1
}

// plantRetiredGeneration makes this session's generation RETIRED in its authority: closed, our
// ordinal below the allocator and no longer outstanding, the state a completed retirement leaves.
func (e *testRejectionEnv) plantRetiredGeneration(t *testing.T) {
	t.Helper()
	e.sess.mu.Lock()
	id, gen := e.sess.id, e.sess.gen
	e.sess.mu.Unlock()
	al := e.asm.jsa.loadOutboundAuthority(id, e.asm.sessStreamEpoch)
	if al.status != mqttOutboundValid || al.rec == nil || al.rec.Life == nil {
		t.Fatalf("fixture: the authority must be readable, reads %v", al.status)
	}
	_, ord, _ := mqttParseGenToken(gen)
	next := *al.rec
	life := *al.rec.Life
	var kept []mqttOutstandingGen
	for _, o := range life.Outstanding {
		if o.Ord != ord {
			kept = append(kept, o)
		}
	}
	life.Outstanding = kept
	if life.NextGenOrd <= ord {
		t.Fatalf("fixture: generation ordinal %d must lie below the allocator %d", ord, life.NextGenOrd)
	}
	life.Phase, life.GenAcqs, life.Pending = mqttLifecycleClosed, false, nil
	next.State, next.Life, next.Rev, next.Txn = mqttAuthorityClosed, &life, next.Rev+1, mqttNewInflightNonce()
	body, err := json.Marshal(&next)
	if err != nil {
		t.Fatal(err)
	}
	msg, hdr := mqttFrameBatchWrite(al.seq, body, _EMPTY_, 0, false)
	if _, err := e.asm.jsa.storeMsg(mqttOutboundAuthoritySubject(getHash(id)), hdr, msg); err != nil {
		t.Fatalf("planting the retired generation: %v", err)
	}
	if standing, _ := e.asm.genStanding(id, gen); standing != mqttGenRetired {
		t.Fatalf("fixture: the generation must stand retired, stands %v", standing)
	}
}

// TestGoneArmReportsTheAbsentReadOnce: the read that arms the ownership question logs what the
// store answered, once per arm, and the retirement's refusal logs the standing it evaluated.
//
// Mutations: deleting the arm line, or reporting every re-observation of an armed marker, must
// fail this test.
func TestGoneArmReportsTheAbsentReadOnce(t *testing.T) {
	e := testNewRejectionEnv(t, "gonearmreport")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	e.sess.mu.Lock()
	seq, gen := e.sess.seq, e.sess.gen
	e.sess.mu.Unlock()
	lc := e.captureLog()

	e.deleteRecordRemotely(t)
	e.asm.reconcileResidentRecordState(e.s)
	e.asm.reconcileResidentRecordState(e.s)
	e.assertStillBound(t, "a current generation is not a licence")

	arm := lc.linesWith(`session "gonearmreport": its record read absent at the periodic ownership check`)
	if len(arm) != 1 {
		t.Fatalf("the arming read must be reported exactly once across two passes, got %d: %q", len(arm), lc.lines)
	}
	for _, want := range []string{"acknowledged sequence " + strconv.FormatUint(seq, 10), `generation "` + gen + `"`, "no message found"} {
		if !strings.Contains(arm[0], want) {
			t.Fatalf("the arm line must carry %q: %q", want, arm[0])
		}
	}
	stays := lc.linesWith(`session "gonearmreport" stays bound`)
	if len(stays) != 1 {
		t.Fatalf("the refusal on standing must be reported once per arm, got %d: %q", len(stays), lc.lines)
	}
	if !strings.Contains(stays[0], `generation "`+gen+`" stands current`) {
		t.Fatalf("the refusal must name the standing it evaluated: %q", stays[0])
	}
}

// TestGoneArmReportsAnUnresolvedAuthority: a standing that could not be evaluated is reported as
// such, never as current.
//
// Mutation: a constant standing on the refusal line must fail this test.
func TestGoneArmReportsAnUnresolvedAuthority(t *testing.T) {
	e := testNewRejectionEnv(t, "gonearmunresolved")
	e.dropPersistObservations(t, e.idHash)
	e.bind(t)
	lc := e.captureLog()

	e.deleteRecordRemotely(t)
	authSubj := mqttOutboundAuthoritySubject(e.sess.idHash)
	restore := setMQTTTestLoadLastMsgHook(func(_, subject string) error {
		if subject == authSubj {
			return errors.New("injected: the authority read is indeterminate")
		}
		return nil
	})
	defer restore()

	e.asm.reconcileResidentRecordState(e.s)
	e.assertStillBound(t, "an unreadable authority is never a licence")
	if got := lc.linesWith(`session "gonearmunresolved" stays bound`, "stands unresolved"); len(got) != 1 {
		t.Fatalf("the refusal must report the standing as unresolved: %q", lc.lines)
	}
}

// TestGoneRetirementReportsItsLicence: the licensed retirement logs the standing its IN-FENCE
// proof evaluated, the sequence and the absence read, ahead of the 0x89 line.
//
// Mutations: a constant standing, the prefilter's standing in place of the proof's, or a line
// without the sequence must each fail one of the sub-tests.
func TestGoneRetirementReportsItsLicence(t *testing.T) {
	cases := []struct {
		name, clientID string
		plant          func(e *testRejectionEnv, t *testing.T)
		inFence        func(e *testRejectionEnv, t *testing.T)
		want           string
	}{
		{name: "outstanding", clientID: "goneretireout",
			plant: func(e *testRejectionEnv, t *testing.T) { e.plantGoneLicence(t, false) }, want: "outstanding"},
		{name: "retired", clientID: "goneretireret",
			plant: func(e *testRejectionEnv, t *testing.T) { e.plantRetiredGeneration(t) }, want: "retired"},
		{name: "retired inside the fence", clientID: "goneretirefence",
			plant:   func(e *testRejectionEnv, t *testing.T) { e.plantGoneLicence(t, false) },
			inFence: func(e *testRejectionEnv, t *testing.T) { e.plantRetiredGeneration(t) }, want: "retired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, tc.clientID)
			e.dropPersistObservations(t, e.idHash)
			e.bindSocket(t, mqttProtoLevel5)
			e.sess.mu.Lock()
			seq, gen := e.sess.seq, e.sess.gen
			e.sess.mu.Unlock()
			lc := e.captureLog()

			e.deleteRecordRemotely(t)
			tc.plant(e, t)
			if tc.inFence != nil {
				var fired bool
				h := func(*mqttSession) {
					if !fired {
						fired = true
						tc.inFence(e, t)
					}
				}
				mqttTestGoneRetireProofEntryHookV.Store(&h)
				defer mqttTestGoneRetireProofEntryHookV.Store(nil)
			}

			e.asm.reconcileResidentRecordState(e.s)
			if e.resident(e.sess) {
				t.Fatal("a licensed bound session whose record is gone must be retired by the pass")
			}

			line := `deleted-record retirement: session "` + tc.clientID + `" is retired`
			got := lc.linesWith(line)
			if len(got) != 1 {
				t.Fatalf("the retirement must be reported once, got %d: %q", len(got), lc.lines)
			}
			for _, want := range []string{`generation "` + gen + `" stands ` + tc.want,
				"record read absent at sequence " + strconv.FormatUint(seq, 10), "no message found"} {
				if !strings.Contains(got[0], want) {
					t.Fatalf("the retirement line must carry %q: %q", want, got[0])
				}
			}
			disposal := lc.indexOf(`session "` + tc.clientID + `" could not be shown to be owned by anyone`)
			if disposal < 0 || lc.indexOf(line) > disposal {
				t.Fatalf("the retirement line must precede the disposal line: %q", lc.lines)
			}
		})
	}
}

// TestBoundProbeDisposalReportsItsEvidence: the bound probe that disposes logs the packet that
// drove it, the base its CAS named, the stream's rejection and the readback, ahead of the 0x89
// line.
//
// Mutations: dropping the CAS rejection from the line, or naming every packet PINGREQ, must fail
// this test.
func TestBoundProbeDisposalReportsItsEvidence(t *testing.T) {
	cases := []struct {
		name, clientID, packet string
		buf                    []byte
	}{
		{name: "PINGREQ", clientID: "goneprobeping", packet: "PINGREQ", buf: []byte{mqttPacketPing, 0}},
		{name: "PUBLISH", clientID: "goneprobepub", packet: "PUBLISH",
			buf: testBoundPublish(true, 0, 0, "gone/probe", "x")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := testNewRejectionEnv(t, tc.clientID)
			e.dropPersistObservations(t, e.idHash)
			b := e.bindSocket(t, mqttProtoLevel5)
			e.sess.mu.Lock()
			seq := e.sess.seq
			e.sess.mu.Unlock()
			lc := e.captureLog()

			e.deleteRecordRemotely(t)
			e.asm.reconcileResidentRecordState(e.s)
			e.assertStillBound(t, "a current generation is not a licence")

			if err := b.deliver(t, tc.buf); err != nil {
				t.Fatalf("a disposing probe returns no error: %v", err)
			}
			if !b.closed() || !b.sawDisconnect(t, mqttReasonServerBusy) {
				t.Fatalf("the probe must dispose with 0x89: closed=%v wire %x", b.closed(), b.out.bytes())
			}

			line := `session "` + tc.clientID + `": the record probe before a ` + tc.packet + ` packet`
			got := lc.linesWith(line)
			if len(got) != 1 {
				t.Fatalf("the probe disposal must be reported once, got %d: %q", len(got), lc.lines)
			}
			for _, want := range []string{"was rejected at sequence " + strconv.FormatUint(seq, 10), "wrong last sequence",
				"the record then read absent", "no message found"} {
				if !strings.Contains(got[0], want) {
					t.Fatalf("the probe line must carry %q: %q", want, got[0])
				}
			}
			disposal := lc.indexOf(`session "` + tc.clientID + `" could not be shown to be owned by anyone`)
			if disposal < 0 || lc.indexOf(line) > disposal {
				t.Fatalf("the probe line must precede the disposal line: %q", lc.lines)
			}
		})
	}
}

// TestPacketTypeNameNamesTheGatedPackets pins the names the probe line uses.
func TestPacketTypeNameNamesTheGatedPackets(t *testing.T) {
	for pt, want := range map[byte]string{
		mqttPacketPub | 0x02: "PUBLISH", mqttPacketPubAck: "PUBACK", mqttPacketPubRec: "PUBREC",
		mqttPacketPubRel | 0x02: "PUBREL", mqttPacketPubComp: "PUBCOMP", mqttPacketSub | 0x02: "SUBSCRIBE",
		mqttPacketUnsub | 0x02: "UNSUBSCRIBE", mqttPacketPing: "PINGREQ", mqttPacketAuth: "AUTH",
		mqttPacketSubAck: "type 0x90",
	} {
		if got := mqttPacketTypeName(pt); got != want {
			t.Fatalf("mqttPacketTypeName(0x%02x) = %q, want %q", pt, got, want)
		}
	}
}
