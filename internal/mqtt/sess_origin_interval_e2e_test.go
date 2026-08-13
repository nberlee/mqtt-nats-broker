//go:build mqttport_fulltests

package mqtt

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// End-to-end cover for the interval register over real sockets: a real CONNECT whose
// client ID has NO history at all, a real record that forks under it, and the
// vocabulary the displaced client is actually told.
//
// This is the acceptance test for the item. Before the register existed, the create
// below was parentless, the sibling that erased it was unrecognisable, and the client
// was told 0x8E "session taken over" — which instructs it to stop reconnecting — with
// its delayed Will cancelled on the way out.

// testE2EOriginToken returns the exact identity a reader would derive from the
// register standing now.
func testE2EOriginToken(t *testing.T, asm *mqttAccountSessionManager, idHash string) string {
	t.Helper()
	_, seq, status := asm.jsa.loadOriginIntervalRecord(asm.domainTk, idHash)
	if status != mqttOriginValid {
		t.Fatalf("expected a valid interval register, got status %v", status)
	}
	tok := mqttOriginToken(asm.originStreamEpoch, seq)
	if tok == _EMPTY_ {
		t.Fatalf("the register at sequence %v yielded no identity (epoch %q)", seq, asm.originStreamEpoch)
	}
	return tok
}

// TestMQTTOriginFirstEverCreateE2EAnswers0x89 is test 2, the item's acceptance test.
//
// A client ID with no record, no retirement watermark and no history of any kind
// connects, and a sibling of its create lands. Before this item the create was
// parentless, so classifyRecordLineage could not see the pair — two unrelated first
// creates must not read as siblings — and the erased side was told 0x8E "session taken
// over", which instructs a client to stop reconnecting. It must now be told 0x89.
//
// The Will assertion at the end is deliberately scoped: this connection carries an
// IMMEDIATE Will, and an immediate Will was measured to fire on BOTH disposal routes, so
// its publication demonstrates that the 0x89 route runs the ordinary close bookkeeping —
// not that the two routes differ. The difference between the routes is the vocabulary,
// and that is asserted against its opposite in TestMQTTOriginDisposalDirectionsE2E.
func TestMQTTOriginFirstEverCreateE2EAnswers0x89(t *testing.T) {
	t.Parallel()
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)

	wc, wr := testMQTTConnect(t, &mqttConnInfo{clientID: "originwatch", cleanSess: true}, o.MQTT.Host, o.MQTT.Port)
	defer wc.Close()
	testMQTTCheckConnAck(t, wr, mqttConnAckRCConnectionAccepted, false)
	testMQTTSub(t, 1, wc, wr, []*mqttFilter{{filter: "origine2e/will", qos: 0}}, []byte{0})
	testMQTTFlush(t, wc, nil, wr)

	asm := testMQTTGetGlobalASM(t, s)
	idHash := getHash("origine2e")
	// The premise: nothing exists for this client ID. Both are asserted rather than
	// assumed, because a fixture that accidentally had a watermark would be testing
	// the RWID arm and would pass without the register existing at all.
	if _, err := asm.jsa.loadSessionMsg(asm.domainTk, idHash); err == nil {
		t.Fatal("the fixture requires a client ID with NO session record")
	}
	if _, _, st := asm.jsa.loadGenRetirementRecord(asm.domainTk, idHash); st != mqttRetirementAbsent {
		t.Fatalf("the fixture requires NO retirement watermark, got status %v", st)
	}
	if _, _, st := asm.jsa.loadOriginIntervalRecord(asm.domainTk, idHash); st != mqttOriginAbsent {
		t.Fatalf("the fixture requires a genuine first-ever interval, got register status %v", st)
	}

	ci := &mqttV5ConnInfo{
		clientID: "origine2e",
		will:     &mqttWill{topic: []byte("origine2e/will"), message: []byte("bye")},
	}
	c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttConnAckRCConnectionAccepted {
		t.Fatalf("CONNACK reason %x", reason)
	}

	sess := testMQTTGetClient(t, s, "origine2e").mqtt.sess
	if sess == nil {
		t.Fatal("the connection must be bound to a session")
	}
	// Read BEFORE any second save. The accept save is the CREATE, and its parent is
	// the interval identity; a later save would parent itself to the create's own WID
	// and the assertion would stop being about this item at all.
	sess.mu.Lock()
	ackWID, ackPWID := sess.wid, sess.pwid
	sess.mu.Unlock()
	if ackWID == _EMPTY_ {
		t.Fatal("the accept save must be acknowledged")
	}
	want := testE2EOriginToken(t, asm, idHash)
	if ackPWID != want {
		t.Fatalf("a first-ever create must descend from the INTERVAL identity: parent=%q want %q. "+
			"Before this item it was empty, and a parentless create's erased sibling is invisible to "+
			"classifyRecordLineage", ackPWID, want)
	}

	// A sibling of the CREATE, written as another broker's record: the session-record
	// watch filters this broker's own writes out of the eviction route on Origin, so a
	// locally-authored sibling would never spawn the worker under test. No intervening
	// save, so the sibling really is a sibling of the create and its recognition rests
	// entirely on the identity the register supplied.
	sess.mu.Lock()
	sibling := sess.buildPersistedSessionLocked()
	sibling.Gen, sibling.Origin = sess.gen, "othernode"
	sibling.WID, sibling.PWID = mqttNewRecordWID(), ackPWID
	sibling.Subs = map[string]byte{"origine2e/sibling": 1}
	domainTk := sess.domainTk
	sess.mu.Unlock()
	body, merr := json.Marshal(&sibling)
	if merr != nil {
		t.Fatalf("marshal the sibling: %v", merr)
	}
	if _, serr := sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); serr != nil {
		t.Fatalf("land the sibling record: %v", serr)
	}

	b, pl := testMQTTReadPacket(t, r)
	if pt := b & mqttPacketMask; pt != mqttPacketDisconnect {
		t.Fatalf("expected a server DISCONNECT, got %x (pl=%d)", pt, pl)
	}
	reason, err := r.readByte("disconnect reason")
	if err != nil {
		t.Fatalf("read the DISCONNECT reason: %v", err)
	}
	if reason != mqttReasonServerBusy {
		t.Fatalf("a create whose sibling erased it answers 0x89 'try again', never 0x8E 'session taken "+
			"over' — the two creates arbitrated nothing. Got %x", reason)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [256]byte
	n, rerr := c.Read(buf[:])
	if n > 0 {
		t.Fatalf("nothing may follow the DISCONNECT: got %x", buf[:n])
	}
	var nerr net.Error
	if rerr == nil || (errors.As(rerr, &nerr) && nerr.Timeout()) {
		t.Fatalf("the connection must be CLOSED after the 0x89, not merely idle: %v", rerr)
	}

	// The ordinary close bookkeeping still runs on the 0x89 route: the client's Will is
	// published. Not a discriminator between the routes — see the comment above — but a
	// real property of this one, and it fails if the fork verdict starts swallowing the
	// close path.
	testMQTTCheckPubMsg(t, wc, wr, "origine2e/will", 0, []byte("bye"))
}

// TestMQTTOriginDisposalDirectionsE2E is test 15's PRODUCTION half, and it asserts BOTH
// directions of the accepted R0a residual over real sockets.
//
// R0a in docs/CONFORMANCE.md accepts, by choice, that a late or duplicate finisher splits
// one interval into two identities: the create before the second rotation holds `O1`, the
// create after it holds `O2`, and neither recognises the other. What that costs is a
// disposal, so the disposal is what is asserted here rather than a classifier verdict —
// and it is asserted against its opposite, because either row alone passes against a
// broker that collapsed the two dispositions into one.
//
//   - a record parented to the SAME identity is a proven sibling: 0x89 "server busy, try
//     again", which invites the reconnect 0x8E forbids.
//   - one parented to a DIFFERENT identity demonstrates no fork: 0x8E "session taken
//     over". That is exactly the behavior R0 had before this item, which is what makes it
//     a stated residual rather than a defect.
//
// It asserts the VOCABULARY and nothing about the Will, deliberately. The Will difference
// the two dispositions carry is not observable at THIS close in either direction: an
// immediate Will fires on both routes, and no pending delayed-Will outbox record survives
// either route here, because the competing record has already replaced the base this
// session would enqueue against. Both were measured rather than assumed, and a test
// asserting a difference it cannot demonstrate is worse than one that does not claim it.
// The connection carries a delayed Will all the same, so neither row is a Will-free path.
func TestMQTTOriginDisposalDirectionsE2E(t *testing.T) {
	for _, tc := range []struct {
		name string
		// rotate: move the register on before the competing record is written, so the
		// two records carry identities from different intervals.
		rotate     bool
		wantReason byte
	}{
		{"same interval is a proven sibling", false, mqttReasonServerBusy},
		{"straddling a second rotation concedes", true, mqttReasonSessionTakenOver},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)

			asm := testMQTTGetGlobalASM(t, s)
			cid := "disposal-" + map[bool]string{true: "straddle", false: "same"}[tc.rotate]
			idHash := getHash(cid)

			ci := &mqttV5ConnInfo{
				clientID:  cid,
				will:      &mqttWill{topic: []byte("disposal/will"), message: []byte("bye")},
				willProps: mqttV5WillDelayProps(30),
				props:     mqttV5ConnPropsSessionExpiry(60),
			}
			c, r := testMQTTConnectV5(t, ci, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			if _, reason, _ := testMQTTReadConnAckV5(t, r); reason != mqttConnAckRCConnectionAccepted {
				t.Fatalf("CONNACK reason %x", reason)
			}
			sess := testMQTTGetClient(t, s, cid).mqtt.sess
			if sess == nil {
				t.Fatal("the connection must be bound to a session")
			}
			sess.mu.Lock()
			ackPWID := sess.pwid
			sess.mu.Unlock()
			o1 := testE2EOriginToken(t, asm, idHash)
			if ackPWID != o1 {
				t.Fatalf("the create must hold the identity standing at its own read: %q want %q",
					ackPWID, o1)
			}

			parent := o1
			if tc.rotate {
				// The late finisher's second rotation, after the release: the register
				// moves while this connection is still acknowledged for o1.
				if _, err := asm.jsa.storeOriginInterval(mqttOriginSubject(asm.domainTk, idHash), false,
					&mqttOriginInterval{ID: cid, Origin: "late-finisher"}); err != nil {
					t.Fatalf("the second rotation: %v", err)
				}
				parent = testE2EOriginToken(t, asm, idHash)
				if parent == o1 {
					t.Fatalf("the fixture needs the rotation to have moved the identity, still %q", o1)
				}
			}

			// The competing create's record, written as another broker's so the
			// session-record watch does not filter it as this broker's own write.
			sess.mu.Lock()
			other := sess.buildPersistedSessionLocked()
			other.Gen, other.Origin = sess.gen, "othernode"
			other.WID, other.PWID = mqttNewRecordWID(), parent
			other.Subs = map[string]byte{"disposal/other": 1}
			domainTk := sess.domainTk
			sess.mu.Unlock()
			body, merr := json.Marshal(&other)
			if merr != nil {
				t.Fatalf("marshal the competing record: %v", merr)
			}
			if _, serr := sess.jsa.storeSessionMsg(domainTk, idHash, 0, body); serr != nil {
				t.Fatalf("land the competing record: %v", serr)
			}

			b, pl := testMQTTReadPacket(t, r)
			if pt := b & mqttPacketMask; pt != mqttPacketDisconnect {
				t.Fatalf("expected a server DISCONNECT, got %x (pl=%d)", pt, pl)
			}
			reason, err := r.readByte("disconnect reason")
			if err != nil {
				t.Fatalf("read the DISCONNECT reason: %v", err)
			}
			if reason != tc.wantReason {
				t.Fatalf("disconnect reason %x, want %x — a record carrying the SAME identity is a "+
					"proven sibling (0x89 'try again'), and one carrying a different interval's "+
					"identity demonstrates no fork (0x8E 'session taken over')", reason, tc.wantReason)
			}
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			var buf [256]byte
			n, rerr := c.Read(buf[:])
			if n > 0 {
				t.Fatalf("nothing may follow the DISCONNECT: got %x", buf[:n])
			}
			var nerr net.Error
			if rerr == nil || (errors.As(rerr, &nerr) && nerr.Timeout()) {
				t.Fatalf("the connection must be CLOSED after the disconnect, not merely idle: %v", rerr)
			}

		})
	}
}

// TestMQTTOriginEstablishFailureE2ENeverFailsTheConnect is test 18 (M12, M23).
//
// The register is a recognition aid, not a prerequisite of a session. Every failure on
// that path concedes to today's behaviour — a parentless create — and NOTHING here may
// fail a CONNECT. Two fixtures, because they mutate differently: a store that failed
// outright, and a rejection followed by an absent re-read for the whole retry bound
// (the arm whose concession a mutation would most plausibly turn into an error).
func TestMQTTOriginEstablishFailureE2ENeverFailsTheConnect(t *testing.T) {
	for _, tc := range []struct {
		name        string
		clientID    string
		storeErr    error
		wantStores  int
		forceAbsent bool
	}{
		{
			name:       "the establish store fails",
			clientID:   "originfail-store",
			storeErr:   errors.New("mqtt test: register establish forced to fail"),
			wantStores: 1,
		},
		{
			name:        "rejected, then absent for the whole retry bound",
			clientID:    "originfail-bound",
			storeErr:    ApiErrors[JSStreamWrongLastSequenceConstantErr],
			wantStores:  mqttOriginAttempts,
			forceAbsent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testMQTTDefaultOptions()
			s := testMQTTRunServer(t, o)
			defer testMQTTShutdownServer(s)
			asm := testMQTTGetGlobalASM(t, s)
			idHash := getHash(tc.clientID)

			var mu sync.Mutex
			stores := 0
			if tc.forceAbsent {
				restore := setMQTTTestOriginLoadHook(func(h string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
					if h != idHash {
						return asm.jsa.loadOriginIntervalRecord(asm.domainTk, h)
					}
					return nil, 0, mqttOriginAbsent
				})
				defer restore()
			}
			restore := setMQTTTestOriginStoreHook(func(subject string, cas bool, r *mqttOriginInterval) (uint64, error) {
				if subject != mqttOriginSubject(asm.domainTk, idHash) {
					return asm.jsa.storeOriginInterval(subject, cas, r)
				}
				mu.Lock()
				stores++
				mu.Unlock()
				return 0, tc.storeErr
			})
			defer restore()

			c, r := testMQTTConnect(t, &mqttConnInfo{clientID: tc.clientID}, o.MQTT.Host, o.MQTT.Port)
			defer c.Close()
			// The whole assertion: ACCEPTED. A helper-level concession can still be
			// turned into a failed CONNECT by its caller, which is what this covers.
			testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)

			sess := testMQTTGetClient(t, s, tc.clientID).mqtt.sess
			if sess == nil {
				t.Fatal("the connection must be bound to a session")
			}
			sess.mu.Lock()
			pwid := sess.pwid
			sess.mu.Unlock()
			if pwid != _EMPTY_ {
				t.Errorf("a create that could not establish an identity must be PARENTLESS, got %q", pwid)
			}
			mu.Lock()
			got := stores
			mu.Unlock()
			if got != tc.wantStores {
				t.Errorf("establish attempts = %d, want %d", got, tc.wantStores)
			}
		})
	}
}

// TestMQTTOriginReDriveRederivesTheIdentity is test 20's re-drive half (M29).
//
// A create whose base-0 record CAS is DEFINITELY rejected (10071) goes back to the
// authoritative restore: the session object is detached and discarded, and the next
// pass derives its identity from a fresh register read. Nothing may carry the first
// pass's identity forward, because between the two passes the interval can have moved
// — which is exactly what this fixture does.
// Deliberately NOT parallel: it sets mqttTestSessStoreSuppressV, which is process-wide,
// so a parallel neighbour's own record write would be suppressed too. That is the
// convention every other user of that flag follows (see
// TestMQTTForkedRecordE2EConnectRebuildsFromRecord).
func TestMQTTOriginReDriveRederivesTheIdentity(t *testing.T) {
	o := testMQTTDefaultOptions()
	s := testMQTTRunServer(t, o)
	defer testMQTTShutdownServer(s)
	asm := testMQTTGetGlobalASM(t, s)
	const clientID = "origin-redrive"
	idHash := getHash(clientID)

	// The register must already STAND, so the first pass READS an identity rather than
	// establishing one: an establishing pass would take its identity from its own
	// confirmed write and there would be no earlier value for a re-drive to carry.
	if _, err := asm.jsa.storeOriginInterval(mqttOriginSubject(asm.domainTk, idHash), true,
		&mqttOriginInterval{ID: clientID, Origin: "pre-existing-interval"}); err != nil {
		t.Fatalf("planting the standing register: %v", err)
	}

	var first atomic.Value // the identity the FIRST pass derived
	var rotated atomic.Bool
	var rejects atomic.Int32

	// Observe what each pass derives, and rotate the register between the two.
	restoreLoad := setMQTTTestOriginLoadHook(func(h string) (*mqttOriginInterval, uint64, mqttOriginStatus) {
		r, seq, status := asm.jsa.loadOriginIntervalRecord(asm.domainTk, h)
		if h == idHash && status == mqttOriginValid && first.Load() == nil {
			first.Store(mqttOriginToken(asm.originStreamEpoch, seq))
		}
		return r, seq, status
	})
	defer restoreLoad()

	// The FIRST accept save is rejected 10071 with nothing stored, which is the T2
	// re-drive's trigger. Everything the first pass needs happens inside this one call,
	// synchronously, because the re-drive issues its second save immediately: the
	// register is rotated, and the suppression that keeps the first write from LANDING
	// is lifted here rather than from the test goroutine, which would race it.
	restoreSave := setMQTTTestSessStoreResultHook(func(sess *mqttSession, resp *JSPubAckResponse, err error) (*JSPubAckResponse, error) {
		if sess == nil || sess.idHash != idHash || rejects.Add(1) > 1 {
			return resp, err
		}
		if _, serr := asm.jsa.storeOriginInterval(mqttOriginSubject(asm.domainTk, idHash), false,
			&mqttOriginInterval{ID: clientID, Origin: "rotated-between-passes"}); serr != nil {
			t.Errorf("rotating the register between passes: %v", serr)
		}
		rotated.Store(true)
		mqttTestSessStoreSuppressV.Store(false)
		return nil, ApiErrors[JSStreamWrongLastSequenceErrF]
	})
	defer restoreSave()
	mqttTestSessStoreSuppressV.Store(true)
	t.Cleanup(func() { mqttTestSessStoreSuppressV.Store(false) })

	c, r := testMQTTConnect(t, &mqttConnInfo{clientID: clientID}, o.MQTT.Host, o.MQTT.Port)
	defer c.Close()
	testMQTTCheckConnAck(t, r, mqttConnAckRCConnectionAccepted, false)
	if rejects.Load() < 1 {
		t.Fatal("the fixture never rejected a save, so no re-drive happened")
	}
	if !rotated.Load() {
		t.Fatal("the fixture never rotated the register between passes")
	}

	firstTok, _ := first.Load().(string)
	secondTok := testE2EOriginToken(t, asm, idHash)
	if firstTok == _EMPTY_ || firstTok == secondTok {
		t.Fatalf("the fixture needs the two passes to see DIFFERENT identities, got %q and %q",
			firstTok, secondTok)
	}
	sess := testMQTTGetClient(t, s, clientID).mqtt.sess
	if sess == nil {
		t.Fatal("the connection must be bound to a session")
	}
	sess.mu.Lock()
	pwid := sess.pwid
	sess.mu.Unlock()
	if pwid == firstTok {
		t.Fatalf("the re-drive carried the DISCARDED pass's identity %q; it must re-derive from the "+
			"register, which now holds %q", firstTok, secondTok)
	}
	if pwid != secondTok {
		t.Fatalf("the re-driven create must descend from the identity standing NOW: parent=%q want %q",
			pwid, secondTok)
	}
}
