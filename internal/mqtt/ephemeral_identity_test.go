package mqtt

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// testEphemeralServer builds a bare Server sharing the harness's NATS connection, so
// several "brokers" can initialize against one account — which is what every identity
// assertion here is about.
func testEphemeralServer(t *testing.T, e *testRejectionEnv, node string) *Server {
	t.Helper()
	srv := testRigServer(t, e.s.nrc, node)
	return srv
}

// testEphemeralInit drives one initialization ATTEMPT on srv, clearing the manager map
// first so a previous success does not short-circuit it. That is the same-run retry
// the derived identity exists to bound.
func testEphemeralInit(srv *Server) (*mqttAccountSessionManager, error) {
	srv.mqtt.sessmgr.mu.Lock()
	srv.mqtt.sessmgr.sessions = make(map[string]*mqttAccountSessionManager)
	srv.mqtt.sessmgr.mu.Unlock()
	return srv.getOrCreateMQTTAccountSessionManager(srv.createInternalAccountClient())
}

// testEphemeralRecorder records every create REQUEST as submitted and every delete
// ATTEMPT (a hook invocation, which is not the same as a JetStream request — with
// send=false none is sent).
type testEphemeralRecorder struct {
	mu      sync.Mutex
	creates []*CreateConsumerRequest
	deletes []string
}

func (r *testEphemeralRecorder) requestsFor(name string) []*CreateConsumerRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*CreateConsumerRequest
	for _, ccr := range r.creates {
		if ccr.Config.Name == name {
			out = append(out, ccr)
		}
	}
	return out
}

func (r *testEphemeralRecorder) createdNames() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.creates))
	for _, ccr := range r.creates {
		out = append(out, ccr.Config.Name)
	}
	return out
}

func testConsumerNames(t *testing.T, jsa *mqttJSA, stream string) []string {
	t.Helper()
	cis, err := jsa.consumerList(stream)
	if err != nil {
		t.Fatalf("list %s consumers: %v", stream, err)
	}
	out := make([]string, 0, len(cis))
	for _, ci := range cis {
		if ci != nil {
			out = append(out, ci.Name)
		}
	}
	return out
}

func testCountName(names []string, want string) int {
	n := 0
	for _, got := range names {
		if got == want {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// T1: the identity is stable ACROSS ATTEMPTS of one server run
// ---------------------------------------------------------------------------

// TestMQTTEphemeralConsumerIdentityIsStableAcrossInitAttempts asserts on the requests
// this run SUBMITS, not on what survives on the stream. A rollback that tidied up
// after each attempt would hide a per-attempt name behind a final count of one, so
// the count that matters is the number of DISTINCT names asked for.
//
// Both purposes are required to appear at least twice, on one Server: a purpose
// observed once cannot discriminate a name that is freshly minted every attempt.
func TestMQTTEphemeralConsumerIdentityIsStableAcrossInitAttempts(t *testing.T) {
	e := testNewWatchEnv(t, "ephid")
	rec := &testEphemeralRecorder{}

	srv := testEphemeralServer(t, e, "ephid")
	id, _ := srv.ephemeralIdentity()
	obsName, watchName := mqttRetainedObserverName(id), mqttSessRecordWatchName(id)

	// The first two attempts fail at the watch, and they must fail with the request
	// ISSUED: setMQTTTestWatchConsumerCreateFail short-circuits before the create, so
	// it would leave the watch observed only once — and one observation of a name
	// cannot tell a derived name from a freshly minted one.
	var watchFails int
	var mu sync.Mutex
	defer setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		rec.mu.Lock()
		cp := *ccr
		rec.creates = append(rec.creates, &cp)
		rec.mu.Unlock()
		if ccr.Stream == mqttSessStreamName {
			mu.Lock()
			defer mu.Unlock()
			if watchFails < 2 {
				watchFails++
				return errMQTTTestLostResponse
			}
		}
		return err
	})()
	defer setMQTTTestConsumerDeleteHook(func(_, name string, _ int) (bool, error, bool) {
		rec.mu.Lock()
		rec.deletes = append(rec.deletes, name)
		rec.mu.Unlock()
		return true, nil, false
	})()

	for i := 0; i < 2; i++ {
		if _, err := testEphemeralInit(srv); err == nil {
			t.Fatal("fixture: initialization must fail while the watch create's response is lost")
		}
	}
	if _, err := testEphemeralInit(srv); err != nil {
		t.Fatalf("initialization must succeed once nothing is forced to fail: %v", err)
	}

	// The watch's delivery subject is derived and stable. The observer's is derived from
	// the same identity but per GENERATION, and a generation is never reused within a run —
	// across initialization attempts included — because the subscription is the only thing
	// that tells a stale delivery from a current one (retained_index.go).
	obsPrefix := mqttSubPrefix + id + ".rmsgs."
	for _, tc := range []struct {
		purpose, name, subject string
	}{
		{"retained observer", obsName, ""},
		{"session-record watch", watchName, mqttSessRecordWatchDeliverSubject(id)},
	} {
		reqs := rec.requestsFor(tc.name)
		if len(reqs) < 2 {
			t.Fatalf("%s: the run must have asked for this name at least twice to prove stability, got %d of %v",
				tc.purpose, len(reqs), rec.createdNames())
		}
		seen := map[string]bool{}
		for _, ccr := range reqs {
			ds := ccr.Config.DeliverSubject
			if tc.subject == "" {
				if !strings.HasPrefix(ds, obsPrefix) || len(ds) == len(obsPrefix) {
					t.Fatalf("%s: the delivery subject must be derived from the identity plus a generation, got %q",
						tc.purpose, ds)
				}
				if seen[ds] {
					t.Fatalf("%s: delivery subject %q was reused by a second attempt", tc.purpose, ds)
				}
				seen[ds] = true
			} else if ds != tc.subject {
				t.Fatalf("%s: the delivery subject must be derived too, got %q want %q",
					tc.purpose, ds, tc.subject)
			}
			if ccr.Action != ActionCreate {
				t.Fatalf("%s: the create must be ActionCreate so an unexpected occupant is refused, got %v",
					tc.purpose, ccr.Action)
			}
		}
	}

	// The two purposes must not share a delivery subject: one subscription callback
	// per purpose is what keeps a retained record out of the watch's decoder.
	if strings.HasPrefix(mqttSessRecordWatchDeliverSubject(id), obsPrefix) {
		t.Fatal("the two purposes must derive DIFFERENT delivery subjects from the same identity")
	}

	// And exactly one consumer per purpose survives.
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttRetainedMsgsStreamName), obsName); got != 1 {
		t.Fatalf("the retained observer must end at exactly one consumer, got %d", got)
	}
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttSessStreamName), watchName); got != 1 {
		t.Fatalf("the session-record watch must end at exactly one consumer, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// T4: the identity is per SERVER, and every metadata value is exact
// ---------------------------------------------------------------------------

// TestMQTTEphemeralIdentityIsPerServer pins that two brokers in one process derive
// different names AND different delivery subjects, and that the metadata each stamps
// carries the exact values the refusal rests on — asserted by VALUE, because a test
// that only checks a key is present passes against a builder that writes the wrong
// one.
func TestMQTTEphemeralIdentityIsPerServer(t *testing.T) {
	e := testNewWatchEnv(t, "ephperserver")

	srvA := testEphemeralServer(t, e, "A")
	srvB := testEphemeralServer(t, e, "B")
	idA, ownerA := srvA.ephemeralIdentity()
	idB, ownerB := srvB.ephemeralIdentity()

	if idA == idB {
		t.Fatal("two Servers in one process must not share the name token, or each would delete the other's consumers")
	}
	if ownerA == ownerB {
		t.Fatal("two Servers must not share the owner nonce, which is what makes a collision a refusal")
	}
	if idA == ownerA || idB == ownerB {
		t.Fatal("the two tokens must be INDEPENDENT draws: a copy of the name token cannot discriminate a collision")
	}
	for _, pair := range [][2]string{
		{mqttRetainedObserverName(idA), mqttRetainedObserverName(idB)},
		{mqttSessRecordWatchName(idA), mqttSessRecordWatchName(idB)},
		{mqttRetainedObserverDeliverSubject(idA, 1), mqttRetainedObserverDeliverSubject(idB, 1)},
		{mqttSessRecordWatchDeliverSubject(idA), mqttSessRecordWatchDeliverSubject(idB)},
	} {
		if pair[0] == pair[1] {
			t.Fatalf("two Servers derived the same value %q for one purpose", pair[0])
		}
	}

	for _, tc := range []struct {
		purpose mqttEphemeralPurpose
		want    string
	}{
		{mqttEphemeralRetainedObserver, mqttEphemeralPurposeRetained},
		{mqttEphemeralSessRecordWatch, mqttEphemeralPurposeWatch},
	} {
		md := mqttEphemeralConsumerMetadata(ownerA, tc.purpose)
		if got := md[mqttEphemeralOwnerKey]; got != mqttEphemeralOwnerValue {
			t.Fatalf("owner metadata: got %q want %q", got, mqttEphemeralOwnerValue)
		}
		if got := md[mqttEphemeralPurposeKey]; got != tc.want {
			t.Fatalf("purpose metadata: got %q want %q", got, tc.want)
		}
		if got := md[mqttEphemeralRunKey]; got != ownerA {
			t.Fatalf("run metadata must be this server's OWNER nonce, got %q want %q", got, ownerA)
		}
		if got := md[mqttEphemeralSchemaKey]; got != mqttEphemeralSchema {
			t.Fatalf("schema metadata: got %q want %q", got, mqttEphemeralSchema)
		}
		if md[mqttEphemeralRunKey] == idA {
			t.Fatal("the run metadata must carry the owner nonce, not the name token")
		}
	}

	// Both really initialize, and both consumers coexist: neither run's provisioning
	// names anything of the other's.
	if _, err := testEphemeralInit(srvA); err != nil {
		t.Fatalf("server A initialization: %v", err)
	}
	if _, err := testEphemeralInit(srvB); err != nil {
		t.Fatalf("server B initialization: %v", err)
	}

	// What the server STORED, for BOTH purposes. Asserting the builder alone would pass
	// against a request that never carried the map: the builder is not the thing the
	// ActionCreate equality check reads.
	for _, tc := range []struct {
		stream, name, purpose string
	}{
		{mqttRetainedMsgsStreamName, mqttRetainedObserverName(idA), mqttEphemeralPurposeRetained},
		{mqttSessStreamName, mqttSessRecordWatchName(idA), mqttEphemeralPurposeWatch},
	} {
		ci, err := e.asm.jsa.lookupConsumer(tc.stream, tc.name)
		if err != nil {
			t.Fatalf("reading the stored config of %q: %v", tc.name, err)
		}
		md := ci.Config.Metadata
		if got := md[mqttEphemeralOwnerKey]; got != mqttEphemeralOwnerValue {
			t.Fatalf("%s: stored owner metadata is %q, want %q", tc.name, got, mqttEphemeralOwnerValue)
		}
		if got := md[mqttEphemeralPurposeKey]; got != tc.purpose {
			t.Fatalf("%s: stored purpose metadata is %q, want %q", tc.name, got, tc.purpose)
		}
		if got := md[mqttEphemeralRunKey]; got != ownerA {
			t.Fatalf("%s: stored run metadata is %q, want server A's owner nonce %q", tc.name, got, ownerA)
		}
		if got := md[mqttEphemeralSchemaKey]; got != mqttEphemeralSchema {
			t.Fatalf("%s: stored schema metadata is %q, want %q", tc.name, got, mqttEphemeralSchema)
		}
		if ci.Config.DeliverSubject == "" {
			t.Fatalf("%s: the stored config must carry a delivery subject", tc.name)
		}
	}
	sessNames := testConsumerNames(t, &e.asm.jsa, mqttSessStreamName)
	for _, want := range []string{mqttSessRecordWatchName(idA), mqttSessRecordWatchName(idB)} {
		if testCountName(sessNames, want) != 1 {
			t.Fatalf("both runs' watch consumers must coexist, %q missing from %v", want, sessNames)
		}
	}
}

// ---------------------------------------------------------------------------
// T3: a colliding NAME with a different owner nonce is REFUSED, not adopted
// ---------------------------------------------------------------------------

// TestMQTTWatchCreateRefusesAForeignRun forces the collision AT THE DRAW: both
// Servers are fed the same name token and independent owner nonces, which is the only
// arrangement that discriminates a nonce DERIVED from the name token — post-mint
// assignment would leave each nonce already derived from its own original id, so the
// two would still differ and the mutation would survive.
//
// The watch is the purpose under test: the observer deletes its name before creating,
// so it has no refusal to make.
func TestMQTTWatchCreateRefusesAForeignRun(t *testing.T) {
	e := testNewWatchEnv(t, "ephcollide")

	// The name token is forced equal for both runs while each nonce draw returns a
	// distinct value, so the two configs differ in exactly the field the server's
	// equality check can see.
	const sharedID = "COLLIDING-ID-TOKEN"
	var seq int
	var seqMu sync.Mutex
	defer setMQTTTestEphemeralDraw(func(which string) (string, bool) {
		if which == "id" {
			return sharedID, true
		}
		seqMu.Lock()
		defer seqMu.Unlock()
		seq++
		return "OWNER-NONCE-" + strings.Repeat("x", seq), true
	})()

	srvA := testEphemeralServer(t, e, "A")
	srvB := testEphemeralServer(t, e, "B")
	idA, ownerA := srvA.ephemeralIdentity()
	idB, ownerB := srvB.ephemeralIdentity()
	if idA != idB {
		t.Fatalf("fixture: the two runs must collide on the name token, got %q and %q", idA, idB)
	}
	if ownerA == ownerB {
		t.Fatalf("fixture: the two runs must keep INDEPENDENT owner nonces, both got %q", ownerA)
	}

	if _, err := testEphemeralInit(srvA); err != nil {
		t.Fatalf("server A initialization: %v", err)
	}
	watchName := mqttSessRecordWatchName(idA)
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttSessStreamName), watchName); got != 1 {
		t.Fatalf("fixture: server A must own one watch consumer, got %d", got)
	}

	// B now meets A's consumer under its own derived name. Its config differs in the
	// owner nonce alone, which is what ActionCreate refuses.
	if _, err := testEphemeralInit(srvB); err == nil {
		t.Fatal("a colliding run must be REFUSED at the watch create, not silently adopt another run's consumer")
	}
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttSessStreamName), watchName); got != 1 {
		t.Fatalf("the refused run must leave the incumbent's consumer alone, got %d under %q", got, watchName)
	}
}

// ---------------------------------------------------------------------------
// T5 / T8: an indefinite pre-create delete fails closed, within its own budget
// ---------------------------------------------------------------------------

// TestMQTTEphemeralDeleteAmbiguityFailsClosed pins that an ambiguity is not a verdict:
// with the delete never reaching JetStream, initialization fails and NO create is
// issued for that purpose. A create here would adopt a consumer whose position is
// unknown, which is exactly the silent retained-index loss the protocol exists to
// prevent.
//
// It also pins the budget's scope (T8): exactly two delete ATTEMPTS — hook
// invocations, since with send=false no JetStream request is sent at all — and the
// rollback adds none, because nothing was created to roll back.
func TestMQTTEphemeralDeleteAmbiguityFailsClosed(t *testing.T) {
	e := testNewWatchEnv(t, "ephambig")

	srv := testEphemeralServer(t, e, "ambig")
	id, _ := srv.ephemeralIdentity()
	obsName := mqttRetainedObserverName(id)

	var attempts int
	var createdObserver bool
	var mu sync.Mutex
	defer setMQTTTestConsumerDeleteHook(func(_, name string, _ int) (bool, error, bool) {
		if name != obsName {
			return true, nil, false
		}
		mu.Lock()
		attempts++
		mu.Unlock()
		// Not sent: the consumer, if any, is untouched and the caller cannot know.
		return false, errMQTTTestLostResponse, true
	})()
	defer setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if ccr.Config.Name == obsName {
			mu.Lock()
			createdObserver = true
			mu.Unlock()
		}
		return err
	})()

	if _, err := testEphemeralInit(srv); err == nil {
		t.Fatal("an indefinite delete must fail initialization closed rather than create over it")
	}

	mu.Lock()
	gotAttempts, gotCreate := attempts, createdObserver
	mu.Unlock()
	if gotCreate {
		t.Fatal("no create may be issued for a purpose whose absence was never established")
	}
	if gotAttempts != mqttEphemeralDeleteAttempts {
		t.Fatalf("the absence phase must spend exactly its budget of %d attempts, made %d",
			mqttEphemeralDeleteAttempts, gotAttempts)
	}

	// A LATER invocation is a new attempt and is entitled to its own budget: the
	// alternative is a Server permanently unable to initialize after one transient
	// JetStream outage.
	mu.Lock()
	attempts = 0
	mu.Unlock()
	if _, err := testEphemeralInit(srv); err == nil {
		t.Fatal("fixture: the second invocation must also fail while the delete is forced not to land")
	}
	mu.Lock()
	gotAttempts = attempts
	mu.Unlock()
	if gotAttempts != mqttEphemeralDeleteAttempts {
		t.Fatalf("a later invocation must get its own budget of %d attempts, made %d",
			mqttEphemeralDeleteAttempts, gotAttempts)
	}
}

// ---------------------------------------------------------------------------
// T10: a LOST delete response converges on the re-attempt, and the create proceeds
// ---------------------------------------------------------------------------

// TestMQTTLostDeleteResponseConvergesThenCreates is the arm that makes the
// re-attempt load-bearing. The first delete LANDS and its reply is lost; the second
// returns not-found, which is a definite absence; the create then proceeds. Without
// the re-attempt the first ambiguity would fail initialization outright.
func TestMQTTLostDeleteResponseConvergesThenCreates(t *testing.T) {
	e := testNewWatchEnv(t, "ephlostdel")

	srv := testEphemeralServer(t, e, "lostdel")
	id, _ := srv.ephemeralIdentity()
	obsName := mqttRetainedObserverName(id)

	var firstSeen bool
	var mu sync.Mutex
	defer setMQTTTestConsumerDeleteHook(func(_, name string, attempt int) (bool, error, bool) {
		if name != obsName || attempt != 1 {
			return true, nil, false
		}
		mu.Lock()
		firstSeen = true
		mu.Unlock()
		// Sent — so it really lands — but the caller sees a lost response.
		return true, errMQTTTestLostResponse, true
	})()

	if _, err := testEphemeralInit(srv); err != nil {
		t.Fatalf("a lost delete response must converge on the re-attempt, not fail initialization: %v", err)
	}
	mu.Lock()
	seen := firstSeen
	mu.Unlock()
	if !seen {
		t.Fatal("fixture: the ambiguous first delete never ran, so this test proves nothing")
	}
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttRetainedMsgsStreamName), obsName); got != 1 {
		t.Fatalf("the create must proceed once absence is definite, got %d consumers under %q", got, obsName)
	}
}

// ---------------------------------------------------------------------------
// T12 / T11: an ERRORED create never licenses a delete
// ---------------------------------------------------------------------------

// TestMQTTErroredCreateNeverLicensesADelete pins the rule that replaced the
// create-outcome partition: only a CONFIRMED create licenses a rollback delete.
//
// The case that forced it is the one with a foreign occupant: both server paths
// return structured errors BEFORE they look the existing consumer up, so a rollback
// that trusted an error could delete a consumer that was never ours. The test runs
// both ways — with a foreign consumer under the derived watch name, and without —
// so it discriminates the LICENCE rather than the error text.
func TestMQTTErroredCreateNeverLicensesADelete(t *testing.T) {
	e := testNewWatchEnv(t, "epherrcreate")

	// Both arms end with exactly one consumer alive under the name and ZERO deletes,
	// and they get there differently — which is the point. With a foreign occupant the
	// create is REFUSED and the incumbent survives untouched; with the name free our
	// own create LANDS and only its reply is rewritten, so the survivor is ours,
	// waiting for the next attempt of this run to adopt it. Neither may be deleted,
	// because the error the rollback saw cannot tell those two worlds apart.
	for _, tc := range []struct {
		name       string
		preCreate  bool
		wantRunKey string
	}{
		{"a foreign consumer already occupies the name", true, "A-FOREIGN-RUN"},
		{"nothing occupies the name, so our own create lands and its reply is lost", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := testEphemeralServer(t, e, "errcreate")
			id, _ := srv.ephemeralIdentity()
			watchName := mqttSessRecordWatchName(id)

			if tc.preCreate {
				// A consumer under our derived name that is NOT ours: a different
				// nonce is all it takes to make it foreign.
				ccr := mqttSessRecordWatchConsumerConfig(e.asm.domainTk,
					mqttSessRecordWatchDeliverSubject(id), id, "A-FOREIGN-RUN")
				if _, err := e.asm.jsa.createEphemeralConsumer(ccr); err != nil {
					t.Fatalf("fixture: planting the foreign consumer: %v", err)
				}
			}

			var deletes int
			var mu sync.Mutex
			defer setMQTTTestConsumerDeleteHook(func(_, name string, _ int) (bool, error, bool) {
				if name == watchName {
					mu.Lock()
					deletes++
					mu.Unlock()
				}
				return true, nil, false
			})()
			// Every watch create returns a structured error, whatever really happened.
			defer setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
				if ccr.Stream == mqttSessStreamName {
					return errMQTTTestLostResponse
				}
				return err
			})()

			if _, err := testEphemeralInit(srv); err == nil {
				t.Fatal("fixture: initialization must fail while the watch create errors")
			}
			mu.Lock()
			got := deletes
			mu.Unlock()
			if got != 0 {
				t.Fatalf("an errored create licenses no delete, but %q was deleted %d time(s)", watchName, got)
			}
			if surviving := testCountName(testConsumerNames(t, &e.asm.jsa, mqttSessStreamName), watchName); surviving != 1 {
				t.Fatalf("exactly one consumer must survive under %q, found %d", watchName, surviving)
			}
			// Whose consumer survived is the sharper half: in the foreign arm it must
			// still be the incumbent's, which is what a rollback trusting the error
			// would have destroyed.
			wantRun := tc.wantRunKey
			if wantRun == "" {
				_, owner := srv.ephemeralIdentity()
				wantRun = owner
			}
			ci, err := e.asm.jsa.lookupConsumer(mqttSessStreamName, watchName)
			if err != nil {
				t.Fatalf("reading the surviving consumer: %v", err)
			}
			if got := ci.Config.Metadata[mqttEphemeralRunKey]; got != wantRun {
				t.Fatalf("the surviving consumer belongs to run %q, want %q", got, wantRun)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// T2: the retained INDEX survives a retried initialization
// ---------------------------------------------------------------------------

// TestMQTTRetainedIndexSurvivesInitRetry is the regression test for the hazard the
// whole observer protocol exists to close, and it is the one that fails if the
// pre-create delete is dropped.
//
// A repeated named create ADOPTS the existing consumer at its current position and
// replays nothing. The retained index (as.retmsgs) is written by nothing but this
// consumer's callback and the publishing broker's own local seed, and a WILDCARD
// subscription has nothing else to consult — so a retry that adopted an observer
// already drained by a discarded manager would serve no retained message for the life
// of the process, silently.
//
// The message is stored by a PEER manager, never by the retrying broker, so its own
// local seed cannot mask the bug. Attempt 1 consumes it and then fails, with its
// rollback delete forced not to land — the only state that leaves a drained consumer
// alive for attempt 2 to meet.
func TestMQTTRetainedIndexSurvivesInitRetry(t *testing.T) {
	e := testNewWatchEnv(t, "ephretained")

	const topic = "eph/retained/probe"
	subject := mqttRetainedMsgsStreamSubject + "eph.retained.probe"
	msg, hdrLen := mqttEncodeRetainedMessage(&mqttRetainedMsg{
		Topic:  topic,
		Origin: "peer",
		Source: "peer",
		Flags:  1,
		Msg:    []byte("retained-payload"),
	})
	if _, err := e.asm.jsa.storeMsg(subject, hdrLen, msg); err != nil {
		t.Fatalf("peer store of the retained message: %v", err)
	}

	srv := testEphemeralServer(t, e, "retainedretry")
	id, _ := srv.ephemeralIdentity()
	obsName := mqttRetainedObserverName(id)

	// Block ONLY the rollback's delete of the observer — the second time this name is
	// deleted in attempt 1. The pre-create delete (the first) must go through, or the
	// attempt fails closed before it ever creates the consumer that drains the stream.
	var obsDeletes int
	var mu sync.Mutex
	defer setMQTTTestConsumerDeleteHook(func(_, name string, _ int) (bool, error, bool) {
		if name != obsName {
			return true, nil, false
		}
		mu.Lock()
		obsDeletes++
		n := obsDeletes
		mu.Unlock()
		if n == 2 {
			return false, errMQTTTestLostResponse, true
		}
		return true, nil, false
	})()

	watchFailed := false
	watchRestore := setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if ccr.Stream == mqttSessStreamName {
			mu.Lock()
			watchFailed = true
			mu.Unlock()
			return errMQTTTestLostResponse
		}
		return err
	})

	// Attempt 1: the observer is created, drains the retained backlog into a manager
	// that is then discarded, and the attempt fails at the watch.
	asm1, err := testEphemeralInit(srv)
	if err == nil {
		t.Fatal("fixture: attempt 1 must fail at the watch create")
	}
	_ = asm1
	watchRestore()
	mu.Lock()
	failed, deletes := watchFailed, obsDeletes
	mu.Unlock()
	if !failed || deletes < 2 {
		t.Fatalf("fixture: attempt 1 must have created the observer and then tried to roll it back (watch failed=%v, observer deletes=%d)", failed, deletes)
	}
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttRetainedMsgsStreamName), obsName); got != 1 {
		t.Fatalf("fixture: the blocked rollback must leave attempt 1's observer alive, found %d under %q", got, obsName)
	}

	// Attempt 2: this is where an adopted consumer would deliver nothing.
	asm2, err := testEphemeralInit(srv)
	if err != nil {
		t.Fatalf("attempt 2 must succeed: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !asm2.retainedIndexedLive("eph.retained.probe") {
		if time.Now().After(deadline) {
			t.Fatalf("the retried initialization must replay the retained backlog into its own index; %q never arrived, "+
				"which is what a consumer ADOPTED at its previous position delivers", topic)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// The OBSERVER side of the same two rules
// ---------------------------------------------------------------------------

// TestMQTTErroredObserverCreateNeverLicensesADelete is the retained observer's half of
// TestMQTTErroredCreateNeverLicensesADelete, and it exists because that test rewrites
// only $MQTT_sess creates: without this one, an observer-side change that confirmed a
// create on an error would go untested.
//
// The observer deletes its name BEFORE creating, so the count that discriminates is
// the delete count AFTER the create: the pre-create delete is expected, a second one
// is the rollback acting on a licence it does not have.
func TestMQTTErroredObserverCreateNeverLicensesADelete(t *testing.T) {
	e := testNewWatchEnv(t, "epherrobs")

	srv := testEphemeralServer(t, e, "errobs")
	id, _ := srv.ephemeralIdentity()
	obsName := mqttRetainedObserverName(id)

	var deletesBeforeCreate, deletesAfterCreate int
	var created bool
	var mu sync.Mutex
	t.Cleanup(setMQTTTestConsumerDeleteHook(func(_, name string, _ int) (bool, error, bool) {
		if name == obsName {
			mu.Lock()
			if created {
				deletesAfterCreate++
			} else {
				deletesBeforeCreate++
			}
			mu.Unlock()
		}
		return true, nil, false
	}))
	// The observer's create LANDS and its response is lost — the state in which the
	// consumer really exists and the caller cannot know whose it is.
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if ccr.Stream == mqttRetainedMsgsStreamName {
			mu.Lock()
			created = true
			mu.Unlock()
			return errMQTTTestLostResponse
		}
		return err
	}))

	if _, err := testEphemeralInit(srv); err == nil {
		t.Fatal("fixture: initialization must fail while the observer create's response is lost")
	}

	mu.Lock()
	before, after, ran := deletesBeforeCreate, deletesAfterCreate, created
	mu.Unlock()
	if !ran {
		t.Fatal("fixture: the observer create never ran, so this test proves nothing")
	}
	if before != 1 {
		t.Fatalf("the observer must establish absence with exactly one delete before creating, made %d", before)
	}
	if after != 0 {
		t.Fatalf("an errored observer create licenses no rollback delete, but %q was deleted %d time(s) after it", obsName, after)
	}
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttRetainedMsgsStreamName), obsName); got != 1 {
		t.Fatalf("the consumer whose create response was lost must survive for the next attempt, found %d under %q", got, obsName)
	}
}

// TestMQTTEphemeralDeleteDefiniteSuccessProceeds pins the other half of the absence
// partition: a delete that comes back DEFINITE consumes exactly one attempt and the
// create then proceeds. The count is what discriminates a success misclassified as an
// ambiguity — that mutation would issue a second delete, receive not-found, and still
// create, so only the attempt count can see it.
func TestMQTTEphemeralDeleteDefiniteSuccessProceeds(t *testing.T) {
	e := testNewWatchEnv(t, "ephdefinite")

	srv := testEphemeralServer(t, e, "definite")
	id, _ := srv.ephemeralIdentity()
	obsName := mqttRetainedObserverName(id)

	// A first initialization leaves a real consumer under the derived name, so the
	// second attempt's delete has something to remove and returns a definite SUCCESS
	// rather than not-found.
	if _, err := testEphemeralInit(srv); err != nil {
		t.Fatalf("fixture: the first initialization must succeed: %v", err)
	}
	if got := testCountName(testConsumerNames(t, &e.asm.jsa, mqttRetainedMsgsStreamName), obsName); got != 1 {
		t.Fatalf("fixture: the first attempt must leave one observer, got %d", got)
	}

	var attempts int
	var mu sync.Mutex
	t.Cleanup(setMQTTTestConsumerDeleteHook(func(_, name string, _ int) (bool, error, bool) {
		if name == obsName {
			mu.Lock()
			attempts++
			mu.Unlock()
		}
		return true, nil, false
	}))

	if _, err := testEphemeralInit(srv); err != nil {
		t.Fatalf("the second attempt must succeed after a definite delete: %v", err)
	}
	mu.Lock()
	got := attempts
	mu.Unlock()
	if got != 1 {
		t.Fatalf("a definite delete success must consume exactly one attempt, made %d", got)
	}
	if n := testCountName(testConsumerNames(t, &e.asm.jsa, mqttRetainedMsgsStreamName), obsName); n != 1 {
		t.Fatalf("the recreated observer must be exactly one consumer, found %d", n)
	}
}

// TestMQTTInitRefusedWhileShuttingDown pins the admission check that keeps a CONNECT
// queued behind a failed eager initialization from building a whole manager — two
// ephemeral consumers included — for a server on its way out. The success path has no
// rollback, so such a manager's consumers would wait out InactiveThreshold.
func TestMQTTInitRefusedWhileShuttingDown(t *testing.T) {
	e := testNewWatchEnv(t, "ephshutdown")

	srv := testEphemeralServer(t, e, "shutdown")
	id, _ := srv.ephemeralIdentity()
	srv.running.Store(false)

	if _, err := testEphemeralInit(srv); err == nil {
		t.Fatal("a shutting-down server must refuse to CREATE an account manager")
	}
	for _, tc := range []struct{ stream, name string }{
		{mqttRetainedMsgsStreamName, mqttRetainedObserverName(id)},
		{mqttSessStreamName, mqttSessRecordWatchName(id)},
	} {
		if got := testCountName(testConsumerNames(t, &e.asm.jsa, tc.stream), tc.name); got != 0 {
			t.Fatalf("the refused initialization must have created nothing, found %d under %q", got, tc.name)
		}
	}
}
