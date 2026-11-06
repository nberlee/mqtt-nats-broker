package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The tests in this file arm package-level seams and so never run in parallel.

// setStartupSeam arms a package-level test seam for the rest of the test.
func setStartupSeam[T any](t *testing.T, p *atomic.Pointer[T], v T) {
	t.Helper()
	prev := p.Load()
	p.Store(&v)
	t.Cleanup(func() { p.Store(prev) })
}

// The seams these tests arm, one setter each: the address of a package global is taken
// here, never in a test that also starts goroutines (TestNoPackageGlobalIsAssignedFromAGoroutine).

func setStartupBackoffHook(t *testing.T, v func(attempt int, err error)) {
	setStartupSeam(t, &mqttTestStartupBackoffHookV, v)
}

func setStartupTerminalHook(t *testing.T, v func(err error)) {
	setStartupSeam(t, &mqttTestStartupTerminalHookV, v)
}

func setStartupAttemptHook(t *testing.T, v func(jsa *mqttJSA)) {
	setStartupSeam(t, &mqttTestStartupAttemptHookV, v)
}

func setStartupBackoff(t *testing.T, v time.Duration) { setStartupSeam(t, &mqttTestStartupBackoffV, v) }

func setObserverOccupantLookupHook(t *testing.T, v func(ci *ConsumerInfo, err error) (*ConsumerInfo, error)) {
	setStartupSeam(t, &mqttTestObserverOccupantLookupHookV, v)
}

func setPreflightLookupHook(t *testing.T, v func(scan int, stream string, present bool)) {
	setStartupSeam(t, &mqttTestPreflightLookupHookV, v)
}

func setJSSendPublishHook(t *testing.T, v func(subject string)) {
	setStartupSeam(t, &mqttTestJSSendPublishHookV, v)
}

// startupFastBackoff shortens Start's first backoff, so a retry costs milliseconds.
func startupFastBackoff(t *testing.T) {
	t.Helper()
	setStartupBackoff(t, 10*time.Millisecond)
}

// startupTestServer builds, without starting, a broker on its own service connection to
// url. user/pass are the service credentials, empty for none.
func startupTestServer(t *testing.T, url, node, user, pass string, mod func(*Options)) *Server {
	t.Helper()
	var copts []nats.Option
	if user != "" {
		copts = append(copts, nats.UserInfo(user, pass))
	}
	nc, err := nats.Connect(url, copts...)
	if err != nil {
		t.Fatalf("service connect: %v", err)
	}
	t.Cleanup(nc.Close)
	opts := &Options{MQTT: MQTTOpts{StreamReplicas: 1, Host: "127.0.0.1", Port: -1}}
	if mod != nil {
		mod(opts)
	}
	s := NewServer(nc, opts, node)
	t.Cleanup(s.Shutdown)
	return s
}

// startWithin runs Start(ctx) and fails the test if it has not returned within d. A Start
// that never returns must fail its test rather than hang the binary.
func startWithin(t *testing.T, s *Server, ctx context.Context, d time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		t.Fatalf("Start did not return within %s", d)
		return nil
	}
}

func isObserverCreate(ccr *CreateConsumerRequest) bool {
	return strings.HasPrefix(ccr.Config.Name, mqttRetainedMsgsStreamName+"_obs_")
}

func isWatchCreate(ccr *CreateConsumerRequest) bool {
	return strings.HasPrefix(ccr.Config.Name, mqttSessStreamName+"_watch_")
}

// consumerNamesOn lists every consumer name on stream, read on a separate connection.
func consumerNamesOn(t *testing.T, url, stream string) map[string]bool {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("stream %s: %v", stream, err)
	}
	out := map[string]bool{}
	names := st.ConsumerNames(ctx)
	for n := range names.Name() {
		out[n] = true
	}
	if err := names.Err(); err != nil {
		t.Fatalf("list consumers on %s: %v", stream, err)
	}
	return out
}

// newNamesWithPrefix is how many names in after, and not in before, start with prefix.
func newNamesWithPrefix(before, after map[string]bool, prefix string) int {
	n := 0
	for name := range after {
		if !before[name] && strings.HasPrefix(name, prefix) {
			n++
		}
	}
	return n
}

// seedRetained starts a broker on url, publishes one retained message on topic through it,
// shuts it down, and returns the consumer names it left on the two ephemeral streams.
func seedRetained(t *testing.T, url, topic string) (obs, watch map[string]bool) {
	t.Helper()
	seed := startupTestServer(t, url, "seed", "", "", nil)
	if err := seed.Start(context.Background()); err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + seed.Addr().String()).
		SetClientID("seeder").SetConnectTimeout(5 * time.Second))
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("seed connect: %v", tok.Error())
	}
	if tok := c.Publish(topic, 1, true, "kept"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("seed retained publish: %v", tok.Error())
	}
	c.Disconnect(100)
	seed.Shutdown()
	return consumerNamesOn(t, url, mqttRetainedMsgsStreamName), consumerNamesOn(t, url, mqttSessStreamName)
}

// expectRetained subscribes to filter on s and requires the retained message on topic.
func expectRetainedReplayed(t *testing.T, s *Server, filter, topic string) {
	t.Helper()
	got := make(chan string, 4)
	c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
		SetClientID("reader").SetConnectTimeout(5 * time.Second))
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("reader connect: %v", tok.Error())
	}
	defer c.Disconnect(100)
	if tok := c.Subscribe(filter, 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- m.Topic() }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("reader subscribe: %v", tok.Error())
	}
	select {
	case tp := <-got:
		if tp != topic {
			t.Fatalf("retained delivery on %q, want %q", tp, topic)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("no retained message for %q after the retried initialization: the observer did not replay the store", filter)
	}
}

// lostResponse is a create that LANDED and whose response the broker never saw.
func lostResponse() error {
	return &mqttJSATimeoutError{cause: errors.New("test: the create landed and its response was lost")}
}

// TestStartRetriesALostObserverCreateResponse: the retained observer's CREATE lands and
// its response is lost, twice. Start retries within the same run: it ends with ONE
// observer of this run — the definite delete reconciles each landed create — and that
// observer replays the retained store from the start.
func TestStartRetriesALostObserverCreateResponse(t *testing.T) {
	ns := runJetStreamServerHA(t)
	url := ns.ClientURL()
	startupFastBackoff(t)
	obsBefore, _ := seedRetained(t, url, "r/kept")

	var observerCreates atomic.Int32
	defer setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if err == nil && isObserverCreate(ccr) && observerCreates.Add(1) <= 2 {
			return lostResponse()
		}
		return err
	})()

	s := startupTestServer(t, url, "node-b", "", "", nil)
	if err := startWithin(t, s, context.Background(), 60*time.Second); err != nil {
		t.Fatalf("Start through two lost observer responses: %v", err)
	}
	if n := observerCreates.Load(); n < 3 {
		t.Fatalf("%d observer creates, want at least 3 (two lost, one answered)", n)
	}
	obsAfter := consumerNamesOn(t, url, mqttRetainedMsgsStreamName)
	if n := newNamesWithPrefix(obsBefore, obsAfter, mqttRetainedMsgsStreamName+"_obs_"); n != 1 {
		t.Fatalf("%d retained observers after the retried start, want exactly 1 (names %v)", n, obsAfter)
	}
	expectRetainedReplayed(t, s, "r/#", "r/kept")
}

// TestStartRetriesALostWatchCreateResponse: the session-record watch's CREATE lands and its
// response is lost. The retry adopts the config-identical watch: one watch of this run.
func TestStartRetriesALostWatchCreateResponse(t *testing.T) {
	ns := runJetStreamServerHA(t)
	url := ns.ClientURL()
	startupFastBackoff(t)
	_, watchBefore := seedRetained(t, url, "r/kept")

	var watchCreates atomic.Int32
	defer setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if err == nil && isWatchCreate(ccr) && watchCreates.Add(1) == 1 {
			return lostResponse()
		}
		return err
	})()

	s := startupTestServer(t, url, "node-b", "", "", nil)
	if err := startWithin(t, s, context.Background(), 60*time.Second); err != nil {
		t.Fatalf("Start through a lost watch response: %v", err)
	}
	watchAfter := consumerNamesOn(t, url, mqttSessStreamName)
	if n := newNamesWithPrefix(watchBefore, watchAfter, mqttSessStreamName+"_watch_"); n != 1 {
		t.Fatalf("%d session-record watches after the retried start, want exactly 1 (names %v)", n, watchAfter)
	}
}

// TestStartRetriesNoRespondersAndClusterUnavailable: a transport no-responders status and
// JetStream 10008 are availability answers; Start retries through both.
func TestStartRetriesNoRespondersAndClusterUnavailable(t *testing.T) {
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	var backoffs atomic.Int32
	setStartupBackoffHook(t, func(int, error) { backoffs.Add(1) })

	var observerCreates atomic.Int32
	defer setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if err != nil || !isObserverCreate(ccr) {
			return err
		}
		switch observerCreates.Add(1) {
		case 1:
			return &ApiError{Code: 503}
		case 2:
			return &ApiError{Code: 503, ErrCode: 10008, Description: "JetStream system temporarily unavailable"}
		}
		return nil
	})()

	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
	if err := startWithin(t, s, context.Background(), 60*time.Second); err != nil {
		t.Fatalf("Start through no-responders and 10008: %v", err)
	}
	if n := backoffs.Load(); n != 2 {
		t.Fatalf("%d backoffs, want 2 (one per availability answer)", n)
	}
}

// TestStartFailsAtOnceOnAPermanentInitializationError: a refusal ends startup after ONE
// attempt, with the refusal as the error and nothing left running.
func TestStartFailsAtOnceOnAPermanentInitializationError(t *testing.T) {
	alreadyExists := &ApiError{Code: 400, ErrCode: 10148, Description: "consumer already exists"}

	cases := []struct {
		name string
		// opts configures the NATS server; nil for the default.
		opts  func(*natsserver.Options)
		setup func(t *testing.T, url string, s *Server)
		want  string
	}{
		{
			name: "a foreign consumer holds the observer's name",
			setup: func(t *testing.T, _ string, _ *Server) {
				restore := setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
					if isObserverCreate(ccr) {
						return alreadyExists
					}
					return err
				})
				t.Cleanup(restore)
				setObserverOccupantLookupHook(t, func(ci *ConsumerInfo, err error) (*ConsumerInfo, error) {
					return &ConsumerInfo{Config: &ConsumerConfig{Metadata: map[string]string{mqttEphemeralRunKey: "another-run"}}}, nil
				})
			},
			want: "consumer already exists",
		},
		{
			name: "a partial stream bundle",
			setup: func(t *testing.T, url string, s *Server) {
				nc, err := nats.Connect(url)
				if err != nil {
					t.Fatal(err)
				}
				defer nc.Close()
				js, _ := jetstream.New(nc)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: mqttOutStreamName, Subjects: []string{"partial.bundle.>"}, Metadata: map[string]string{mqttStoreFormatMetaKey: mqttStoreFormat}}); err != nil {
					t.Fatalf("create one broker-owned stream: %v", err)
				}
				s.bundleSettleBudget = 200 * time.Millisecond
			},
			want: "partial",
		},
		{
			name: "a request the connection refuses to publish",
			opts: func(o *natsserver.Options) { o.MaxPayload = 256 },
			setup: func(t *testing.T, _ string, s *Server) {
				s.opts.MQTT.JSAPITimeout = time.Second
			},
			want: nats.ErrMaxPayload.Error(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir()}
			if tc.opts != nil {
				tc.opts(o)
			}
			ns := runStartupNATS(t, o)
			startupFastBackoff(t)
			var backoffs atomic.Int32
			setStartupBackoffHook(t, func(int, error) { backoffs.Add(1) })
			s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
			tc.setup(t, ns.ClientURL(), s)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := startWithin(t, s, ctx, 60*time.Second)
			if err == nil || errors.Is(err, errMQTTStartupCancelled) {
				t.Fatalf("Start = %v, want the refusal (a retry loop ends only by cancellation)", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Start error %q does not name %q", err, tc.want)
			}
			if n := backoffs.Load(); n != 0 {
				t.Errorf("%d backoffs before the refusal, want 0", n)
			}
			if s.running.Load() {
				t.Error("the server still runs after a failed Start")
			}
		})
	}
}

// runStartupNATS starts a NATS server with o.
func runStartupNATS(t *testing.T, o *natsserver.Options) *natsserver.Server {
	t.Helper()
	ns, err := natsserver.NewServer(o)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)
	return ns
}

// permissionServer starts NATS with a service user svc whose permissions are perms.
func permissionServer(t *testing.T, perms *natsserver.Permissions) *natsserver.Server {
	t.Helper()
	return runStartupNATS(t, &natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		Users: []*natsserver.User{{Username: "svc", Password: "svcpass", Permissions: perms}},
	})
}

// TestStartFailsOnServiceConnectionPermissionViolations: a denial on the service
// connection during startup fails it, whether the denied operation would otherwise time
// out forever, was a subscription, or left the initialization standing.
func TestStartFailsOnServiceConnectionPermissionViolations(t *testing.T) {
	allowAll := []string{">"}
	cases := []struct {
		name  string
		perms *natsserver.Permissions
		arm   func(t *testing.T, s *Server)
	}{
		{
			name: "publishing the observer's CREATE",
			perms: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: allowAll, Deny: []string{"$JS.API.CONSUMER.CREATE.$MQTT_rmsgs", "$JS.API.CONSUMER.CREATE.$MQTT_rmsgs.>"}},
				Subscribe: &natsserver.SubjectPermission{Allow: allowAll},
			},
		},
		{
			name: "subscribing to the JSA replies",
			perms: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: allowAll},
				Subscribe: &natsserver.SubjectPermission{Allow: allowAll, Deny: []string{"$MQTT.JSA.>"}},
			},
		},
		{
			name: "subscribing to the observer's deliveries",
			perms: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: allowAll},
				Subscribe: &natsserver.SubjectPermission{Allow: allowAll, Deny: []string{"$MQTT.sub.*.rmsgs.>"}},
			},
		},
		{
			// Only the success-path check can see this one: every subscription is allowed
			// and the initialization succeeds.
			name: "an unrelated publication while the initialization succeeds",
			perms: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: allowAll, Deny: []string{"forbidden.>"}},
				Subscribe: &natsserver.SubjectPermission{Allow: allowAll},
			},
			arm: func(t *testing.T, s *Server) {
				var once sync.Once
				t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
					if err == nil && isWatchCreate(ccr) {
						once.Do(func() {
							_ = s.nrc.Publish("forbidden.x", nil)
							_ = s.nrc.Flush()
						})
					}
					return err
				}))
			},
		},
		{
			// The occupant lookup retries in place without returning from the attempt, so
			// the permission check has to run inside that loop.
			name: "looking up the observer's occupant",
			perms: &natsserver.Permissions{
				Publish:   &natsserver.SubjectPermission{Allow: allowAll, Deny: []string{"$JS.API.CONSUMER.INFO.>"}},
				Subscribe: &natsserver.SubjectPermission{Allow: allowAll},
			},
			arm: func(t *testing.T, _ *Server) {
				var first atomic.Bool
				t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
					if isObserverCreate(ccr) && first.CompareAndSwap(false, true) {
						return &ApiError{Code: 400, ErrCode: 10148, Description: "consumer already exists"}
					}
					return err
				}))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ns := permissionServer(t, tc.perms)
			startupFastBackoff(t)
			s := startupTestServer(t, ns.ClientURL(), "node-a", "svc", "svcpass", func(o *Options) {
				o.MQTT.JSAPITimeout = time.Second
			})
			if tc.arm != nil {
				tc.arm(t, s)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := startWithin(t, s, ctx, 60*time.Second)
			if err == nil || errors.Is(err, errMQTTStartupCancelled) {
				t.Fatalf("Start = %v, want the permission violation", err)
			}
			if !errors.Is(err, nats.ErrPermissionViolation) {
				t.Fatalf("Start error %q is not the service connection's permission violation", err)
			}
		})
	}
}

// TestStartReturnsPromptlyWhenCancelled: a cancelled startup ends within seconds wherever
// it waits — far inside one request timeout (15 s here) — and leaves nothing running.
func TestStartReturnsPromptlyWhenCancelled(t *testing.T) {
	const within = 3 * time.Second
	timeout15 := func(o *Options) { o.MQTT.JSAPITimeout = 15 * time.Second }

	run := func(t *testing.T, s *Server, entered <-chan struct{}) error {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Start(ctx) }()
		select {
		case <-entered:
		case err := <-done:
			cancel()
			t.Fatalf("Start returned %v before the target wait was entered", err)
		case <-time.After(60 * time.Second):
			cancel()
			t.Fatal("the target wait was never entered")
		}
		cancelled := time.Now()
		cancel()
		select {
		case err := <-done:
			if d := time.Since(cancelled); d > within {
				t.Errorf("Start returned %s after the cancellation, want within %s", d, within)
			}
			return err
		case <-time.After(60 * time.Second):
			t.Fatal("Start did not return after its context ended")
			return nil
		}
	}

	t.Run("during the backoff", func(t *testing.T) {
		ns := runJetStreamServerHA(t)
		setStartupBackoff(t, time.Minute)
		entered := make(chan struct{})
		var once sync.Once
		setStartupBackoffHook(t, func(int, error) { once.Do(func() { close(entered) }) })
		t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
			if isObserverCreate(ccr) {
				return lostResponse()
			}
			return err
		}))
		s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", timeout15)
		if err := run(t, s, entered); !errors.Is(err, errMQTTStartupCancelled) {
			t.Fatalf("Start = %v, want the startup cancellation", err)
		}
	})

	t.Run("during the bundle preflight", func(t *testing.T) {
		ns := runJetStreamServerHA(t)
		startupFastBackoff(t)
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		js, _ := jetstream.New(nc)
		if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: mqttOutStreamName, Subjects: []string{"partial.bundle.>"}, Metadata: map[string]string{mqttStoreFormatMetaKey: mqttStoreFormat}}); err != nil {
			t.Fatal(err)
		}
		nc.Close()
		entered := make(chan struct{})
		var once sync.Once
		setPreflightLookupHook(t, func(scan int, _ string, _ bool) {
			if scan >= 1 {
				once.Do(func() { close(entered) })
			}
		})
		s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", timeout15)
		s.bundleSettleBudget = time.Minute
		if err := run(t, s, entered); !errors.Is(err, errMQTTStartupCancelled) {
			t.Fatalf("Start = %v, want the startup cancellation", err)
		}
	})

	t.Run("during an unanswered request", func(t *testing.T) {
		ns := runJetStreamServerHA(t)
		startupFastBackoff(t)
		entered := make(chan struct{})
		var once sync.Once
		setStartupAttemptHook(t, func(jsa *mqttJSA) {
			drop := func(subject string, _ []byte) bool {
				if strings.Contains(subject, "CONSUMER.CREATE."+mqttRetainedMsgsStreamName) {
					once.Do(func() { close(entered) })
					return true
				}
				return false
			}
			jsa.testDropRequest.Store(&drop)
		})
		s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", timeout15)
		if err := run(t, s, entered); !errors.Is(err, errMQTTStartupCancelled) {
			t.Fatalf("Start = %v, want the startup cancellation", err)
		}
	})

	t.Run("between the observer and the watch", func(t *testing.T) {
		ns := runJetStreamServerHA(t)
		startupFastBackoff(t)
		obsBefore := map[string]bool{}
		entered := make(chan struct{})
		release := make(chan struct{})
		t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
			if err == nil && isObserverCreate(ccr) {
				close(entered)
				<-release
			}
			return err
		}))
		s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", timeout15)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Start(ctx) }()
		<-entered
		cancel()
		// The watcher closes the cancellation asynchronously; let it land before the
		// attempt moves on from the confirmed observer.
		time.Sleep(100 * time.Millisecond)
		close(release)
		select {
		case err := <-done:
			if !errors.Is(err, errMQTTStartupCancelled) {
				t.Fatalf("Start = %v, want the startup cancellation", err)
			}
		case <-time.After(within):
			t.Fatal("Start did not return promptly after the cancellation")
		}
		obsAfter := consumerNamesOn(t, ns.ClientURL(), mqttRetainedMsgsStreamName)
		if n := newNamesWithPrefix(obsBefore, obsAfter, mqttRetainedMsgsStreamName+"_obs_"); n != 0 {
			t.Errorf("%d observers left after a cancelled start, want the confirmed one rolled back", n)
		}
		watches := consumerNamesOn(t, ns.ClientURL(), mqttSessStreamName)
		if n := newNamesWithPrefix(map[string]bool{}, watches, mqttSessStreamName+"_watch_"); n != 0 {
			t.Errorf("%d watches created after the cancellation, want none", n)
		}
	})

	t.Run("while a CONNECT holds the initialization", func(t *testing.T) {
		ns := runJetStreamServerHA(t)
		setStartupBackoff(t, 300*time.Millisecond)
		// Start's first attempt fails retryably; while Start backs off, a CONNECT drives an
		// attempt of its own whose observer CREATE goes unanswered, holding sm.mu.
		var attempts atomic.Int32
		entered := make(chan struct{})
		var once sync.Once
		setStartupAttemptHook(t, func(jsa *mqttJSA) {
			if attempts.Add(1) == 1 {
				return
			}
			drop := func(subject string, _ []byte) bool {
				if strings.Contains(subject, "CONSUMER.CREATE."+mqttRetainedMsgsStreamName) {
					once.Do(func() { close(entered) })
					return true
				}
				return false
			}
			jsa.testDropRequest.Store(&drop)
		})
		var first atomic.Bool
		t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
			if isObserverCreate(ccr) && first.CompareAndSwap(false, true) {
				return lostResponse()
			}
			return err
		}))
		connectDuringBackoff := make(chan struct{})
		var cOnce sync.Once
		setStartupBackoffHook(t, func(int, error) { cOnce.Do(func() { close(connectDuringBackoff) }) })

		s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", timeout15)
		go func() {
			<-connectDuringBackoff
			c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
				SetClientID("early").SetConnectTimeout(30 * time.Second).SetConnectRetry(false))
			c.Connect().WaitTimeout(30 * time.Second)
		}()
		if err := run(t, s, entered); !errors.Is(err, errMQTTStartupCancelled) {
			t.Fatalf("Start = %v, want the startup cancellation", err)
		}
	})
}

// TestStartSucceedsWhenCancelledAtTheMomentOfSuccess: a cancellation racing the last step
// does not turn success into failure, and the started server serves.
func TestStartSucceedsWhenCancelledAtTheMomentOfSuccess(t *testing.T) {
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if err == nil && isWatchCreate(ccr) {
			cancel()
		}
		return err
	}))
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
	if err := startWithin(t, s, ctx, 60*time.Second); err != nil {
		t.Fatalf("Start whose context ended at its last step = %v, want nil", err)
	}
	got := make(chan string, 1)
	c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
		SetClientID("after-race").SetConnectTimeout(5 * time.Second))
	if tok := c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("connect to the started server: %v", tok.Error())
	}
	defer c.Disconnect(100)
	if tok := c.Subscribe("race/t", 1, func(_ pmqtt.Client, m pmqtt.Message) { got <- string(m.Payload()) }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	if tok := c.Publish("race/t", 1, false, "served"); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}
	select {
	case p := <-got:
		if p != "served" {
			t.Fatalf("received %q", p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server Start reported as started does not deliver")
	}
}

// TestAPermanentStartFailureAbandonsAConcurrentConnectInitialization: Start's attempt
// meets a refusal; the refusal cancels startup before the manager lock is released, so a
// CONNECT that takes the lock next — while Start has not even handled the error yet —
// abandons its attempt without issuing a single request.
func TestAPermanentStartFailureAbandonsAConcurrentConnectInitialization(t *testing.T) {
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	var attempts atomic.Int32
	connectAttemptBuilt := make(chan struct{})
	var connectRequests atomic.Int32
	setStartupAttemptHook(t, func(jsa *mqttJSA) {
		if attempts.Add(1) == 1 {
			return
		}
		drop := func(string, []byte) bool {
			connectRequests.Add(1)
			return true
		}
		jsa.testDropRequest.Store(&drop)
		close(connectAttemptBuilt)
	})
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if isObserverCreate(ccr) {
			return &ApiError{Code: 400, ErrCode: 10148, Description: "consumer already exists"}
		}
		return err
	}))
	setObserverOccupantLookupHook(t, func(*ConsumerInfo, error) (*ConsumerInfo, error) {
		return &ConsumerInfo{Config: &ConsumerConfig{Metadata: map[string]string{mqttEphemeralRunKey: "another-run"}}}, nil
	})
	parked := make(chan struct{})
	release := make(chan struct{})
	setStartupTerminalHook(t, func(error) {
		close(parked)
		<-release
	})

	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = 15 * time.Second })
	done := make(chan error, 1)
	go func() { done <- s.Start(context.Background()) }()
	<-parked
	connectDone := make(chan struct{})
	go func() {
		defer close(connectDone)
		c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
			SetClientID("racer").SetConnectTimeout(10 * time.Second))
		c.Connect().WaitTimeout(10 * time.Second)
	}()
	select {
	case <-connectAttemptBuilt:
	case <-time.After(30 * time.Second):
		t.Fatal("the CONNECT never drove an initialization attempt")
	}
	// Give the CONNECT's attempt the time it needs to issue its first request, were it
	// to issue one.
	time.Sleep(300 * time.Millisecond)
	if n := connectRequests.Load(); n != 0 {
		t.Fatalf("the CONNECT's attempt issued %d requests after Start's attempt met a refusal, want none", n)
	}
	close(release)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "consumer already exists") {
			t.Fatalf("Start = %v, want the refusal", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return")
	}
	<-connectDone
}

// connectDrivenRefusal arms one refusal for the attempt numbered n: its observer CREATE is
// refused by a foreign occupant. Every other attempt provisions normally.
func connectDrivenRefusal(t *testing.T, n int32) *atomic.Int32 {
	t.Helper()
	var attempts atomic.Int32
	setStartupAttemptHook(t, func(*mqttJSA) { attempts.Add(1) })
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if isObserverCreate(ccr) && attempts.Load() == n {
			return &ApiError{Code: 400, ErrCode: 10148, Description: "consumer already exists"}
		}
		return err
	}))
	setObserverOccupantLookupHook(t, func(ci *ConsumerInfo, err error) (*ConsumerInfo, error) {
		if attempts.Load() == n {
			return &ConsumerInfo{Config: &ConsumerConfig{Metadata: map[string]string{mqttEphemeralRunKey: "another-run"}}}, nil
		}
		return ci, err
	})
	return &attempts
}

// TestARefusalInAConnectDrivenAttemptDuringTheBackoffEndsStartup: Start's first attempt
// fails retryably; during the backoff a CONNECT's attempt meets a refusal. Startup ends
// with that refusal, and no further attempt begins.
func TestARefusalInAConnectDrivenAttemptDuringTheBackoffEndsStartup(t *testing.T) {
	ns := runJetStreamServerHA(t)
	setStartupBackoff(t, 3*time.Second)
	attempts := connectDrivenRefusal(t, 2)
	var first atomic.Bool
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if isObserverCreate(ccr) && first.CompareAndSwap(false, true) {
			return lostResponse()
		}
		if isObserverCreate(ccr) && attempts.Load() == 2 {
			return &ApiError{Code: 400, ErrCode: 10148, Description: "consumer already exists"}
		}
		return err
	}))
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
	var launched atomic.Bool
	setStartupBackoffHook(t, func(int, error) {
		if launched.CompareAndSwap(false, true) {
			go func() {
				c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
					SetClientID("during-backoff").SetConnectTimeout(10 * time.Second))
				c.Connect().WaitTimeout(10 * time.Second)
			}()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := startWithin(t, s, ctx, 60*time.Second)
	if err == nil || !strings.Contains(err.Error(), "consumer already exists") {
		t.Fatalf("Start = %v, want the CONNECT-driven attempt's refusal", err)
	}
	if n := attempts.Load(); n != 2 {
		t.Fatalf("%d initialization attempts, want 2: one after the CONNECT's refusal retried past it", n)
	}
}

// TestARefusalInAConnectDrivenAttemptBeforeTheEagerInitializationEndsStartup: a CONNECT
// that drives the first attempt, before Start's own eager initialization, meets a refusal.
// Startup ends with it; Start's own attempt never begins.
func TestARefusalInAConnectDrivenAttemptBeforeTheEagerInitializationEndsStartup(t *testing.T) {
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	attempts := connectDrivenRefusal(t, 1)
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
	refused := make(chan struct{})
	var once sync.Once
	setStartupTerminalHook(t, func(error) {})
	setStartupBeforeEagerHook(t, func() {
		go func() {
			c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
				SetClientID("first").SetConnectTimeout(10 * time.Second))
			c.Connect().WaitTimeout(10 * time.Second)
			once.Do(func() { close(refused) })
		}()
		select {
		case <-refused:
		case <-time.After(30 * time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := startWithin(t, s, ctx, 60*time.Second)
	if err == nil || !strings.Contains(err.Error(), "consumer already exists") {
		t.Fatalf("Start = %v, want the early CONNECT's refusal", err)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("%d initialization attempts, want 1: Start's own attempt ran after the CONNECT's refusal", n)
	}
}

// TestFailedStartJoinsTheLifecycleBeforeReturning: a failed Start returns only once the
// server's shutdown lifecycle has COMPLETED, however long that takes — here a CONNECT
// held inside its own initialization attempt holds the lifecycle's client join past
// ShutdownDrainTimeout, where the bounded Shutdown would already have returned. ~12 s.
func TestFailedStartJoinsTheLifecycleBeforeReturning(t *testing.T) {
	if testing.Short() {
		t.Skip("holds the lifecycle past ShutdownDrainTimeout")
	}
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	hold := ShutdownDrainTimeout + 2*time.Second

	// Start's attempt is refused; a CONNECT then drives an attempt of its own, which the
	// attempt seam holds — under the manager lock, on the CONNECT's read loop — for hold.
	var attempts atomic.Int32
	connectHeld := make(chan struct{})
	setStartupAttemptHook(t, func(*mqttJSA) {
		if attempts.Add(1) == 2 {
			close(connectHeld)
			time.Sleep(hold)
		}
	})
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		if isObserverCreate(ccr) && attempts.Load() == 1 {
			return &ApiError{Code: 400, ErrCode: 10148, Description: "consumer already exists"}
		}
		return err
	}))
	setObserverOccupantLookupHook(t, func(*ConsumerInfo, error) (*ConsumerInfo, error) {
		return &ConsumerInfo{Config: &ConsumerConfig{Metadata: map[string]string{mqttEphemeralRunKey: "another-run"}}}, nil
	})
	parked := make(chan struct{})
	releaseStart := make(chan struct{})
	setStartupTerminalHook(t, func(error) {
		close(parked)
		<-releaseStart
	})

	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
	done := make(chan error, 1)
	go func() { done <- s.Start(context.Background()) }()
	<-parked
	go func() {
		c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
			SetClientID("holder").SetConnectTimeout(60 * time.Second))
		c.Connect().WaitTimeout(60 * time.Second)
	}()
	select {
	case <-connectHeld:
	case <-time.After(30 * time.Second):
		t.Fatal("the CONNECT never drove an initialization attempt")
	}
	began := time.Now()
	close(releaseStart)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Start = nil, want the refusal")
		}
		if d := time.Since(began); d < hold-time.Second {
			t.Fatalf("Start returned %s after its failure, before the held lifecycle completed (~%s)", d, hold)
		}
	case <-time.After(hold + 60*time.Second):
		t.Fatal("Start did not return")
	}
}

// TestAFailedAttemptsSenderFinishesBeforeTheNextAttemptPublishes: an attempt whose
// CREATE sits in its sender past the request timeout fails; the next attempt publishes
// nothing until that sender has released the CREATE, so the late publication cannot land
// after the next attempt's definite delete.
func TestAFailedAttemptsSenderFinishesBeforeTheNextAttemptPublishes(t *testing.T) {
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	var mu sync.Mutex
	var parking atomic.Bool
	var released time.Time
	var earlyPublishes []string
	parked := make(chan struct{})
	setJSSendPublishHook(t, func(subject string) {
		if strings.Contains(subject, "CONSUMER.CREATE."+mqttRetainedMsgsStreamName) && parking.CompareAndSwap(false, true) {
			close(parked)
			// Past the 1 s request timeout and the first backoff.
			time.Sleep(2500 * time.Millisecond)
			mu.Lock()
			released = time.Now()
			mu.Unlock()
			return
		}
		mu.Lock()
		defer mu.Unlock()
		select {
		case <-parked:
			if released.IsZero() {
				earlyPublishes = append(earlyPublishes, subject)
			}
		default:
		}
	})
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = time.Second })
	if err := startWithin(t, s, context.Background(), 60*time.Second); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !parking.Load() {
		t.Fatal("the first observer CREATE never reached the publish seam: nothing was parked, the test proved nothing")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(earlyPublishes) > 0 {
		t.Fatalf("the next attempt published %v while the failed attempt's CREATE was still in its sender", earlyPublishes)
	}
	if released.IsZero() {
		t.Fatal("Start completed while the failed attempt's CREATE was still in its sender")
	}
}

// injectObserverOccupant creates, on a separate connection, the retained observer under
// name with owner as its run nonce and a delivery subject of an earlier generation — what
// an earlier attempt's CREATE leaves when it applies after the next attempt's delete.
func injectObserverOccupant(t *testing.T, url, name, owner string) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Errorf("inject connect: %v", err)
		return
	}
	defer nc.Close()
	ccr := mqttRetainedObserverConfig(name, mqttSubPrefix+"stale.rmsgs.0", owner, 0, 5*time.Second)
	ccr.Action = ActionCreate
	body, _ := json.Marshal(ccr)
	m, err := nc.Request(fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s", mqttRetainedMsgsStreamName, name), body, 5*time.Second)
	if err != nil {
		t.Errorf("inject create: %v", err)
		return
	}
	if strings.Contains(string(m.Data), `"error"`) {
		t.Errorf("inject create answered %s", m.Data)
	}
}

// TestStartRecognisesAnObserverCreateAppliedAfterTheNextDelete: an earlier attempt's
// CREATE applies after this attempt's definite delete. Its owner nonce is this run's, so it
// is recognised as a stray and reconciled by the next attempt; a foreign nonce stays a
// refusal; and an occupant lookup JetStream does not answer yet is retried without
// another delete in between.
func TestStartRecognisesAnObserverCreateAppliedAfterTheNextDelete(t *testing.T) {
	for _, tc := range []struct {
		name          string
		foreign       bool
		lookupStalls  int
		wantSucceeded bool
	}{
		{name: "this run's stray", wantSucceeded: true},
		{name: "this run's stray behind a stalled lookup", lookupStalls: 2, wantSucceeded: true},
		{name: "a foreign occupant", foreign: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := runJetStreamServerHA(t)
			url := ns.ClientURL()
			startupFastBackoff(t)
			s := startupTestServer(t, url, "node-a", "", "", nil)
			id, owner := s.ephemeralIdentity()
			name := mqttRetainedObserverName(id)
			if tc.foreign {
				owner = "another-run"
			}

			var mu sync.Mutex
			var events []string
			record := func(e string) { mu.Lock(); events = append(events, e); mu.Unlock() }
			var injected atomic.Bool
			setJSSendPublishHook(t, func(subject string) {
				if strings.Contains(subject, "CONSUMER.CREATE."+mqttRetainedMsgsStreamName) && injected.CompareAndSwap(false, true) {
					injectObserverOccupant(t, url, name, owner)
				}
				if strings.Contains(subject, "CONSUMER.DELETE."+mqttRetainedMsgsStreamName) {
					record("delete")
				}
			})
			var stalls atomic.Int32
			setObserverOccupantLookupHook(t, func(ci *ConsumerInfo, err error) (*ConsumerInfo, error) {
				record("lookup")
				if int(stalls.Add(1)) <= tc.lookupStalls {
					return nil, lostResponse()
				}
				return ci, err
			})

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := startWithin(t, s, ctx, 60*time.Second)
			if tc.wantSucceeded && err != nil {
				t.Fatalf("Start = %v, want the stray reconciled", err)
			}
			if !tc.wantSucceeded && (err == nil || !strings.Contains(err.Error(), "consumer already exists")) {
				t.Fatalf("Start = %v, want the refusal for a foreign occupant", err)
			}
			if tc.wantSucceeded {
				names := consumerNamesOn(t, url, mqttRetainedMsgsStreamName)
				if n := newNamesWithPrefix(map[string]bool{}, names, mqttRetainedMsgsStreamName+"_obs_"); n != 1 {
					t.Fatalf("%d observers, want 1 (names %v)", n, names)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			// Between the first and the last lookup of the stalled occupant, no delete.
			first, last := -1, -1
			for i, e := range events {
				if e == "lookup" {
					if first < 0 {
						first = i
					}
					last = i
				}
			}
			for i := first + 1; first >= 0 && i < last; i++ {
				if events[i] == "delete" {
					t.Fatalf("a delete ran while the occupant's ownership was unresolved: %v", events)
				}
			}
		})
	}
}

// TestStartupRetryableClassification: exactly the availability answers are retryable.
func TestStartupRetryableClassification(t *testing.T) {
	timeout := fmt.Errorf("create retained messages consumer for account %q: %w", "$G",
		&mqttJSATimeoutError{cause: errors.New("timeout after 5s")})
	wrap := func(err error) error { return fmt.Errorf("MQTT %s stream for account %q: %w", "x", "$G", err) }
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"a request timeout", timeout, true},
		{"a subscription flush timeout", fmt.Errorf("subscribe: %w", nats.ErrTimeout), true},
		{"a flush during reconnect", nats.ErrConnectionReconnecting, true},
		{"no responders", wrap(&ApiError{Code: 503}), true},
		{"a 503 carrying a JetStream code", wrap(&ApiError{Code: 503, ErrCode: 10039}), false},
		{"cluster unavailable", wrap(&ApiError{Code: 503, ErrCode: 10008}), true},
		{"stream offline", wrap(&ApiError{Code: 500, ErrCode: 10118}), true},
		{"consumer offline", wrap(&ApiError{Code: 500, ErrCode: 10119}), true},
		{"stream offline with a reason", wrap(&ApiError{Code: 500, ErrCode: 10194}), false},
		{"consumer offline with a reason", wrap(&ApiError{Code: 500, ErrCode: 10195}), false},
		{"consumer already exists", wrap(&ApiError{Code: 400, ErrCode: 10148}), false},
		{"this run's stray", fmt.Errorf("%w (%w)", errMQTTOwnEphemeralStray, &ApiError{Code: 400, ErrCode: 10148}), true},
		{"an occupant that is gone", fmt.Errorf("%w (%w)", errMQTTObserverOccupantGone, &ApiError{Code: 400, ErrCode: 10148}), true},
		{"a startup cancellation", fmt.Errorf("x: %w", errMQTTStartupCancelled), false},
		{"a permission violation", fmt.Errorf("x: %w", nats.ErrPermissionViolation), false},
		{"a plain refusal", errors.New("nonconforming stream"), false},
	} {
		if got := mqttStartupRetryable(tc.err); got != tc.want {
			t.Errorf("%s: retryable = %v, want %v (%v)", tc.name, got, tc.want, tc.err)
		}
	}
}

// TestOnlyARefusalBeforeAnyWriteIsDefinite: the sender ends startup on a publish error only
// when nats.go refused before writing anything; a write error or a reconnect-buffer
// overflow is as ambiguous as the timeout it ends in, and leaves startup retrying.
func TestOnlyARefusalBeforeAnyWriteIsDefinite(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nats.ErrMaxPayload, true},
		{nats.ErrBadSubject, true},
		{nats.ErrHeadersNotSupported, true},
		{fmt.Errorf("publish: %w", nats.ErrMaxPayload), true},
		{io.ErrClosedPipe, false},
		{nats.ErrReconnectBufExceeded, false},
		{nats.ErrConnectionReconnecting, false},
		{errors.New("write tcp: i/o timeout"), false},
	} {
		if got := mqttDefiniteLocalRefusal(tc.err); got != tc.want {
			t.Errorf("mqttDefiniteLocalRefusal(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// TestEveryJSATimeoutIsTheRequestTimeoutSentinel: the real wait paths — a plain request, an
// atomic batch and a store — all produce errors the startup classifier reads as
// retryable timeouts, with their messages unchanged.
func TestEveryJSATimeoutIsTheRequestTimeoutSentinel(t *testing.T) {
	ns := runJetStreamServerHA(t)
	// The short timeout is an option, never written into a live manager whose background
	// sweeps read it.
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = 500 * time.Millisecond })
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	asm, err := s.getOrCreateMQTTAccountSessionManager(s.createInternalAccountClient())
	if err != nil {
		t.Fatal(err)
	}
	view := asm.jsa
	drop := func(string, []byte) bool { return true }
	asm.jsa.testDropRequest.Store(&drop)
	t.Cleanup(func() { asm.jsa.testDropRequest.Store(nil) })

	_, rerr := view.newRequest(mqttJSAConsumerLookup, fmt.Sprintf(JSApiConsumerInfoT, mqttRetainedMsgsStreamName, "nobody"), 0, nil)
	_, berr := view.newAtomicBatchRequest(mqttJSAMsgStore, []mqttBatchMsg{{subj: mqttSessStreamSubjectPrefix + "x"}})
	_, serr := view.storeMsg(mqttSessStreamSubjectPrefix+"x", 0, []byte("{}"))
	for name, err := range map[string]error{"request": rerr, "atomic batch": berr, "store": serr} {
		if !errors.Is(err, errMQTTJSARequestTimeout) || !mqttStartupRetryable(err) {
			t.Errorf("%s timeout %v is not the retryable request timeout", name, err)
		}
		if err != nil && !strings.Contains(err.Error(), "timeout after") {
			t.Errorf("%s timeout message changed: %q", name, err)
		}
	}
}

func setServiceSubscriptionHook(t *testing.T, v func(subject string)) {
	setStartupSeam(t, &mqttTestServiceSubscriptionHookV, v)
}

func setStartupBeforeEagerHook(t *testing.T, v func()) {
	setStartupSeam(t, &mqttTestStartupBeforeEagerHookV, v)
}

// TestAConnectBeforeTheEagerInitializationObservesStartupCancellation: the startup window
// opens before the listener, so a CONNECT that drives an attempt before Start's own
// eager initialization begins observes the cancellation like every other attempt.
func TestAConnectBeforeTheEagerInitializationObservesStartupCancellation(t *testing.T) {
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	entered := make(chan struct{})
	var once sync.Once
	setStartupAttemptHook(t, func(jsa *mqttJSA) {
		drop := func(string, []byte) bool {
			once.Do(func() { close(entered) })
			return true
		}
		jsa.testDropRequest.Store(&drop)
	})
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = 15 * time.Second })
	setStartupBeforeEagerHook(t, func() {
		go func() {
			c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
				SetClientID("first").SetConnectTimeout(30 * time.Second))
			c.Connect().WaitTimeout(30 * time.Second)
		}()
		select {
		case <-entered:
		case <-time.After(30 * time.Second):
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the CONNECT never entered its initialization request")
	}
	cancelled := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, errMQTTStartupCancelled) {
			t.Fatalf("Start = %v, want the startup cancellation", err)
		}
		if d := time.Since(cancelled); d > 3*time.Second {
			t.Fatalf("Start returned %s after the cancellation: the early CONNECT's attempt did not observe it", d)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Start did not return")
	}
}

// TestStartupCancellationStopsBetweenServiceSubscriptions: a cancellation that lands
// while one service subscription is being made stops the attempt before the next one, so
// a signal costs at most the flush in progress.
func TestStartupCancellationStopsBetweenServiceSubscriptions(t *testing.T) {
	ns := runJetStreamServerHA(t)
	startupFastBackoff(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var subjects []string
	setServiceSubscriptionHook(t, func(subject string) {
		mu.Lock()
		subjects = append(subjects, subject)
		first := len(subjects) == 1
		mu.Unlock()
		if first {
			cancel()
			// Let the watcher close the cancellation before this subscription completes.
			time.Sleep(100 * time.Millisecond)
		}
	})
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", nil)
	if err := startWithin(t, s, ctx, 60*time.Second); !errors.Is(err, errMQTTStartupCancelled) {
		t.Fatalf("Start = %v, want the startup cancellation", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(subjects) != 1 {
		t.Fatalf("subscriptions made after the cancellation: %v, want only the one in progress", subjects)
	}
}

// TestAPermissionDenialInAConnectDrivenAttemptEndsStartup: during Start's backoff a CONNECT
// drives an attempt that meets a publication denial. That attempt's verdict ends startup;
// Start's next attempt must not take the recorded violation as its own baseline.
func TestAPermissionDenialInAConnectDrivenAttemptEndsStartup(t *testing.T) {
	ns := permissionServer(t, &natsserver.Permissions{
		Publish:   &natsserver.SubjectPermission{Allow: []string{">"}, Deny: []string{"forbidden.>"}},
		Subscribe: &natsserver.SubjectPermission{Allow: []string{">"}},
	})
	setStartupBackoff(t, 3*time.Second)
	var attempts atomic.Int32
	setStartupAttemptHook(t, func(*mqttJSA) { attempts.Add(1) })
	s := startupTestServer(t, ns.ClientURL(), "node-a", "svc", "svcpass", nil)
	// Exactly one denial, in the CONNECT's attempt: a later attempt that met one of its own
	// would end startup by itself and prove nothing about the CONNECT's verdict.
	var denied atomic.Bool
	t.Cleanup(setMQTTTestConsumerCreateResultHook(func(ccr *CreateConsumerRequest, err error) error {
		switch {
		case attempts.Load() == 1 && isObserverCreate(ccr):
			return lostResponse()
		case attempts.Load() == 2 && err == nil && isWatchCreate(ccr) && denied.CompareAndSwap(false, true):
			_ = s.nrc.Publish("forbidden.x", nil)
			_ = s.nrc.Flush()
		}
		return err
	}))
	var launched atomic.Bool
	setStartupBackoffHook(t, func(int, error) {
		if launched.CompareAndSwap(false, true) {
			go func() {
				c := pmqtt.NewClient(pmqtt.NewClientOptions().AddBroker("tcp://" + s.Addr().String()).
					SetClientID("during-backoff").SetConnectTimeout(10 * time.Second))
				c.Connect().WaitTimeout(10 * time.Second)
			}()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := startWithin(t, s, ctx, 60*time.Second)
	if err == nil || !errors.Is(err, nats.ErrPermissionViolation) {
		t.Fatalf("Start = %v, want the denial the CONNECT-driven attempt recorded", err)
	}
	if n := attempts.Load(); n < 2 {
		t.Fatalf("%d attempts, want the CONNECT's during the backoff", n)
	}
}

// TestStartFailsWhenTheServiceConnectionIsAtItsSubscriptionLimit: the server refuses the
// JSA reply subscription asynchronously while the flush succeeds. That refusal ends
// startup instead of surfacing as every request timing out forever.
func TestStartFailsWhenTheServiceConnectionIsAtItsSubscriptionLimit(t *testing.T) {
	ns := runStartupNATS(t, &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), MaxSubs: 1})
	startupFastBackoff(t)
	var backoffs atomic.Int32
	setStartupBackoffHook(t, func(int, error) { backoffs.Add(1) })
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = time.Second })
	// Something else on the service connection already holds its only subscription, as the
	// backend's JetStream probe can.
	if _, err := s.nrc.SubscribeSync("occupied.elsewhere"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := startWithin(t, s, ctx, 60*time.Second)
	if err == nil || !errors.Is(err, nats.ErrMaxSubscriptionsExceeded) {
		t.Fatalf("Start = %v, want the subscription-limit refusal", err)
	}
	if n := backoffs.Load(); n != 0 {
		t.Errorf("%d backoffs before the refusal, want 0", n)
	}
}

// TestADefinitePublishRefusalEndsStartupAfterItsWaiterGaveUp: the sender is held past the
// request's timeout, so the waiter has unregistered when the connection refuses the
// oversized request. The refusal still ends startup, with no retry.
func TestADefinitePublishRefusalEndsStartupAfterItsWaiterGaveUp(t *testing.T) {
	ns := runStartupNATS(t, &natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), MaxPayload: 256})
	startupFastBackoff(t)
	var backoffs atomic.Int32
	setStartupBackoffHook(t, func(int, error) { backoffs.Add(1) })
	var held atomic.Bool
	setJSSendPublishHook(t, func(subject string) {
		if strings.Contains(subject, "STREAM.CREATE.") && held.CompareAndSwap(false, true) {
			time.Sleep(2 * time.Second) // past the 1 s request timeout
		}
	})
	s := startupTestServer(t, ns.ClientURL(), "node-a", "", "", func(o *Options) { o.MQTT.JSAPITimeout = time.Second })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := startWithin(t, s, ctx, 60*time.Second)
	if err == nil || !errors.Is(err, nats.ErrMaxPayload) {
		t.Fatalf("Start = %v, want the max-payload refusal", err)
	}
	if !held.Load() {
		t.Fatal("no stream CREATE reached the publish seam: nothing was held, the test proved nothing")
	}
	if n := backoffs.Load(); n != 0 {
		t.Errorf("%d backoffs before the refusal, want 0: the held request's refusal was lost and startup retried", n)
	}
}
