package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/nberlee/mqtt-nats-broker/internal/backend"
	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
	"github.com/nberlee/mqtt-nats-broker/internal/operational"
)

// /readyz's JetStream input (issue #188; docs/ARCHITECTURE.md, "The operational interface").
// Startup proves JetStream answered once; nothing re-checked it while the broker served, so a
// pod stayed ready through a meta layer catching up after a restart, and the Service and a
// rolling update kept treating it as able to serve. The broker now observes JetStream on its own
// schedule, and the probe handler only reads the last observation: the kubelet's one-second
// probe timeout is shorter than one JetStream request's.

// jetStreamReadinessInterval is the pause between the end of one observation and the start of
// the next. Rounds never overlap.
const jetStreamReadinessInterval = 2 * time.Second

// readinessCheckEmbeddedServer names the embedded server's own health in a failed probe.
const readinessCheckEmbeddedServer mqtt.ReadinessCheck = "embedded server health"

// embeddedServerHealthCheck is the embedded server's check the loop composes, read once by
// newJetStreamReadiness: a var so a test can make it fail through the real wiring.
var embeddedServerHealthCheck = embeddedServerHealth

// embeddedServerHealth is the embedded server's own health check with JSMetaOnly. Clustered, it
// is the server's view of its meta replica: a meta leader is known, the replica is current or
// catching up (raft Healthy), it has no write or apply error and is not recovering — a restarted
// peer's answer during the minute its meta layer falls behind, which the probe's requests,
// answered by leaders elsewhere, do not see. Standalone there is no meta layer, and the same
// check walks the store's accounts and streams instead: a stream that was not recovered or has a
// write error fails it, the broker's own or not, and the error names it.
func embeddedServerHealth(ns *server.Server) error {
	h := ns.Healthz(&server.HealthzOptions{JSMetaOnly: true})
	if h.StatusCode == http.StatusOK {
		return nil
	}
	if h.Error != "" {
		return errors.New(h.Error)
	}
	return fmt.Errorf("health status %d", h.StatusCode)
}

// jetStreamReadiness is the last completed observation of JetStream answering the broker.
type jetStreamReadiness struct {
	probe      func() error  // the core's probe, then the embedded server's health when embedded
	reconnects func() uint64 // the service connection's reconnect count
	now        func() time.Time
	interval   time.Duration
	// stale is how old a success may be: the interval plus the probe's two reply waits and slack.
	// An evaluation rejects an older success whatever the round in flight is blocked on.
	stale time.Duration
	log   *slog.Logger
	// beforeCommit runs between a round's probe and its commit. Nil in production.
	beforeCommit func()
	done         chan struct{}

	mu sync.Mutex
	// okAt is when the last successful round began; zero when the last round failed or none has
	// completed: unknown is not ready.
	okAt time.Time
	// okEpoch is the reconnect count the successful round began under: a success that began
	// before the connection's last reconnect describes another link.
	okEpoch uint64
	// failing is whether the last committed round failed; the records report its changes.
	failing bool
}

// newJetStreamReadiness is the one composition runContext uses: the core's probe through the
// account's assistant, and the embedded server's own health when there is one.
func newJetStreamReadiness(srv *mqtt.Server, be *backend.Backend, ns *server.Server, log *slog.Logger) *jetStreamReadiness {
	probe := srv.ProbeJetStream
	if ns != nil {
		meta := embeddedServerHealthCheck
		probe = func() error {
			if err := srv.ProbeJetStream(); err != nil {
				return err
			}
			if err := meta(ns); err != nil {
				return &mqtt.ReadinessProbeError{Check: readinessCheckEmbeddedServer, Err: err}
			}
			return nil
		}
	}
	return &jetStreamReadiness{
		probe:      probe,
		reconnects: func() uint64 { return be.Conn().Stats().Reconnects },
		now:        time.Now,
		interval:   jetStreamReadinessInterval,
		stale:      jetStreamReadinessInterval + 2*srv.JetStreamRequestTimeout() + time.Second,
		log:        log,
		done:       make(chan struct{}),
	}
}

// run observes until ctx ends, then closes done. A round that ends after the signal is
// discarded: shutdown is not an observation, and draining has its own record.
func (j *jetStreamReadiness) run(ctx context.Context) {
	defer close(j.done)
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if ctx.Err() != nil {
			return
		}
		epoch := j.reconnects()
		// The read above can wait on the connection's mutex through a reconnect.
		if ctx.Err() != nil {
			return
		}
		// A round's evidence dates from just before its requests, after the reconnect count it is
		// judged under was read: a call that stalls after the requests were answered must not make
		// their answers look newer, and a reconnect during that read must not pair a new count
		// with an older start.
		started := j.now()
		err := j.probe()
		if j.beforeCommit != nil {
			j.beforeCommit()
		}
		if !j.commit(ctx, started, epoch, err) {
			return
		}
		t.Reset(j.interval)
	}
}

// commit stores a round's outcome and reports a change of outcome, unless ctx has ended. The
// record is written after the mutex is released, so a slow log never holds /readyz; the loop is
// the only writer, so the records keep the rounds' order.
func (j *jetStreamReadiness) commit(ctx context.Context, started time.Time, epoch uint64, err error) bool {
	j.mu.Lock()
	if ctx.Err() != nil {
		j.mu.Unlock()
		return false
	}
	changed := j.failing != (err != nil)
	j.failing = err != nil
	if err == nil {
		j.okAt, j.okEpoch = started, epoch
	} else {
		// A failure withdraws at once, whatever the age of the last success.
		j.okAt = time.Time{}
	}
	j.mu.Unlock()
	switch {
	case !changed:
	case err == nil:
		operational.JetStreamProbeSucceeded.Log(j.log)
	default:
		check := ""
		var pe *mqtt.ReadinessProbeError
		if errors.As(err, &pe) {
			check = string(pe.Check)
		}
		operational.JetStreamProbeFailed.Log(j.log, operational.Str(check), operational.Err(err))
	}
	return true
}

// answering is /readyz's read: a success, begun under the connection's current reconnect
// count, no older than stale. The mutex is released before the reconnect count is read, so
// the handler never waits on a round; the count itself takes nats.go's connection mutex, as
// IsConnected does.
func (j *jetStreamReadiness) answering() bool {
	j.mu.Lock()
	okAt, epoch := j.okAt, j.okEpoch
	j.mu.Unlock()
	return !okAt.IsZero() && epoch == j.reconnects() && j.now().Sub(okAt) <= j.stale
}
