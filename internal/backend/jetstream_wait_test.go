package backend

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/nberlee/mqtt-nats-broker/internal/config"
)

// answeringJetStream answers AccountInfo with err after ending the caller's context, so the
// wait sees the answer and the stop together.
type answeringJetStream struct {
	jetstream.JetStream
	stop context.CancelFunc
	err  error
}

func (j answeringJetStream) AccountInfo(context.Context) (*jetstream.AccountInfo, error) {
	j.stop()
	return nil, j.err
}

// TestTheJetStreamWaitKeepsARefusalOverACancellation: a definite refusal that arrives
// with the stop is the refusal (startup fails, exit 1), and a request the stop cut short
// is the cancellation (a requested stop, exit 0): the wait judges the answer first.
func TestTheJetStreamWaitKeepsARefusalOverACancellation(t *testing.T) {
	cfg := &config.Config{JSTimeout: time.Second}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"a refusal", jetstream.ErrJetStreamNotEnabledForAccount},
		{"a cancelled request", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := waitJetStream(ctx, cfg, nil, answeringJetStream{stop: cancel, err: tc.err}, slog.New(slog.DiscardHandler))
			if !errors.Is(err, tc.err) {
				t.Fatalf("waitJetStream = %v, want %v", err, tc.err)
			}
			if tc.err != context.Canceled && errors.Is(err, context.Canceled) {
				t.Fatalf("waitJetStream = %v: the refusal was replaced by the cancellation", err)
			}
		})
	}
}
