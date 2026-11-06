//go:build mqtt_fulltests

package mqtt

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// jsRawUpdateStream rewrites a stream's configuration through the raw JS API: the
// out-of-band edit an operator, or another release, could make to the stamp.
func jsRawUpdateStream(t *testing.T, nc *nats.Conn, cfg *StreamConfig) {
	t.Helper()
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal stream config: %v", err)
	}
	msg, err := nc.Request(fmt.Sprintf(server.JSApiStreamUpdateT, cfg.Name), b, 5*time.Second)
	if err != nil {
		t.Fatalf("update stream %s: %v", cfg.Name, err)
	}
	var resp server.JSApiStreamUpdateResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("unmarshal update response: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("update stream %s: %+v", cfg.Name, resp.Error)
	}
}

// TestStartupRefusesABundleOfAnotherStoreFormat: against a real server, a complete
// bundle a broker provisioned and then left — its members restamped with another
// format, or with the stamp removed — fails the next broker's initialization with
// the format verdict, and that refusal neither creates nor modifies a stream.
func TestStartupRefusesABundleOfAnotherStoreFormat(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		stamp func(map[string]string)
		want  string
	}{
		{"another format", func(m map[string]string) { m[mqttStoreFormatMetaKey] = "2" }, "Run a broker release"},
		{"no stamp", func(m map[string]string) { delete(m, mqttStoreFormatMetaKey) }, "carry no"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s, c, nc := newBringUpServer(t)
			if _, err := s.getOrCreateMQTTAccountSessionManager(c); err != nil {
				t.Fatalf("bringing the bundle up: %v", err)
			}
			testStopBringUpServer(t, s)
			for _, name := range mqttTestBrokerStreamNames {
				cfg := jsRawStreamConfig(t, nc, name)
				if cfg.Metadata[mqttStoreFormatMetaKey] != mqttStoreFormat {
					t.Fatalf("%s was provisioned without this build's format stamp: %v", name, cfg.Metadata)
				}
				tc.stamp(cfg.Metadata)
				jsRawUpdateStream(t, nc, cfg)
			}
			before := map[string]string{}
			for _, name := range mqttTestBrokerStreamNames {
				b, _ := json.Marshal(jsRawStreamConfig(t, nc, name))
				before[name] = string(b)
			}

			s2, c2, _ := newBringUpServerOn(t, nc, "node2")
			_, err := s2.getOrCreateMQTTAccountSessionManager(c2)
			if err == nil {
				t.Fatal("a bundle in another persisted format must fail initialization")
			}
			if !strings.Contains(err.Error(), mqttStoreFormatMetaKey) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the refusal is not the persisted-format verdict (%q): %v", tc.want, err)
			}
			if got := testStreamNames(t, nc); len(got) != len(mqttTestBrokerStreamNames) {
				t.Fatalf("the refusal changed the account's streams: %v", got)
			}
			for _, name := range mqttTestBrokerStreamNames {
				b, _ := json.Marshal(jsRawStreamConfig(t, nc, name))
				if string(b) != before[name] {
					t.Fatalf("the refusal modified %s:\nbefore %s\nafter  %s", name, before[name], b)
				}
			}
		})
	}
}
