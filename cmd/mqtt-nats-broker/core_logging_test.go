package main

import (
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The MQTT core's records are the broker's records: they go to the one JSON stream on
// stdout at -log-level, never to Go's default text logger on stderr. These run the real
// main() as a process (embedded mode, so no external NATS) and read both streams apart.

// Core-originated records the tests drive. Each is written by internal/mqtt through
// Server.logger(), not by the command's own logger.
const (
	coreStreamsRecord     = "Creating MQTT streams/consumers with replicas" // Info, account manager init
	coreConnCreatedRecord = "Client connection created"                     // Debug, every accepted connection
	coreWSRefusedRecord   = "MQTT over WebSocket:"                          // Error, a refused upgrade
)

func startLoggingBroker(t *testing.T, level string, extra ...string) *brokerProcess {
	t.Helper()
	env := []string{"MQTTNB_NATS_USER=svc", "MQTTNB_NATS_PASS=svcpass", "MQTTNB_DEFAULT_USER=anon", "MQTTNB_DEFAULT_PASS=anonpass"}
	conf := writeConf(t, "listen: 127.0.0.1:-1\n"+jetStreamBlock(t.TempDir())+twoUserAccount)
	args := append([]string{"-nats-embedded-config", conf, "-log-level", level}, extra...)
	p := startBrokerProcess(t, env, args...)
	waitFor(t, 30*time.Second, func() bool { return p.readyz() == http.StatusOK })
	return p
}

// stop signals the broker and waits for it, after which stderr is complete.
func (p *brokerProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := p.waitExit(t, 60*time.Second); code != 0 {
		t.Fatalf("broker exited %d; stderr:\n%s", code, p.stderr.String())
	}
}

// record is the first stdout JSON record whose msg contains fragment, or nil.
func (p *brokerProcess) record(fragment string) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.lines {
		if msg, _ := m["msg"].(string); strings.Contains(msg, fragment) {
			return m
		}
	}
	return nil
}

func (p *brokerProcess) requireOneJSONStream(t *testing.T, absent ...string) {
	t.Helper()
	p.mu.Lock()
	nonJSON := append([]string(nil), p.nonJSON...)
	p.mu.Unlock()
	if len(nonJSON) != 0 {
		t.Errorf("stdout carries non-JSON lines: %q", nonJSON)
	}
	stderr := p.stderr.String()
	for _, frag := range absent {
		if strings.Contains(stderr, frag) {
			t.Errorf("core record %q went to stderr:\n%s", frag, stderr)
		}
	}
}

func TestCoreLogsFollowLogLevelOnStdout(t *testing.T) {
	t.Run("debug", func(t *testing.T) {
		p := startLoggingBroker(t, "debug")
		c, err := net.Dial("tcp", p.mqttAddr)
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 10*time.Second, func() bool { return p.record(coreConnCreatedRecord) != nil })
		c.Close()
		p.stop(t)

		for frag, level := range map[string]string{coreStreamsRecord: "INFO", coreConnCreatedRecord: "DEBUG"} {
			r := p.record(frag)
			if r == nil {
				t.Errorf("no core record %q on stdout at -log-level debug", frag)
				continue
			}
			if r["level"] != level {
				t.Errorf("core record %q has level %v, want %s", frag, r["level"], level)
			}
		}
		p.requireOneJSONStream(t, coreStreamsRecord, coreConnCreatedRecord)
	})

	t.Run("error", func(t *testing.T) {
		wsAddr := reservedAddr(t)
		p := startLoggingBroker(t, "error", "-ws-port", portOf(t, wsAddr))
		c, err := net.Dial("tcp", p.mqttAddr)
		if err != nil {
			t.Fatal(err)
		}
		// A WebSocket upgrade that offers no "mqtt" subprotocol is refused by the core.
		req, _ := http.NewRequest(http.MethodGet, "http://"+wsAddr+"/mqtt", nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		if resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req); err == nil {
			resp.Body.Close()
		}
		waitFor(t, 10*time.Second, func() bool { return p.record(coreWSRefusedRecord) != nil })
		c.Close()
		p.stop(t)

		if r := p.record(coreWSRefusedRecord); r["level"] != "ERROR" {
			t.Errorf("core record %q has level %v, want ERROR", coreWSRefusedRecord, r["level"])
		}
		for _, frag := range []string{coreStreamsRecord, coreConnCreatedRecord} {
			if p.record(frag) != nil {
				t.Errorf("core record %q on stdout at -log-level error", frag)
			}
			if strings.Contains(p.stderr.String(), frag) {
				t.Errorf("core record %q on stderr at -log-level error", frag)
			}
		}
		p.requireOneJSONStream(t, coreWSRefusedRecord)
	})
}
