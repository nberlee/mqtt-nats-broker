//go:build mqtt_fulltests

package mqtt

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The backup procedure in docs/OPERATIONS.md is written as natscli commands, so its
// commands are run as written: `nats backup stream` for each of the seven, `nats backup
// validate`, `nats stream rm`, `nats backup restore stream`. The other tests in this
// family speak the same JetStream API directly; this one proves the documented commands,
// their backup directories and their validation reach the same result.
//
// It runs when MQTTNB_NATSCLI names a nats binary (CI installs the version the
// documentation names) and is skipped otherwise; a configured binary that does not run
// fails it.
func TestBackupRestoreWithNatsCLI(t *testing.T) {
	cli := os.Getenv("MQTTNB_NATSCLI")
	if cli == _EMPTY_ {
		t.Skip("set MQTTNB_NATSCLI to a nats (natscli) binary to run the documented backup commands")
	}
	t.Parallel()
	ns, nc, _ := testBackupNATS(t, testBackupStoreDir(t))
	js, _ := nc.JetStream()
	home := t.TempDir()
	nats := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(cli, append([]string{"--server", ns.ClientURL()}, args...)...)
		// natscli keeps contexts under the user's configuration directory: a private one
		// keeps the test from reading or writing the operator's.
		cmd.Env = append(os.Environ(), "HOME="+home, "XDG_CONFIG_HOME="+home)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("nats %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	if v := nats("--version"); !strings.HasPrefix(strings.TrimSpace(v), "v0.") {
		t.Fatalf("MQTTNB_NATSCLI does not run as natscli: %q", v)
	}

	a := testBackupBroker(t, ns, "cli-a")
	st := testBackupBuildState(t, a)
	a.stop()
	testBackupWillPending(t, js)
	// Unlike the API test, every member is captured at once: wait for the expiring
	// source first, so the restore does not depend on how long the commands take.
	_, seq := testBackupInflightRegister(t, js, st.expPI)
	checkFor(t, 3*testBackupExpiry*time.Second, 100*time.Millisecond, func() error {
		if testBackupSourcePresent(js, seq) {
			return fmt.Errorf("source %d not expired yet", seq)
		}
		return nil
	})

	dir := t.TempDir()
	for _, name := range mqttTestBrokerStreamNames {
		target := filepath.Join(dir, name)
		nats("backup", "stream", name, target, "--no-progress")
		nats("backup", "validate", target, "--no-progress")
	}
	time.Sleep(time.Until(st.willAt.Add(testBackupWillDelay*time.Second + time.Second)))

	for _, name := range mqttTestBrokerStreamNames {
		nats("stream", "rm", name, "-f")
	}
	for _, name := range mqttTestBrokerStreamNames {
		nats("backup", "restore", "stream", filepath.Join(dir, name), "--no-progress")
	}

	testBackupServesTheCapturedState(t, testBackupBroker(t, ns, "cli-b"), st)
}
