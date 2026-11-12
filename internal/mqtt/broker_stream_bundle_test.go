package mqtt

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// testBundleBringUp wires a minimal Server plus internal account client against a
// live in-process JetStream, so a test can bring an account-session manager up (and
// bring a SECOND one up after tampering with the streams). The tagged full-suite has
// its own copy of this; the epoch coupling belongs to the default suite.
func testBundleBringUp(t *testing.T) (*Server, *client, *nats.Conn) {
	t.Helper()
	_, nc := runNATS(t)
	return testBundleBringUpOn(t, nc, "node1")
}

func testBundleBringUpOn(t *testing.T, nc *nats.Conn, node string) (*Server, *client, *nats.Conn) {
	t.Helper()
	s := testRigServer(t, nc, node)
	c := s.createInternalAccountClient()
	return s, c, nc
}

// testBundleJS is an administrative JetStream handle, for the one thing the
// broker's own API deliberately cannot do: delete a stream.
func testBundleJS(t *testing.T, nc *nats.Conn) (jetstream.JetStream, context.Context) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return js, ctx
}

// The broker-owned streams are ONE recreation unit, asserted from both directions: a
// bundle missing ANY single member is refused, and a complete bundle whose members
// disagree on the recovery epoch is refused too (issue #70), for every position.
func TestBrokerStreamBundleRefusesAPartialAccount(t *testing.T) {
	for _, missing := range mqttTestBrokerStreamNames {
		t.Run("without "+missing, func(t *testing.T) {
			s1, c1, nc := testBundleBringUp(t)
			asm1, err := s1.getOrCreateMQTTAccountSessionManager(c1)
			if err != nil {
				t.Fatalf("first bring-up: %v", err)
			}
			// Every member carries the adopted epoch: that stamp is what makes the
			// seven one unit, and a fixture whose premise is unchecked proves nothing.
			for _, name := range mqttTestBrokerStreamNames {
				si, lerr := asm1.jsa.lookupStream(name)
				if lerr != nil || si == nil {
					t.Fatalf("%s must be provisioned: %+v %v", name, si, lerr)
				}
				if got := si.Config.Metadata[mqttSessEpochMetaKey]; got != asm1.sessStreamEpoch {
					t.Fatalf("%s carries %q, want the adopted %q", name, got, asm1.sessStreamEpoch)
				}
			}

			js, ctx := testBundleJS(t, nc)
			if derr := js.DeleteStream(ctx, missing); derr != nil {
				t.Fatalf("delete %s: %v", missing, derr)
			}

			s2, c2, _ := testBundleBringUpOn(t, nc, "node2")
			// A deleted member stays deleted, so the rescans would only wait out their
			// budget before the same refusal; they are TestBrokerStreamBundlePreflightRescans'
			// subject, and one scan is this test's.
			s2.bundleSettleBudget = 0
			_, err = s2.getOrCreateMQTTAccountSessionManager(c2)
			if err == nil {
				t.Fatalf("an account holding six of the seven broker-owned streams must fail "+
					"initialization; %s was deleted and the rest were silently reprovisioned", missing)
			}
			// The PREFLIGHT's reason specifically. Without this the test passes on any
			// refusal at all — including a later epoch mismatch — which would make it
			// blind to the preflight being removed entirely.
			for _, want := range []string{"of the 7 broker-owned streams", "ONE recreation unit"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal must name %q, got: %v", want, err)
				}
			}
			// The two LISTS, not just the name somewhere in the message: the
			// refusal also enumerates all seven as the streams to delete, so a
			// bare Contains passes even when both vectors are empty.
			gotPresent, gotAbsent := testBundleRefusalLists(t, err)
			if len(gotAbsent) != 1 || gotAbsent[0] != missing {
				t.Errorf("the refusal's absent list is %v, want exactly [%s]", gotAbsent, missing)
			}
			if len(gotPresent) != len(mqttTestBrokerStreamNames)-1 {
				t.Errorf("the refusal's present list is %v, want the other six streams", gotPresent)
			}
			// And it must not have recreated what it refused: a preflight that creates
			// is the defect this whole mechanism exists to prevent. Checked through
			// the FIRST manager's connection, because the refused one published no
			// manager to look through — which is itself asserted here.
			if len(s2.mqtt.sessmgr.sessions) != 0 {
				t.Fatalf("a refused initialization must publish no manager, found %d", len(s2.mqtt.sessmgr.sessions))
			}
			if si, lerr := asm1.jsa.lookupStream(missing); si != nil {
				t.Fatalf("the refused startup recreated %s (err=%v)", missing, lerr)
			}
		})
	}
}

// A COMPLETE bundle in which one member carries a valid but FOREIGN epoch is
// refused by that stream's validator.
//
// This is the coverage a partial-set test cannot give: the preflight refuses a
// proper subset before any validator runs, so without this fixture the epoch
// comparison would never be exercised end to end at all.
func TestBrokerStreamBundleRefusesAForeignEpochMember(t *testing.T) {
	for _, foreign := range mqttTestBrokerStreamNames {
		if foreign == mqttSessStreamName {
			// $MQTT_sess MINTS the epoch, so "foreign" is not a state it can be in:
			// whatever well-formed value it carries becomes the account's. Its own
			// malformed-stamp arm is TestSessionManagerEpochMissingFailsInit.
			continue
		}
		t.Run(foreign+" from another incarnation", func(t *testing.T) {
			s, c, nc := testBundleBringUp(t)
			testBundleThenPoisonEpoch(t, s, c, nc, foreign,
				map[string]string{mqttSessEpochMetaKey: mqttGenSessEpoch()})

			s2, c2, _ := testBundleBringUpOn(t, nc, "node2")
			_, err := s2.getOrCreateMQTTAccountSessionManager(c2)
			if err == nil {
				t.Fatalf("%s carrying another incarnation's recovery epoch must fail initialization: "+
					"its records reference identities the rest of the bundle no longer issues", foreign)
			}
			for _, want := range []string{foreign, mqttSessEpochMetaKey, "different incarnation"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal must name %q, got: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), "of the 7 broker-owned streams") {
				t.Fatalf("the refusal came from the count rule, so the epoch comparison was never reached: %v", err)
			}
		})
	}
}
