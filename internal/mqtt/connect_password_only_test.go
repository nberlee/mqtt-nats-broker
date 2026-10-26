//go:build mqtt_fulltests

package mqtt

import (
	"net"
	"strconv"
	"testing"
)

// TestMQTTV5PasswordWithoutUserNameIsRefused pins a stated deviation
// (docs/CONFORMANCE.md, "A password without a user name"): MQTT 5.0 [3.1.2.9] allows
// the Password flag without the User Name flag, and the broker answers that CONNECT
// as malformed, because auth passthrough has no password-only form and accepting it
// would hand the client the default identity while it presented a credential.
//
// The refused packet is complete and otherwise valid, so the refusal cannot come from
// truncation; the control is the same packet with a user name, which is admitted.
func TestMQTTV5PasswordWithoutUserNameIsRefused(t *testing.T) {
	t.Parallel()
	_, addr := startAuthBrokerTuned(t, nil, nil)
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	c, r := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pw-only", cleanStart: true, pass: "pw"}, host, port)
	defer c.Close()
	if _, rc, _ := testMQTTReadConnAckV5(t, r); rc != mqttReasonMalformedPacket {
		t.Fatalf("a password without a user name got CONNACK 0x%02x, want 0x%02x (Malformed Packet)", rc, mqttReasonMalformedPacket)
	}
	testMQTTExpectClosed(t, c, "the connection after the malformed-packet CONNACK")

	ok, okr := testMQTTConnectV5(t, &mqttV5ConnInfo{clientID: "pw-only", cleanStart: true, user: "alice", pass: "pw"}, host, port)
	defer ok.Close()
	if _, rc, _ := testMQTTReadConnAckV5(t, okr); rc != mqttReasonSuccess {
		t.Fatalf("the same CONNECT with a user name got CONNACK 0x%02x, want success", rc)
	}
}
