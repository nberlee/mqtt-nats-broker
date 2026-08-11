package mqtt

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	pmqtt "github.com/eclipse/paho.mqtt.golang"
)

// mqttStr encodes a length-prefixed MQTT UTF-8 string.
func mqttStr(s string) []byte {
	b := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(b, uint16(len(s)))
	copy(b[2:], s)
	return b
}

// buildConnectWithWill builds a minimal MQTT 3.1.1 CONNECT packet with a QoS0
// will (clean session).
func buildConnectWithWill(clientID, willTopic, willMsg string) []byte {
	var vh []byte
	vh = append(vh, mqttStr("MQTT")...) // protocol name
	vh = append(vh, 0x04)               // protocol level 4 (3.1.1)
	vh = append(vh, 0x06)               // flags: clean session (0x02) | will (0x04)
	vh = append(vh, 0x00, 0x3c)         // keep-alive 60s

	var payload []byte
	payload = append(payload, mqttStr(clientID)...)
	payload = append(payload, mqttStr(willTopic)...)
	payload = append(payload, mqttStr(willMsg)...)

	body := append(vh, payload...)
	pkt := []byte{0x10} // CONNECT
	pkt = append(pkt, encodeRemLen(len(body))...)
	pkt = append(pkt, body...)
	return pkt
}

func encodeRemLen(n int) []byte {
	var out []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			return out
		}
	}
}

// TestProtocolWillUngraceful verifies that when a client with a Will drops its
// TCP connection ungracefully (no DISCONNECT), the broker publishes the Will.
// This exercises the readLoop -> mqttHandleClosedClient path.
func TestProtocolWillUngraceful(t *testing.T) {
	t.Parallel()
	broker, _ := startBroker(t)

	// Subscriber watching the will topic.
	watcher := connectClient(t, broker, "will-watcher")
	got := make(chan string, 1)
	tok := watcher.Subscribe("will/topic", 1, func(_ pmqtt.Client, m pmqtt.Message) {
		got <- string(m.Payload())
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}

	// Raw client: CONNECT with a will, read the CONNACK, then abruptly close.
	conn, err := net.Dial("tcp", strings.TrimPrefix(broker, "tcp://"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := conn.Write(buildConnectWithWill("will-client", "will/topic", "client-died")); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	// Read CONNACK (4 bytes: 0x20 0x02 sp rc).
	ack := make([]byte, 4)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(ack); err != nil {
		t.Fatalf("read CONNACK: %v", err)
	}
	if ack[0] != 0x20 || ack[3] != 0x00 {
		t.Fatalf("unexpected CONNACK: %v", ack)
	}
	// Ungraceful drop: close the socket without sending DISCONNECT.
	_ = conn.Close()

	select {
	case msg := <-got:
		if msg != "client-died" {
			t.Fatalf("will payload = %q, want client-died", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("will was not delivered after ungraceful disconnect")
	}
}
