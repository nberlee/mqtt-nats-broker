package mqtt

import (
	"errors"
	"testing"
)

// The MQTT listener speaks MQTT over TCP only. An HTTP request line arriving
// before CONNECT is a client pointed at the wrong protocol, and the parse error
// must say so rather than report the request's first byte as a packet type --
// while any other non-CONNECT first packet keeps the generic refusal, so the
// diagnostic cannot swallow ordinary protocol violations.
func TestMQTTHTTPRequestLineOnMQTTListenerIsNamed(t *testing.T) {
	httpClient := &client{mqtt: &mqtt{r: &mqttReader{}}}
	err := httpClient.mqttParse([]byte("GET /mqtt HTTP/1.1\r\nHost: broker\r\n\r\n"))
	if !errors.Is(err, errMQTTHTTPRequestOnMQTTListener) {
		t.Fatalf("an HTTP request line before CONNECT must be named as one, got %v", err)
	}

	pingClient := &client{mqtt: &mqtt{r: &mqttReader{}}}
	err = pingClient.mqttParse([]byte{mqttPacketPing, 0})
	if err == nil {
		t.Fatal("a PINGREQ before CONNECT must be refused")
	}
	if errors.Is(err, errMQTTHTTPRequestOnMQTTListener) {
		t.Fatalf("a non-HTTP first packet must keep the generic refusal, got %v", err)
	}
}
