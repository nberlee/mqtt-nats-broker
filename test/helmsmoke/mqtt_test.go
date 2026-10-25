package helmsmoke

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// client is one MQTT 5.0 connection subscribed QoS 1 to its own topic.
type client struct {
	c     *paho.Client
	topic string
	got   chan *paho.Publish
}

// dial connects clientID to addr (over TLS when cfg is set) and subscribes to topic.
// The caller closes it.
func dial(addr, clientID, topic string, cfg *tls.Config) (*client, error) {
	var conn net.Conn
	var err error
	if cfg != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
	} else {
		conn, err = net.DialTimeout("tcp", addr, 5*time.Second)
	}
	if err != nil {
		return nil, err
	}
	cl := &client{topic: topic, got: make(chan *paho.Publish, 16)}
	cl.c = paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(pr paho.PublishReceived) (bool, error) {
			cl.got <- pr.Packet
			return true, nil
		}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ca, err := cl.c.Connect(ctx, &paho.Connect{ClientID: clientID, CleanStart: true, KeepAlive: 60})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	if ca.ReasonCode != 0 {
		conn.Close()
		return nil, fmt.Errorf("CONNACK reason %#x", ca.ReasonCode)
	}
	if _, err := cl.c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: topic, QoS: 1}}}); err != nil {
		cl.close()
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	return cl, nil
}

func (cl *client) close() { _ = cl.c.Disconnect(&paho.Disconnect{}) }

// roundTrip publishes payload QoS 1 (retained when retain) and waits for it to come back.
func (cl *client) roundTrip(payload string, retain bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := cl.c.Publish(ctx, &paho.Publish{Topic: cl.topic, QoS: 1, Retain: retain, Payload: []byte(payload)}); err != nil {
		return fmt.Errorf("publish %q: %w", payload, err)
	}
	return cl.await(ctx, payload, false)
}

// await waits for payload; retained requires the delivery to carry the retain flag (a
// retained message delivered on subscribe).
func (cl *client) await(ctx context.Context, payload string, retained bool) error {
	for {
		select {
		case p := <-cl.got:
			if string(p.Payload) == payload && (!retained || p.Retain) {
				return nil
			}
		case <-ctx.Done():
			return fmt.Errorf("%q never arrived (retained=%v)", payload, retained)
		}
	}
}

// roundTripOnce dials, round-trips one QoS 1 message, and disconnects.
func roundTripOnce(addr, clientID string, cfg *tls.Config) error {
	cl, err := dial(addr, clientID, "smoke/"+clientID, cfg)
	if err != nil {
		return err
	}
	defer cl.close()
	return cl.roundTrip("hello "+clientID, false)
}

// awaitRetained subscribes to topic and requires payload as a retained delivery.
func awaitRetained(addr, clientID, topic, payload string) error {
	cl, err := dial(addr, clientID, topic, nil)
	if err != nil {
		return err
	}
	defer cl.close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return cl.await(ctx, payload, true)
}

var errNotYet = errors.New("not yet")

// routeRefused connects to a NATS route port as a router presenting no credentials and
// requires the server to refuse it: reaching the port must not be joining the cluster.
func routeRefused(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	info, err := r.ReadString('\n')
	if err != nil || !strings.HasPrefix(info, "INFO ") {
		return fmt.Errorf("no route INFO: %q %v", info, err)
	}
	if _, err := conn.Write([]byte("CONNECT {\"verbose\":false,\"pedantic\":false,\"name\":\"intruder\"}\r\nPING\r\n")); err != nil {
		return err
	}
	for {
		line, err := r.ReadString('\n')
		switch {
		case strings.Contains(line, "Authorization Violation"):
			return nil
		case strings.HasPrefix(line, "PONG"):
			return fmt.Errorf("an unauthenticated router was admitted")
		case err != nil:
			return fmt.Errorf("no refusal before the connection ended: %v", err)
		}
	}
}
