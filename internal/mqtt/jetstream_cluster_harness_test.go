//go:build mqttport_fulltests

// Real replicated-JetStream cluster harness for the delayed-Will outbox tests.
//
// Unlike will_outbox_test.go — which models two brokers in front of ONE embedded
// JetStream by sharing a StoreDir — this harness stands up three genuine
// nats-server processes joined into one cluster, so the $MQTT_willq stream is
// created with Replicas=3 and its claim/publish/delete lifecycle can be exercised
// across a real Raft leader failover.
//
// It deliberately does NOT use the single-node pairing registry
// (testEmbeddedNATSByDir) or testMQTTShutdownServer: those own the 1:1
// shim-Server-to-embedded-nats model and their restart idiom would corrupt
// cluster state. Cluster nodes and cluster-backed brokers are torn down by their
// own t.Cleanup closures registered here.

package mqtt

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// testGlobalAccount is the nats-server default global account ("$G"), the account
// the MQTT streams live under when the embedded servers run without accounts.
const testGlobalAccount = "$G"

// testJSClusterNode is one embedded nats-server in the cluster.
type testJSClusterNode struct {
	name string
	ns   *natsserver.Server
	// opts is the pristine restart template. natsserver.NewServer mutates the
	// Options it is handed, so this is NEVER the value passed to NewServer — each
	// (re)start gets opts.Clone(). Its ServerName, StoreDir, cluster endpoint and
	// route list are fixed at creation; the bound client Port is stamped in after
	// first start so a restart re-binds the same endpoint.
	opts    *natsserver.Options
	running bool
}

// testJetStreamCluster is a set of embedded nats-server processes joined into one
// JetStream cluster. All methods run on the test goroutine (checkFor polls
// inline), so no locking is needed.
type testJetStreamCluster struct {
	name  string
	nodes []*testJSClusterNode
}

// testNewJetStreamCluster starts a size-node JetStream cluster and blocks until it
// has formed (full route mesh, a metadata leader, all nodes current). Cleanup is
// registered before the formation wait so a partial start is still torn down.
//
// Clustered JetStream requires every node (including the seed) to have configured
// routes — enableJetStreamClustering rejects a node with none. So cluster ports are
// reserved up front and every node is given the SAME full route list (a node's
// route to itself is ignored); this avoids the dynamic-port chicken-and-egg where
// a seed with Cluster.Port:-1 would have no route to advertise.
func testNewJetStreamCluster(t *testing.T, size int) *testJetStreamCluster {
	t.Helper()
	c := &testJetStreamCluster{name: "JSC-" + nuid.Next()}

	// One store root for the whole cluster, taken BEFORE c.cleanup is registered.
	// t.Cleanup is LIFO, so this ordering is what guarantees the directory removal
	// runs AFTER every node has been shut down. Calling t.TempDir() per node (which
	// registers its own removal later, and therefore runs earlier) deletes store
	// directories out from under still-running servers: JetStream keeps writing
	// during the removal and RemoveAll fails with "directory not empty" — a teardown
	// failure that looks like a test failure and hides real ones.
	root := t.TempDir()
	t.Cleanup(c.cleanup)

	var err error
	for attempt := 1; attempt <= testClusterStartAttempts; attempt++ {
		if err = c.startNodes(t, root, size); err == nil {
			c.waitFormed(t, size)
			return c
		}
		// A lost port is not a cluster that will ever form. Tear the partial one
		// down and start over with fresh ports. See startNodes.
		t.Logf("cluster %q start attempt %d/%d: %v", c.name, attempt, testClusterStartAttempts, err)
		c.cleanup()
		c.nodes = nil
	}
	t.Fatalf("cluster %q never started: %v", c.name, err)
	return nil
}

// testClusterStartAttempts bounds how many times a cluster start is retried when a
// node loses its reserved cluster port to another process.
const testClusterStartAttempts = 3

// startNodes reserves cluster ports and brings up size nodes on them.
//
// A port is only reserved until the reservation is closed, and the server that
// wants it does not bind until it starts, so there is always a window in which
// another process can take it. Losing it is silent: the node's client listener
// comes up, its cluster listener does not, and the node never reports ready. The
// window is kept as short as possible here — every reservation is held open until
// the moment its own server is started — and the caller retries with fresh ports
// if a node loses it anyway. (Reserving all the ports and only then constructing
// and starting three servers, as this used to, leaves the last node's reservation
// closed for as long as the first two take to start, which on a loaded machine is
// long enough to lose regularly.)
func (c *testJetStreamCluster) startNodes(t *testing.T, root string, size int) error {
	t.Helper()
	reservations := make([]net.Listener, size)
	routeURLs := make([]string, size)
	defer func() {
		for _, l := range reservations {
			if l != nil {
				_ = l.Close()
			}
		}
	}()
	for i := range reservations {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve free port: %v", err)
		}
		reservations[i] = l
		routeURLs[i] = fmt.Sprintf("nats://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port)
	}
	routes := natsserver.RoutesFromStr(strings.Join(routeURLs, ","))

	for i := 0; i < size; i++ {
		port := reservations[i].Addr().(*net.TCPAddr).Port
		// Release this node's own port immediately before it is started, so the
		// gap between "nobody holds it" and "this server holds it" is as narrow as
		// it can be. The others stay reserved meanwhile.
		_ = reservations[i].Close()
		reservations[i] = nil
		if err := c.newNode(t, root, port, routes); err != nil {
			return err
		}
	}
	return nil
}

// newNode creates, starts and registers one embedded nats-server bound to the given
// explicit cluster port, with the shared full route list.
func (c *testJetStreamCluster) newNode(t *testing.T, storeRoot string, clusterPort int, routes []*url.URL) error {
	t.Helper()
	name := "N-" + nuid.Next()
	// A subdirectory of the cluster-wide root rather than its own t.TempDir(), so
	// removal is ordered after node shutdown — see testNewJetStreamCluster.
	storeDir := filepath.Join(storeRoot, name)
	if err := os.MkdirAll(storeDir, 0o750); err != nil {
		t.Fatalf("create store dir for node %q: %v", name, err)
	}
	opts := &natsserver.Options{
		ServerName: name,
		Host:       "127.0.0.1",
		Port:       -1,
		JetStream:  true,
		StoreDir:   storeDir,
		NoSigs:     true, // three in-process servers must not each grab signal handlers
		NoLog:      true, // per-node current-state checks are the diagnostics, not logs
		Cluster: natsserver.ClusterOpts{
			Name: c.name,
			Host: "127.0.0.1",
			Port: clusterPort,
			// Disable route pooling: nats-server defaults PoolSize to
			// DEFAULT_ROUTE_POOL_SIZE, which would make NumRoutes()==size-1
			// non-deterministic. -1 gives exactly one route per peer.
			PoolSize: -1,
		},
		Routes: routes,
	}
	// Keep opts as the pristine restart template; hand NewServer a clone (it mutates
	// what it is given).
	ns, err := natsserver.NewServer(opts.Clone())
	if err != nil {
		t.Fatalf("create cluster node %q: %v", opts.ServerName, err)
	}
	// Register BEFORE Start so a readiness-timeout Fatal still lets c.cleanup stop
	// this server (t.Cleanup ran before the wait); otherwise it would leak.
	n := &testJSClusterNode{name: opts.ServerName, ns: ns, opts: opts, running: true}
	c.nodes = append(c.nodes, n)
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		// A nil cluster address with the client listener up means the cluster port
		// was taken between reservation and bind; the caller retries the cluster
		// with fresh ports. Returned rather than fatal for exactly that reason.
		return fmt.Errorf("cluster node %q not ready: running=%v addr=%v clusterAddr=%v",
			opts.ServerName, ns.Running(), ns.Addr(), ns.ClusterAddr())
	}
	// Stamp the bound client port into the template so a restart re-binds the same
	// endpoint (equivalence only — cluster rejoin/catch-up relies on the preserved
	// ServerName, cluster port and StoreDir, not on the client port).
	opts.Port = ns.Addr().(*net.TCPAddr).Port
	return nil
}

// restartNode brings a stopped node back with the SAME ServerName, cluster endpoint,
// route list and StoreDir, so JetStream restores it from its persisted state and it
// rejoins the existing cluster (state restoration, not literal peer identity — a
// fresh nkey is minted). Creates no new node entry, store dir, name or port. Callers
// follow with waitFormed + waitStreamCurrent to confirm rejoin and catch-up.
func (c *testJetStreamCluster) restartNode(t *testing.T, n *testJSClusterNode) {
	t.Helper()
	if n.running {
		t.Fatalf("restartNode: node %q is already running", n.name)
	}
	ns, err := natsserver.NewServer(n.opts.Clone())
	if err != nil {
		t.Fatalf("restart cluster node %q: %v", n.name, err)
	}
	// Mark running BEFORE Start so cluster cleanup owns a partially-started process.
	n.ns = ns
	n.running = true
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatalf("restarted node %q not ready: running=%v addr=%v clusterAddr=%v\n%s",
			n.name, ns.Running(), ns.Addr(), ns.ClusterAddr(), c.describe())
	}
}

// waitFormed blocks until every running node has size-1 routes, JetStream is
// current on all of them, and exactly one metadata leader exists.
func (c *testJetStreamCluster) waitFormed(t *testing.T, size int) {
	t.Helper()
	checkFor(t, 30*time.Second, 250*time.Millisecond, func() error {
		leaders := 0
		for _, n := range c.nodes {
			if !n.running {
				continue
			}
			if nr := n.ns.NumRoutes(); nr != size-1 {
				return fmt.Errorf("node %q has %d routes, want %d\n%s", n.name, nr, size-1, c.describe())
			}
			if !n.ns.JetStreamIsCurrent() {
				return fmt.Errorf("node %q JetStream not current\n%s", n.name, c.describe())
			}
			if n.ns.JetStreamIsLeader() {
				leaders++
			}
		}
		if leaders != 1 {
			return fmt.Errorf("want exactly 1 metadata leader, got %d\n%s", leaders, c.describe())
		}
		return nil
	})
}

// orderedURLs returns the running nodes' client URLs with the preferred node
// first, so a broker using nats.DontRandomize() places deterministically.
func (c *testJetStreamCluster) orderedURLs(preferred int) []string {
	var urls []string
	if preferred >= 0 && preferred < len(c.nodes) && c.nodes[preferred].running {
		urls = append(urls, c.nodes[preferred].ns.ClientURL())
	}
	for i, n := range c.nodes {
		if i == preferred || !n.running {
			continue
		}
		urls = append(urls, n.ns.ClientURL())
	}
	return urls
}

// streamLeader returns the node currently leading the stream, or nil if none.
func (c *testJetStreamCluster) streamLeader(account, stream string) *testJSClusterNode {
	for _, n := range c.nodes {
		if n.running && n.ns.JetStreamIsStreamLeader(account, stream) {
			return n
		}
	}
	return nil
}

// waitStreamLeader blocks until some running node leads the stream and returns it.
func (c *testJetStreamCluster) waitStreamLeader(t *testing.T, account, stream string) *testJSClusterNode {
	t.Helper()
	var ldr *testJSClusterNode
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		if ldr = c.streamLeader(account, stream); ldr == nil {
			return fmt.Errorf("no leader for stream %q yet\n%s", stream, c.describe())
		}
		return nil
	})
	return ldr
}

// waitStreamCurrent blocks until the stream is current on every running node.
func (c *testJetStreamCluster) waitStreamCurrent(t *testing.T, account, stream string) {
	t.Helper()
	checkFor(t, 20*time.Second, 200*time.Millisecond, func() error {
		for _, n := range c.nodes {
			if !n.running {
				continue
			}
			if !n.ns.JetStreamIsStreamCurrent(account, stream) {
				return fmt.Errorf("stream %q not current on node %q\n%s", stream, n.name, c.describe())
			}
		}
		return nil
	})
}

// stopNode shuts one node down and marks it stopped. Callers must never stop more
// than one node of a 3-node cluster at once, or JetStream loses quorum.
func (c *testJetStreamCluster) stopNode(n *testJSClusterNode) {
	if !n.running {
		return
	}
	n.ns.Shutdown()
	n.ns.WaitForShutdown()
	n.running = false
}

// describe returns a one-shot diagnostic snapshot for embedding in polling errors:
// per-node route count, JetStream-current, metadata-leader, and the
// leader/current state of every MQTT stream a cluster test can hinge on.
//
// $MQTT_msgs and $MQTT_qos2in are included because the QoS 2 move tests are
// otherwise undiagnosable on failure: "the subscriber saw two copies" is a
// symptom of a $MQTT_msgs leader change (the deduplication story) and "the
// terminal commit never landed" is a symptom of a $MQTT_qos2in one, and neither
// can be told apart from a plain timeout without this.
func (c *testJetStreamCluster) describe() string {
	streams := []string{mqttWillQStreamName, mqttSessStreamName, mqttStreamName,
		mqttQoS2IncomingMsgsStreamName, mqttInflightStreamName}
	var b strings.Builder
	fmt.Fprintf(&b, "cluster %q:", c.name)
	for _, n := range c.nodes {
		if !n.running {
			fmt.Fprintf(&b, "\n  node %q: STOPPED", n.name)
			continue
		}
		fmt.Fprintf(&b, "\n  node %q: routes=%d jsCurrent=%v metaLeader=%v",
			n.name, n.ns.NumRoutes(), n.ns.JetStreamIsCurrent(), n.ns.JetStreamIsLeader())
		for _, sn := range streams {
			fmt.Fprintf(&b, " %s{leader=%v current=%v}", sn,
				n.ns.JetStreamIsStreamLeader(testGlobalAccount, sn),
				n.ns.JetStreamIsStreamCurrent(testGlobalAccount, sn))
		}
	}
	return b.String()
}

// cleanup shuts every still-running node down in reverse start order. Idempotent.
func (c *testJetStreamCluster) cleanup() {
	for i := len(c.nodes) - 1; i >= 0; i-- {
		n := c.nodes[i]
		if !n.running {
			continue
		}
		n.ns.Shutdown()
		n.ns.WaitForShutdown()
		n.running = false
	}
}

// testClusterBroker is a standalone MQTT bridge connected to the cluster.
type testClusterBroker struct {
	s    *Server
	nc   *nats.Conn
	opts *Options
}

// startBroker starts an MQTT bridge whose NATS connection prefers the given node
// index (deterministic initial placement). The willq and other MQTT streams are
// created R3 (MQTT.StreamReplicas=3), which requires the cluster to be formed
// first. Cleanup is completion-aware and never touches the shared cluster.
func (c *testJetStreamCluster) startBroker(t *testing.T, preferred int, tweaks ...func(*Options)) *testClusterBroker {
	t.Helper()
	urls := strings.Join(c.orderedURLs(preferred), ",")
	nc, err := nats.Connect(urls,
		nats.DontRandomize(),   // honor the preferred-first ordering
		nats.MaxReconnects(-1), // survive node loss / failover
		nats.ReconnectWait(100*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("broker NATS connect: %v", err)
	}

	node := "bridge-" + nuid.Next()
	// Build on the v5 helper so cluster tests share the exact protocol config the
	// other v5 tests use, then set replicas and override the listener.
	opts := testMQTTDefaultOptionsV5()
	opts.MQTT.StreamReplicas = 3 // willq path reads this (sess_manager.go)
	opts.StreamReplicas = 3      // belt-and-suspenders
	// Applied before Start, so anything baked into consumers at creation time (the
	// AckWait a drain's redelivery waits out, say) takes effect.
	for _, tweak := range tweaks {
		tweak(opts)
	}

	s := NewServer(nc, opts, node)
	if err := s.Start(); err != nil {
		nc.Close()
		t.Fatalf("broker start: %v", err)
	}
	if err := s.readyForConnections(5 * time.Second); err != nil {
		<-s.BeginShutdown() // completion-aware: finish the drain before dropping nc
		nc.Close()
		t.Fatalf("broker not ready: %v", err)
	}
	// testMQTTConnectV5 dials host:port literally, so publish the bound port.
	opts.MQTT.Port = s.Addr().(*net.TCPAddr).Port

	b := &testClusterBroker{s: s, nc: nc, opts: opts}
	t.Cleanup(b.cleanup)
	return b
}

// cleanup awaits full shutdown completion BEFORE closing the NATS connection.
// Server.Shutdown() is only a bounded waiter that can return while the phased
// drain still uses nc; closing nc then would race teardown. BeginShutdown()'s
// channel closes only once every phase has finished with nc.
func (b *testClusterBroker) cleanup() {
	<-b.s.BeginShutdown()
	b.nc.Close()
}
