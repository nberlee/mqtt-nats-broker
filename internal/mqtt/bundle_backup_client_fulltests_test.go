//go:build mqtt_fulltests

package mqtt

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/klauspost/compress/s2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// The backup procedure in docs/OPERATIONS.md is natscli's `nats backup stream` and
// `nats backup restore stream`. Those commands are clients of two JetStream API
// endpoints, and this file is a second client of the same two, so the tests exercise
// the server side an operator's backup actually goes through: the archive format, the
// consumers it carries, and the restore's activation of what it received.

// testStreamArchive is one member of a backup set: what the snapshot request answered
// (the configuration and state the restore request carries back) and the archive.
type testStreamArchive struct {
	name   string
	config natsserver.StreamConfig
	state  natsserver.StreamState
	data   []byte
}

const testBackupAPITimeout = 30 * time.Second

// testSnapshotStream takes a snapshot of one stream over $JS.API.STREAM.SNAPSHOT:
// chunks arrive on a private subject, each acknowledged on its own reply subject so
// the server's flow-control window advances, and an empty message ends it.
func testSnapshotStream(t testing.TB, nc *nats.Conn, name string, noConsumers bool) testStreamArchive {
	t.Helper()
	deliver := nats.NewInbox()
	chunks := make(chan *nats.Msg, 1024)
	sub, err := nc.ChanSubscribe(deliver, chunks)
	if err != nil {
		t.Fatalf("snapshot %s: subscribe: %v", name, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	req, _ := json.Marshal(natsserver.JSApiStreamSnapshotRequest{
		DeliverSubject: deliver,
		NoConsumers:    noConsumers,
		ChunkSize:      64 * 1024,
	})
	resp, err := nc.Request(fmt.Sprintf(natsserver.JSApiStreamSnapshotT, name), req, testBackupAPITimeout)
	if err != nil {
		t.Fatalf("snapshot %s: request: %v", name, err)
	}
	var sr natsserver.JSApiStreamSnapshotResponse
	if err := json.Unmarshal(resp.Data, &sr); err != nil {
		t.Fatalf("snapshot %s: response %q: %v", name, resp.Data, err)
	}
	if sr.Error != nil {
		t.Fatalf("snapshot %s: %v", name, sr.Error)
	}
	a := testStreamArchive{name: name, config: *sr.Config, state: *sr.State}
	var buf bytes.Buffer
	deadline := time.After(testBackupAPITimeout)
	for {
		select {
		case m := <-chunks:
			if len(m.Data) == 0 {
				// The end of the snapshot. Its status is 204 once a full chunk was sent;
				// an archive shorter than one chunk ends with no header at all
				// (jetstream_api.go, streamSnapshot), and an error status names the failure.
				if st := m.Header.Get("Status"); st != _EMPTY_ && st != "204" {
					t.Fatalf("snapshot %s ended with status %s %s", name, st, m.Header.Get("Description"))
				}
				a.data = buf.Bytes()
				return a
			}
			buf.Write(m.Data)
			if m.Reply != _EMPTY_ {
				if err := nc.Publish(m.Reply, nil); err != nil {
					t.Fatalf("snapshot %s: chunk ack: %v", name, err)
				}
			}
		case <-deadline:
			t.Fatalf("snapshot %s: no end of snapshot within %v (%d bytes so far)", name, testBackupAPITimeout, buf.Len())
		}
	}
}

// testSnapshotBundle snapshots all seven broker-owned streams, in the provisioning
// order, with their consumers.
func testSnapshotBundle(t testing.TB, nc *nats.Conn) map[string]testStreamArchive {
	t.Helper()
	set := make(map[string]testStreamArchive, len(mqttTestBrokerStreamNames))
	for _, name := range mqttTestBrokerStreamNames {
		set[name] = testSnapshotStream(t, nc, name, false)
	}
	return set
}

// testRestoreStream restores one archive over $JS.API.STREAM.RESTORE: the request
// carries the snapshot's configuration and state and answers with a subject the
// archive is sent to in request/reply chunks, and an empty chunk ends it; that last
// reply is the restore's outcome. It returns the server's verdict rather than failing,
// because a failed restore is itself a case under test.
func testRestoreStream(nc *nats.Conn, a testStreamArchive, data []byte) error {
	req, _ := json.Marshal(natsserver.JSApiStreamRestoreRequest{Config: a.config, State: a.state})
	resp, err := nc.Request(fmt.Sprintf(natsserver.JSApiStreamRestoreT, a.name), req, testBackupAPITimeout)
	if err != nil {
		return fmt.Errorf("restore %s: request: %w", a.name, err)
	}
	var rr natsserver.JSApiStreamRestoreResponse
	if err := json.Unmarshal(resp.Data, &rr); err != nil {
		return fmt.Errorf("restore %s: response %q: %w", a.name, resp.Data, err)
	}
	if rr.Error != nil {
		return fmt.Errorf("restore %s: %v", a.name, rr.Error)
	}
	const chunk = 64 * 1024
	for off := 0; off < len(data); off += chunk {
		end := min(off+chunk, len(data))
		reply, err := nc.Request(rr.DeliverSubject, data[off:end], testBackupAPITimeout)
		if err != nil {
			return fmt.Errorf("restore %s: chunk at %d: %w", a.name, off, err)
		}
		if len(reply.Data) > 0 {
			if err := testJSAPIResponseError(reply.Data); err != nil {
				return fmt.Errorf("restore %s: chunk at %d: %w", a.name, off, err)
			}
		}
	}
	done, err := nc.Request(rr.DeliverSubject, nil, testBackupAPITimeout)
	if err != nil {
		return fmt.Errorf("restore %s: end: %w", a.name, err)
	}
	return testJSAPIResponseError(done.Data)
}

func testJSAPIResponseError(data []byte) error {
	var cr natsserver.JSApiStreamCreateResponse
	if err := json.Unmarshal(data, &cr); err != nil {
		return fmt.Errorf("response %q: %w", data, err)
	}
	if cr.Error != nil {
		return cr.Error
	}
	return nil
}

// testRestoreBundle restores every archive of one set and requires each to succeed:
// the procedure's rule that a restore is complete only when all seven reported success.
func testRestoreBundle(t testing.TB, nc *nats.Conn, set map[string]testStreamArchive, names ...string) {
	t.Helper()
	if len(names) == 0 {
		names = mqttTestBrokerStreamNames
	}
	for _, name := range names {
		a, ok := set[name]
		if !ok {
			t.Fatalf("the backup set has no %s", name)
		}
		if err := testRestoreStream(nc, a, a.data); err != nil {
			t.Fatalf("%v", err)
		}
	}
}

// testDeleteBundle deletes whichever of the seven exist, as the restore procedure's
// step before restoring does.
func testDeleteBundle(t testing.TB, nc *nats.Conn) {
	t.Helper()
	for _, name := range mqttTestBrokerStreamNames {
		resp, err := nc.Request(fmt.Sprintf(natsserver.JSApiStreamDeleteT, name), nil, testBackupAPITimeout)
		if err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
		var dr natsserver.JSApiStreamDeleteResponse
		if err := json.Unmarshal(resp.Data, &dr); err != nil {
			t.Fatalf("delete %s: response %q: %v", name, resp.Data, err)
		}
		if dr.Error != nil && dr.Error.ErrCode != uint16(natsserver.JSStreamNotFoundErr) {
			t.Fatalf("delete %s: %v", name, dr.Error)
		}
	}
}

// testS2ChunkEnds returns the offset just past every complete S2 chunk of a framed
// stream (a 4-byte header: a type byte and a 24-bit little-endian length). A cut at
// one of them leaves a prefix the reader decodes without a framing error, so a
// truncated archive ends at a known record rather than inside a compressed block.
func testS2ChunkEnds(data []byte) []int {
	var ends []int
	for off := 0; off+4 <= len(data); {
		n := int(binary.LittleEndian.Uint32(append(data[off+1:off+4:off+4], 0)))
		off += 4 + n
		if off > len(data) {
			break
		}
		ends = append(ends, off)
	}
	return ends
}

// testS2Decode decodes an S2 prefix; an error means the cut is not at a chunk end.
func testS2Decode(prefix []byte) ([]byte, error) {
	return io.ReadAll(s2.NewReader(bytes.NewReader(prefix)))
}
