//go:build mqttport_fulltests && !mqttport_experimental_shared_qos2

package mqtt

import (
	"net"
	"testing"
)

// These stubs keep the mixed orphan-sweep cluster file buildable in the
// supported suite. Its one affine test skips at the first helper call; the real
// helpers are compiled by the experimental build tag.
func testQ2AffClusterMember(t *testing.T, _ *testClusterBroker, _, _, _ string) (net.Conn, *mqttReader) {
	t.Helper()
	testRequireExperimentalSharedQoS2(t)
	return nil, nil
}

func testQ2AffClusterPublish(t *testing.T, _ *testClusterBroker, _, _, _ string) {
	t.Helper()
	testRequireExperimentalSharedQoS2(t)
}

func testQ2AffClusterClaims(t *testing.T, _ *testClusterBroker) map[string]*mqttQ2AffClaim {
	t.Helper()
	testRequireExperimentalSharedQoS2(t)
	return nil
}
