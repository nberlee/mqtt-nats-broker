//go:build !mqttport_experimental_shared_qos2

package mqtt

import "testing"

const mqttTestExperimentalSharedQoS2 = false

func testRequireExperimentalSharedQoS2(t testing.TB) {
	t.Helper()
	t.Skip("parked shared QoS 2 test; run with -tags mqttport_experimental_shared_qos2")
}
