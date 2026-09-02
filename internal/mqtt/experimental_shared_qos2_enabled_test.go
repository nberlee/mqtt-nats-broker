//go:build mqttport_experimental_shared_qos2

package mqtt

import "testing"

const mqttTestExperimentalSharedQoS2 = true

func testRequireExperimentalSharedQoS2(t testing.TB) {
	t.Helper()
}
