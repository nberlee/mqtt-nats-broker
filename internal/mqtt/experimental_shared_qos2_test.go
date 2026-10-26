package mqtt

import "testing"

// testRequireExperimentalSharedQoS2 skips a test that only holds while the parked
// shared QoS 2 integration is built in.
//
// It is DERIVED from mqttTestExperimentalSharedQoS2 rather than written once per
// build tag, so the constant has a reader in every build. Stated the other way
// round -- a tagged pair of helpers beside a tagged pair of constants -- the two
// encode one fact twice, and in the default build nothing read the constant at all:
// its only other reader, mqtt_test.go, is itself behind mqtt_fulltests, so
// `golangci-lint run` (which passes no tags) reported it unused.
func testRequireExperimentalSharedQoS2(t testing.TB) {
	t.Helper()
	if !mqttTestExperimentalSharedQoS2 {
		t.Skip("parked shared QoS 2 test; run with -tags mqtt_experimental_shared_qos2")
	}
}
