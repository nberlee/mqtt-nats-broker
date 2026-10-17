// Copyright 2026 The mqtt-nats-broker Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"reflect"
	"testing"
)

// TestF105NoConnectionScopedExecutionRegistry is the structural half of the integrated gate's round-1
// finding: no connection or session type holds a map keyed by acquisition. Such a map outlives every
// acquisition it records, which is the per-transmission growth the consume-once fact must not have;
// the fact lives on each acquisition's holder instead. The runtime half, which also sees a registry
// keyed some other way, is TestF105ALongLivedConnectionRetainsNoTransmissionHistory.
//
// Mutation: M4a (a client map keyed by mqttActionKey).
func TestF105NoConnectionScopedExecutionRegistry(t *testing.T) {
	keyT := reflect.TypeOf(mqttActionKey{})
	pkg := keyT.PkgPath()
	seen := map[reflect.Type]bool{}
	var walk func(rt reflect.Type, path string)
	walk = func(rt reflect.Type, path string) {
		if seen[rt] {
			return
		}
		seen[rt] = true
		switch rt.Kind() {
		case reflect.Map:
			if rt.Key() == keyT {
				t.Errorf("%s is a map keyed by acquisition: a connection-scoped execution registry", path)
			}
			walk(rt.Key(), path+"[key]")
			walk(rt.Elem(), path+"[]")
		case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
			walk(rt.Elem(), path+"[]")
		case reflect.Struct:
			// Only this package's types: another package cannot hold this package's key.
			if rt.PkgPath() != pkg {
				return
			}
			for i := 0; i < rt.NumField(); i++ {
				f := rt.Field(i)
				walk(f.Type, path+"."+f.Name)
			}
		}
	}
	for _, root := range []reflect.Type{reflect.TypeOf(client{}), reflect.TypeOf(mqtt{}), reflect.TypeOf(mqttSession{})} {
		walk(root, root.Name())
	}
	if !seen[reflect.TypeOf(mqttSendAction{})] || !seen[reflect.TypeOf(mqttPublishOutput{})] {
		t.Fatal("the walk never reached the holders, so it inspected nothing that matters")
	}
}
