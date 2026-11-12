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
	"encoding/json"
	"testing"
)

// A generation token is a well-formed generation, and names exactly the (L, GenOrd) it was
// minted from: a reader classifies a generation by parsing its own name.
func TestGenerationTokenRoundTripsItsOrdinals(t *testing.T) {
	for _, c := range []struct{ l, ord uint64 }{
		{1, 0}, {1, 1}, {62, 61}, {mqttMaxLifecycle, 0}, {7, ^uint64(0)},
	} {
		tok, err := mqttGenToken(c.l, c.ord)
		if err != nil {
			t.Fatalf("mqttGenToken(%d, %d): %v", c.l, c.ord, err)
		}
		if !mqttValidSessGen(tok) {
			t.Fatalf("token %q of (%d, %d) is not a well-formed generation", tok, c.l, c.ord)
		}
		l, ord, ok := mqttParseGenToken(tok)
		if !ok || l != c.l || ord != c.ord {
			t.Fatalf("token %q parsed to (%d, %d, %v), want (%d, %d)", tok, l, ord, ok, c.l, c.ord)
		}
	}
	if _, err := mqttGenToken(0, 1); err == nil {
		t.Fatal("a zero lifecycle minted a token")
	}
	if _, err := mqttGenToken(mqttMaxLifecycle+1, 0); err == nil {
		t.Fatal("a lifecycle beyond the token range minted a token")
	}
	if _, _, ok := mqttParseGenToken("0000000000" + "000000000001"); ok {
		t.Fatal("a token naming lifecycle 0 parsed")
	}
	if _, _, ok := mqttParseGenToken("short"); ok {
		t.Fatal("a malformed generation parsed")
	}
}

// Registry membership is explicit: a lifecycle is RETIRED only when the shard allocated it and
// lists it neither active nor retiring.
func TestLifecycleRegistryClassifiesByExplicitMembership(t *testing.T) {
	const epoch = "nuid:0123456789ABCDEFGHIJKL"
	h := "abcdefgh"
	shard := mqttLifecycleRegistryShardFor(h)
	r := mqttNewLifecycleRegistryShard(epoch, shard)
	r.Next = 5
	r.Active[h] = 4
	r.Retiring[h] = mqttRetiringLifecycle{L: 3, Phase: mqttLifecycleBounded, Bound: 900}
	if err := mqttValidateLifecycleRegistryShard(r, epoch, shard); err != nil {
		t.Fatalf("valid shard refused: %v", err)
	}
	for l, want := range map[uint64]mqttLifecycleStatus{
		0: mqttLifecycleUnallocated, 5: mqttLifecycleUnallocated,
		4: mqttLifecycleActive, 3: mqttLifecycleRetiringStatus, 2: mqttLifecycleRetired,
	} {
		if got := r.classify(h, l); got != want {
			t.Fatalf("lifecycle %d classified %d, want %d", l, got, want)
		}
	}

	body, _ := json.Marshal(r)
	back, err := mqttDecodeLifecycleRegistryShard(body, epoch, shard)
	if err != nil || back.Next != 5 || back.Active[h] != 4 || back.Retiring[h].Bound != 900 {
		t.Fatalf("round trip: %+v, %v", back, err)
	}
	if _, err := mqttDecodeLifecycleRegistryShard([]byte(`{"epoch":"`+epoch+`","shard":0,"next":1,"active":{},"retiring":{},"extra":1}`), epoch, 0); err == nil {
		t.Fatal("a shard carrying an unknown member decoded")
	}

	bad := *r
	bad.Retiring = map[string]mqttRetiringLifecycle{h: {L: 4, Phase: mqttLifecycleFenced}}
	if err := mqttValidateLifecycleRegistryShard(&bad, epoch, shard); err == nil {
		t.Fatal("a lifecycle both active and retiring was accepted")
	}
	bad.Retiring = map[string]mqttRetiringLifecycle{h: {L: 3, Phase: mqttLifecycleFenced, Bound: 1}}
	if err := mqttValidateLifecycleRegistryShard(&bad, epoch, shard); err == nil {
		t.Fatal("a fenced lifecycle carrying a bound was accepted")
	}
}
