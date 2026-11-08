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
	"sort"
	"strings"
	"testing"
)

// frozenDollarRoots is every "$" space a production string literal names, and why it is
// there. A deployment's ACLs are written against these spaces, so a new one is a change to
// the permission contract (docs/ARCHITECTURE.md, "The permission contract") and fails here
// until it is classified. The root is the literal up to and including the first ".", "/"
// or "_" after the "$" (the whole literal when it has none).
//
// What this does not prove, and the contract section says so: that a subject is built only
// from literals (a root assembled at run time escapes it), or that a write stays inside the
// grant within an already-classified root. The two roots a grant names narrowly, "$JS." and
// "$SYS.", have their literals pinned below for that reason; the others are granted or
// denied whole, or are not subjects at all.
var frozenDollarRoots = map[string]string{
	"$MQTT.":      "the reserved namespace, granted or denied whole (ReservedNamespace)",
	"$MQTT_":      "stream, consumer and timer names, not subjects",
	"$JS.":        "the JetStream API and acknowledgements (ServicePublish, ClientDeniedSubjects); literals pinned",
	"$JS":         "the first token of an acknowledgement reply being parsed, not a subject",
	"$SYS.":       "the identity request (ClientRequiredPublish); literal pinned",
	"$KV.":        "the recommended client deny (ClientRecommendedDeniedSubjects)",
	"$O.":         "the recommended client deny (ClientRecommendedDeniedSubjects)",
	"$mqttshare.": "the queue group a shared subscription joins (SharedQueueGroups), not a subject",
	"$share/":     "the MQTT shared-subscription filter syntax, not a subject",
	"$sparkplug/": "the derived Sparkplug certificate topic, an application subject the service publishes",
	"$G":          "the account's name, not a subject",
	"$1":          "a regular-expression replacement in the wiki generator, not a subject",
}

// frozenDollarLiterals pins every literal under a root the contract grants narrowly.
var frozenDollarLiterals = map[string][]string{
	"$JS.":  {"$JS.", "$JS.>", "$JS.ACK."},
	"$SYS.": {"$SYS.REQ.USER.INFO"},
}

func dollarRoot(v string) string {
	if i := strings.IndexAny(v[1:], "./_"); i >= 0 {
		return v[:i+2]
	}
	return v
}

func TestEveryDollarSpaceIsClassified(t *testing.T) {
	census := brokerProductionCensus(t)
	seenRoots := map[string]bool{}
	pinned := map[string][]string{}
	for v, where := range census.dollarLiterals {
		root := dollarRoot(v)
		seenRoots[root] = true
		if _, ok := frozenDollarRoots[root]; !ok {
			t.Errorf("%s: the literal %q opens the unclassified space %q; classify it in frozenDollarRoots and state it in the permission contract", where, v, root)
		}
		if _, narrow := frozenDollarLiterals[root]; narrow {
			pinned[root] = append(pinned[root], v)
		}
	}
	for root := range frozenDollarRoots {
		if !seenRoots[root] {
			t.Errorf("frozenDollarRoots classifies %q, which no production literal opens; delete it", root)
		}
	}
	for root, want := range frozenDollarLiterals {
		got := pinned[root]
		sort.Strings(got)
		if strings.Join(got, " ") != strings.Join(want, " ") {
			t.Errorf("the literals under %q are %q, want %q: a new subject there may fall outside the contract's grant", root, got, want)
		}
	}
}
