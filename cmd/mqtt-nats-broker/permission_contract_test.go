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

package main

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/nberlee/mqtt-nats-broker/internal/mqtt"
)

// mqttIdentityDenies is every subject a shipped configuration denies an MQTT identity in both
// directions: the contract's required denies and its recommended ones.
func mqttIdentityDenies() []string {
	return append(slices.Clone(mqtt.ClientDeniedSubjects), mqtt.ClientRecommendedDeniedSubjects...)
}

// mqttIdentityProblems holds one MQTT identity's parsed permissions to the permission contract
// (docs/ARCHITECTURE.md, "The permission contract"): the reserved, JetStream, KV and
// object-store spaces denied both ways; the identity request publishable as nats-server
// evaluates it, deny before allow; and no response permissions, which nats-server consults
// AFTER an explicit deny and would let the identity answer a request into $MQTT.>.
func mqttIdentityProblems(who string, p *server.Permissions) []string {
	var problems []string
	var pub, sub *server.SubjectPermission
	if p != nil {
		pub, sub = p.Publish, p.Subscribe
	}
	for dir, sp := range map[string]*server.SubjectPermission{"publish": pub, "subscribe": sub} {
		var deny []string
		if sp != nil {
			deny = sp.Deny
		}
		for _, s := range mqttIdentityDenies() {
			if !slices.Contains(deny, s) {
				problems = append(problems, fmt.Sprintf("%s may %s %s", who, dir, s))
			}
		}
	}
	if !publishAdmits(pub, mqtt.ClientRequiredPublish) {
		problems = append(problems, fmt.Sprintf("%s may not publish %s, which auth passthrough's identity request needs", who, mqtt.ClientRequiredPublish))
	}
	if p != nil && p.Response != nil {
		problems = append(problems, fmt.Sprintf("%s has response permissions (allow_responses), which re-allow a denied reply subject", who))
	}
	return problems
}

// contractIdentityProblems holds every identity a parsed configuration defines to the
// contract: the service identity to its pinned grant (publish then subscribe, as grantString
// renders them), every other user to mqttIdentityProblems. One check for the shipped files
// and for the configurations the chart renders.
func contractIdentityProblems(opts *server.Options, service, servicePub, serviceSub string) []string {
	var problems []string
	found := false
	for _, u := range opts.Users {
		if u.Username == service {
			found = true
			var pub, sub *server.SubjectPermission
			if u.Permissions != nil {
				pub, sub = u.Permissions.Publish, u.Permissions.Subscribe
			}
			if got := grantString(pub); got != servicePub {
				problems = append(problems, fmt.Sprintf("the service identity's publish grant is %s, want %s", got, servicePub))
			}
			if got := grantString(sub); got != serviceSub {
				problems = append(problems, fmt.Sprintf("the service identity's subscribe grant is %s, want %s", got, serviceSub))
			}
			continue
		}
		problems = append(problems, mqttIdentityProblems("user "+u.Username, u.Permissions)...)
	}
	if !found {
		problems = append(problems, fmt.Sprintf("no service identity %q", service))
	}
	if len(opts.Users) < 2 {
		problems = append(problems, "no MQTT identity to check")
	}
	return problems
}

// publishAdmits reports whether nats-server lets a publish to subject through p: any matching
// deny refuses it, and an empty allow list admits everything else.
func publishAdmits(p *server.SubjectPermission, subject string) bool {
	if p == nil {
		return true
	}
	for _, d := range p.Deny {
		if server.SubjectMatchesFilter(subject, d) {
			return false
		}
	}
	if len(p.Allow) == 0 {
		return true
	}
	for _, a := range p.Allow {
		if server.SubjectMatchesFilter(subject, a) {
			return true
		}
	}
	return false
}

// grantString renders one direction of a grant for comparison: "allow=[...] deny=[...]", or
// "all" when nothing restricts it.
func grantString(p *server.SubjectPermission) string {
	if p == nil || (len(p.Allow) == 0 && len(p.Deny) == 0) {
		return "all"
	}
	return "allow=[" + strings.Join(p.Allow, " ") + "] deny=[" + strings.Join(p.Deny, " ") + "]"
}

// TestShippedACLsStateThePermissionContract parses every NATS configuration the repository
// ships and holds each identity in it to the contract: every MQTT identity by
// checkMQTTIdentityPermissions, and the service identity to the grant pinned for that file.
// The configurations the Helm chart renders are held to the same check by
// checkBrokerNATSOptions.
func TestShippedACLsStateThePermissionContract(t *testing.T) {
	t.Setenv("MQTTNB_NATS_USER", "svc")
	t.Setenv("MQTTNB_NATS_PASS", "svcpass")
	t.Setenv("MQTTNB_DEFAULT_USER", "anon")
	t.Setenv("MQTTNB_DEFAULT_PASS", "anonpass")
	t.Setenv("MQTTNB_EMBEDDED_SERVER_NAME", "acl-check")
	t.Setenv("MQTTNB_EMBEDDED_ROUTE_USER", "route")
	t.Setenv("MQTTNB_EMBEDDED_ROUTE_PASS", "routepass")
	t.Setenv("MQTTNB_EMBEDDED_ROUTES", "[nats-route://route:routepass@127.0.0.1:1]")
	for _, v := range []string{"SERVICE_PASSWORD", "ANONYMOUS_PASSWORD", "ALICE_PASSWORD", "BOB_PASSWORD", "OUTSIDER_PASSWORD"} {
		t.Setenv(v, "x")
	}
	for _, tc := range []struct {
		path    string
		service string
		// The service identity's pinned grant, publish then subscribe. The embedded and chart
		// configurations do not choose the auth-passthrough mode, so they grant everything;
		// the conformance environment runs with passthrough and grants the contract.
		servicePub, serviceSub string
	}{
		{"../../embedded/nats.conf", "svc", "all", "all"},
		{"../../embedded/nats-cluster.conf", "svc", "all", "all"},
		{"../../charts/mqtt-nats-broker/files/nats-cluster.conf", "svc", "all", "all"},
		{"../../test/conformance/nats.conf", "service", "allow=[>] deny=[]", "allow=[" + strings.Join(mqtt.ServiceSubscribe(), " ") + "] deny=[]"},
		{"../../docs/deploy/nats-auth/nats.conf", "mqtt-broker", "all", "all"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			opts, err := server.ProcessConfigFile(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range contractIdentityProblems(opts, tc.service, tc.servicePub, tc.serviceSub) {
				t.Error(p)
			}
		})
	}
}

// The check itself refuses what it exists to refuse, on configurations no shipped file has:
// a service grant narrowed below the pinned one, and an extra MQTT identity left open.
func TestContractIdentityCheckRefusesADeviation(t *testing.T) {
	safe := &server.Permissions{
		Publish:   &server.SubjectPermission{Deny: mqttIdentityDenies()},
		Subscribe: &server.SubjectPermission{Deny: mqttIdentityDenies()},
	}
	for _, tc := range []struct {
		name  string
		users []*server.User
	}{
		{"a restricted service grant", []*server.User{
			{Username: "svc", Permissions: &server.Permissions{Publish: &server.SubjectPermission{Deny: []string{"$JS.>"}}}},
			{Username: "anon", Permissions: safe},
		}},
		{"an extra MQTT identity without denies", []*server.User{
			{Username: "svc"},
			{Username: "anon", Permissions: safe},
			{Username: "extra"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if p := contractIdentityProblems(&server.Options{Users: tc.users}, "svc", "all", "all"); len(p) == 0 {
				t.Fatal("the deviation passed the contract check")
			}
		})
	}
	if p := contractIdentityProblems(&server.Options{Users: []*server.User{{Username: "svc"}, {Username: "anon", Permissions: safe}}}, "svc", "all", "all"); len(p) != 0 {
		t.Fatalf("a conforming configuration was refused: %v", p)
	}
}
