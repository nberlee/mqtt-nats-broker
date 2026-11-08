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

// The permission contract: the NATS permissions a deployment grants the broker's service
// identity and denies its MQTT identities, frozen so that a hardened deployment's ACLs
// survive upgrades (docs/ARCHITECTURE.md, "The permission contract"). It is data only: the
// broker checks none of it, because a deployment's grants are the server's to enforce.
// The shipped NATS configurations and the runtime witness in cmd/mqtt-nats-broker are
// checked against these values, so this is the one place they are stated.

// ReservedNamespace is the broker's own subject space. Everything the broker owns on the
// wire is under it, and an operator grants or denies it whole: the families inside it are
// the persisted format's and the broker-to-broker protocol's, versioned with them.
const ReservedNamespace = mqttPrefix + ">"

// ClientDeniedSubjects are denied to every MQTT identity, publish and subscribe, and are
// REQUIRED: without them an MQTT client forges the broker's state or, publishing to
// $JS/API/STREAM/DELETE/$MQTT_sess, deletes its streams. "$JS.>" covers the domain-qualified
// API and the acknowledgement subjects too.
var ClientDeniedSubjects = []string{ReservedNamespace, "$JS.>"}

// ClientRecommendedDeniedSubjects are denied to every MQTT identity in every shipped
// configuration as default isolation of the account's key-value and object-store data. The
// broker does not depend on them.
var ClientRecommendedDeniedSubjects = []string{"$KV.>", "$O.>"}

// ClientRequiredPublish must stay publishable by every MQTT identity under auth passthrough:
// the broker asks the server who a client is by publishing it on the client's own connection.
const ClientRequiredPublish = mqttUserInfoSubject

// SharedQueueGroups is the queue-group name every shared subscription joins. A subscribe
// permission that names queue groups must admit it, or shared subscriptions are refused.
const SharedQueueGroups = mqttSharedQueuePrefix + "*"

// ServiceSubscribe is what the service identity must be allowed to subscribe to under auth
// passthrough: every delivery subscription and every reply the broker collects is under the
// reserved namespace. Without passthrough every client's subscriptions ride the service
// connection too, so it also needs the deployment's application subjects; those are the
// deployment's to name.
func ServiceSubscribe() []string { return []string{ReservedNamespace} }

// ServicePublish is what the service identity must be allowed to publish to, beyond the
// deployment's application subjects (Wills, and the derived Sparkplug certificate
// publication on $sparkplug.certificates.>, are published on the service connection): the
// reserved namespace, the JetStream API, the acknowledgements, and, under auth passthrough,
// its own identity request.
//
// With a domain the broker sends "$JS.<domain>.API." requests, and the API grant is both
// prefixes: a server that runs that domain itself maps the request onto "$JS.API." BEFORE it
// checks the publisher's permissions (nats-server applies account mappings in the parser),
// so it judges the unprefixed subject; a server that reaches the domain over a leaf node
// judges the prefixed one.
func ServicePublish(domain string, authPassthrough bool) []string {
	grant := []string{ReservedNamespace, mqttJSAPIPrefix(_EMPTY_) + ">"}
	if domain != _EMPTY_ {
		grant = append(grant, mqttJSAPIPrefix(domain)+">")
	}
	grant = append(grant, jsAckPre+">")
	if authPassthrough {
		grant = append(grant, mqttUserInfoSubject)
	}
	return grant
}

// mqttJSAPIPrefix is the JetStream API prefix requests are sent under, with its trailing
// separator: "$JS.API." or, with a domain, "$JS.<domain>.API.".
func mqttJSAPIPrefix(domain string) string {
	if domain == _EMPTY_ {
		return JSApiPrefix + "."
	}
	return "$JS." + domain + ".API."
}
