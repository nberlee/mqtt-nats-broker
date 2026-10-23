// The message-GET transport surface: every point read this broker makes enters through
// one of five mqttJSA methods and leaves through one decode arm.
//
// The persisted-identity ingress census enumerates call sites of those five methods by
// name. That enumeration is only complete if there is no other way to read a message: a
// point read built through a new request kind, a second decoder, or a direct-get route
// would be invisible to it. These tests are what keep the five methods the whole of the
// route. They survived the retirement of F-109's point-read absence census because they
// state a fact about the broker's transport, not about that census's bookkeeping.
//
// What they cannot do is prove there is no other route: they enforce that nothing new
// appears against the identifiers known today. That the listed identifiers are the whole of
// the message-read surface was verified by reading.

package mqtt

import "testing"

// jsaMsgGetMethods are the five mqttJSA methods that construct a message-GET request.
var jsaMsgGetMethods = map[string]string{
	"loadLastMsgFor":      "per-subject read",
	"loadLastMsgForMulti": "batched per-subject read",
	"loadMsg":             "fixed-sequence read",
	"loadMsgsMulti":       "batched fixed-sequence read",
	"loadNextMsgFromSeq":  "forward search over a filter",
}

// jsaMsgGetDecodeArm is the single reply-dispatch arm that decodes a *JSApiMsgGetResponse.
const jsaMsgGetDecodeArm = "processJSAPIReplies"

// TestJSAMsgGetRequestKindIsConfined: the request kind is named by the five constructors and
// the one decode arm, and constructed exactly once in each constructor. A sixth function
// naming it -- a new constructor or a second decoder -- fails by name.
func TestJSAMsgGetRequestKindIsConfined(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	got := map[string]bool{}
	for fn := range sc.FunctionsReferencing("mqttJSAMsgLoad") {
		got[fn.Name] = true
	}
	if len(got) == 0 {
		t.Fatal("no function references mqttJSAMsgLoad, so this check would pass vacuously: " +
			"the request kind has been renamed or the scan is broken")
	}
	for name := range got {
		if _, ok := jsaMsgGetMethods[name]; !ok && name != jsaMsgGetDecodeArm {
			t.Errorf("%s constructs or decodes an mqttJSAMsgLoad request and is not one of the "+
				"five message-GET methods or the decode arm: a new point-read route", name)
		}
	}
	for name, what := range jsaMsgGetMethods {
		if !got[name] {
			t.Errorf("%s (%s) no longer references mqttJSAMsgLoad; a stale entry hides the "+
				"route that replaced it", name, what)
		}
	}
	if !got[jsaMsgGetDecodeArm] {
		t.Errorf("%s no longer decodes mqttJSAMsgLoad replies; the decode moved", jsaMsgGetDecodeArm)
	}
	// By OCCURRENCE, not by function: a second construction added inside an existing method
	// leaves the function set unchanged.
	constructions := 0
	for _, c := range sc.Calls {
		if c.Callee != "newRequest" && c.Callee != "newRequestExMulti" {
			continue
		}
		if len(c.Args) > 0 && c.Args[0] == "mqttJSAMsgLoad" {
			constructions++
		}
	}
	if constructions != len(jsaMsgGetMethods) {
		t.Errorf("the package constructs %d mqttJSAMsgLoad requests; there are %d message-GET "+
			"methods. A new point read, including one inside a method that already makes one, "+
			"is a new route", constructions, len(jsaMsgGetMethods))
	}
}

// TestJSAMsgGetHasNoAlternateRoute: the message-GET request type and subject template are
// named only inside the five methods, and no direct-get route exists. This catches a raw
// request route that leaves the five constructions alone. Consumers of the DECODED response
// are not routes and are deliberately not policed here.
func TestJSAMsgGetHasNoAlternateRoute(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	for _, ident := range []string{"JSApiMsgGetRequest", "JSApiMsgGetT"} {
		fns := sc.FunctionsReferencing(ident)
		if len(fns) == 0 {
			t.Errorf("no function references %s, so this check would pass vacuously", ident)
		}
		for fn := range fns {
			if _, ok := jsaMsgGetMethods[fn.Name]; !ok {
				t.Errorf("%s references %s and is not one of the five message-GET methods: a "+
					"new point-read route", fn.String(), ident)
			}
		}
	}
	// A direct-get route would be a sixth entry point, with server-side error mapping that
	// differs from the message-GET handler's.
	for _, ident := range []string{"JSDirectMsgGetT", "JSApiDirectMsgGet", "getDirectRequest"} {
		for fn := range sc.FunctionsReferencing(ident) {
			t.Errorf("%s references %s: a DIRECT-GET route has appeared", fn.String(), ident)
		}
	}
}

// jsaMsgGetResponseHolders are the functions that name the decoded message-GET response type in
// their bodies: the five methods, the decode arm, and the inventory traversal, which holds the
// responses loadInflightRecordsMulti and loadMsgsMulti already decoded.
var jsaMsgGetResponseHolders = map[string]string{
	jsaMsgGetDecodeArm:          "decodes the reply",
	"mqttTraverseInventoryOnce": "holds batched responses a message-GET method decoded",
}

// TestJSAMsgGetHasNoSecondDecoder: the decoded response type is named only by the message-GET
// methods, the decode arm and the accounted holders. A function that unmarshals a reply into it
// without naming the request kind or the request type -- a second decoder -- fails here by name.
// Like the other checks it reads function BODIES, so a function naming the type only in its
// signature (mqttDecodeDutyResponse takes one as a parameter) is not seen; such a function
// receives a response somebody else decoded.
func TestJSAMsgGetHasNoSecondDecoder(t *testing.T) {
	t.Parallel()
	sc := censusScanPackage(t)
	fns := sc.FunctionsReferencing("JSApiMsgGetResponse")
	if len(fns) == 0 {
		t.Fatal("no function references JSApiMsgGetResponse, so this check would pass vacuously")
	}
	seen := map[string]bool{}
	for fn := range fns {
		seen[fn.Name] = true
		_, method := jsaMsgGetMethods[fn.Name]
		_, holder := jsaMsgGetResponseHolders[fn.Name]
		if !method && !holder {
			t.Errorf("%s references JSApiMsgGetResponse and is neither a message-GET method nor an "+
				"accounted holder: a second decoder of message-GET replies", fn.String())
		}
	}
	for name, what := range jsaMsgGetResponseHolders {
		if !seen[name] {
			t.Errorf("%s (%s) no longer references JSApiMsgGetResponse; a stale entry hides "+
				"whatever replaced it", name, what)
		}
	}
}
