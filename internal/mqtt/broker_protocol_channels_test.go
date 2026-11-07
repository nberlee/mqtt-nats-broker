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

// The broker-to-broker protocol is versioned by the persisted format's one key
// (docs/ARCHITECTURE.md, "The broker-to-broker protocol"). These checks hold the
// protocol's CHANNELS and VOCABULARY to what that section names, so a new way for one
// broker to reach another cannot appear without being classified:
//
//   - every nats.go function or method the production build uses is classified, and
//     every use that sends, subscribes or requests is a counted site with its channel;
//   - every live publication writer is censused, including the one that rides the JSA queue;
//   - the Nmqtt-* header names in the code are exactly the documented ones;
//   - the names peers must derive identically are pinned to literal values.
//
// What they do not prove, and the section says so: that two releases' decision rules
// agree, that a documented header's carrier or meaning is right, or anything about a
// call routed through a broker-defined interface or reflection. The census reads the
// default production build (no build tags), whatever flags this test runs under.

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
)

const (
	brokerModulePath = "github.com/nberlee/mqtt-nats-broker"
	natsClientPath   = "github.com/nats-io/nats.go"
	natsJetStreamAPI = "github.com/nats-io/nats.go/jetstream"
)

// natsUseClass is what a use of a nats.go function or method does on the wire.
type natsUseClass string

const (
	// Sends a message. Each site names its channel in frozenNATSTransportSites.
	natsUsePublish natsUseClass = "publish"
	// Creates a subscription. Each site names its channel.
	natsUseSubscribe natsUseClass = "subscribe"
	// A request to the NATS server or JetStream, answered by the backend.
	natsUseBackendRequest natsUseClass = "backend-request"
	// Connection or subscription lifecycle and state: dialing, options, handlers,
	// flushing, closing, unsubscribing, reading a subscription's own queue.
	natsUseLifecycle natsUseClass = "lifecycle"
	// Builds or edits a message in memory.
	natsUseNoIO natsUseClass = "no-io"
)

// classifiedNATSObjects is every nats.go and jetstream function or method the production
// build uses, by its full name. A newly used one fails until it is classified here.
var classifiedNATSObjects = map[string]natsUseClass{
	"(*github.com/nats-io/nats.go.Conn).PublishMsg":     natsUsePublish,
	"(*github.com/nats-io/nats.go.Conn).PublishRequest": natsUseBackendRequest,
	"(*github.com/nats-io/nats.go.Conn).Subscribe":      natsUseSubscribe,
	"(*github.com/nats-io/nats.go.Conn).QueueSubscribe": natsUseSubscribe,
	"(*github.com/nats-io/nats.go.Conn).SubscribeSync":  natsUseSubscribe,

	"(github.com/nats-io/nats.go/jetstream.JetStream).AccountInfo": natsUseBackendRequest,

	"github.com/nats-io/nats.go.Connect":                         natsUseLifecycle,
	"github.com/nats-io/nats.go/jetstream.New":                   natsUseLifecycle,
	"github.com/nats-io/nats.go/jetstream.NewWithDomain":         natsUseLifecycle,
	"github.com/nats-io/nats.go.ClosedHandler":                   natsUseLifecycle,
	"github.com/nats-io/nats.go.CustomInboxPrefix":               natsUseLifecycle,
	"github.com/nats-io/nats.go.DrainTimeout":                    natsUseLifecycle,
	"github.com/nats-io/nats.go.ErrorHandler":                    natsUseLifecycle,
	"github.com/nats-io/nats.go.InProcessServer":                 natsUseLifecycle,
	"github.com/nats-io/nats.go.MaxReconnects":                   natsUseLifecycle,
	"github.com/nats-io/nats.go.Name":                            natsUseLifecycle,
	"github.com/nats-io/nats.go.ReconnectBufSize":                natsUseLifecycle,
	"github.com/nats-io/nats.go.ReconnectWait":                   natsUseLifecycle,
	"github.com/nats-io/nats.go.UserCredentials":                 natsUseLifecycle,
	"github.com/nats-io/nats.go.UserInfo":                        natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).Close":                   natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).Drain":                   natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).Flush":                   natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).IsConnected":             natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).LastError":               natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).ConnectedClusterName":    natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).ConnectedServerName":     natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).DisconnectErrHandler":    natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).ReconnectHandler":        natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).SetClosedHandler":        natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).SetDisconnectErrHandler": natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Conn).SetReconnectHandler":     natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Subscription).Drain":           natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Subscription).NextMsg":         natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Subscription).Pending":         natsUseLifecycle,
	"(*github.com/nats-io/nats.go.Subscription).Unsubscribe":     natsUseLifecycle,

	"github.com/nats-io/nats.go.NewMsg":       natsUseNoIO,
	"(github.com/nats-io/nats.go.Header).Del": natsUseNoIO,
}

// natsChannel is which of the documented channels a transport site carries.
type natsChannel string

const (
	// The live publication: one broker's PUBLISH, on its user subject, read by every
	// peer whose client subscribed to it. Broker-to-broker.
	channelLivePublication natsChannel = "live-publication"
	// The subscribe site inside (*client).processSub: a user subject is a peer's live
	// fan-out (broker-to-broker); an $MQTT. subject is a JetStream delivery or the JSA
	// reply subscription. The prefix picks the service connection; it does not tell those
	// two apart, and neither is broker-to-broker.
	channelLiveSubOrStoreOrReply natsChannel = "live-subscription-or-store-delivery-or-backend-reply"
	// The JSA transport: JetStream API requests, consumer acknowledgements and confirmed
	// acknowledgements, store publications. Broker-to-backend mechanics; the payloads,
	// names and decision rules it carries are protocol.
	channelBackendJSA natsChannel = "backend-jsa"
	// The identity query on $SYS.REQ.USER.INFO and its reply inbox. Broker-to-backend.
	channelBackendIdentity natsChannel = "backend-identity"
	// The startup JetStream probe. Broker-to-backend.
	channelBackendProbe natsChannel = "backend-probe"
)

// natsTransportSite is one use of a sending, subscribing or requesting object.
type natsTransportSite struct {
	pkg    string // module-relative package directory
	fn     string // enclosing function, "Recv.Name" for a method, "<pkg-init>" outside any
	object string
}

// frozenNATSTransportSites is every such use, counted, with its channel.
var frozenNATSTransportSites = map[natsTransportSite]struct {
	n       int
	channel natsChannel
}{
	{"internal/mqtt", "client.mqttFanOut", "(*github.com/nats-io/nats.go.Conn).PublishMsg"}:               {1, channelLivePublication},
	{"internal/mqtt", "client.processInboundClientMsg", "(*github.com/nats-io/nats.go.Conn).PublishMsg"}:  {1, channelBackendJSA},
	{"internal/mqtt", "client.processSub", "(*github.com/nats-io/nats.go.Conn).Subscribe"}:                {1, channelLiveSubOrStoreOrReply},
	{"internal/mqtt", "client.processSub", "(*github.com/nats-io/nats.go.Conn).QueueSubscribe"}:           {1, channelLiveSubOrStoreOrReply},
	{"internal/mqtt", "mqttFetchIdentity", "(*github.com/nats-io/nats.go.Conn).SubscribeSync"}:            {1, channelBackendIdentity},
	{"internal/mqtt", "mqttFetchIdentity", "(*github.com/nats-io/nats.go.Conn).PublishRequest"}:           {1, channelBackendIdentity},
	{"internal/backend", "Connect", "(github.com/nats-io/nats.go/jetstream.JetStream).AccountInfo"}:       {1, channelBackendProbe},
	{"internal/backend", "waitJetStream", "(github.com/nats-io/nats.go/jetstream.JetStream).AccountInfo"}: {1, channelBackendProbe},
}

// The production packages that import nats.go, and jetstream, DIRECTLY. The census
// itself covers every package that depends on nats.go transitively; these sets say
// where the dependency is declared.
var (
	natsClientImporters    = []string{"cmd/mqtt-nats-broker", "internal/backend", "internal/mqtt"}
	natsJetStreamImporters = []string{"internal/backend"}
)

// frozenPublishWrapperRefs is every reference to the two wrappers around the raw publish
// sites, by enclosing function: mqttFanOut is the live publication, processInboundClientMsg
// the JSA queue's sender.
var frozenPublishWrapperRefs = map[string]map[string]int{
	"mqttFanOut":              {"Server.mqttIssuePublication": 1},
	"processInboundClientMsg": {"mqttAccountSessionManager.sendJSAPIrequests": 1},
}

// jsaQueueMember is one direct reference to the queue type the JSA sender publishes from.
type jsaQueueMember struct {
	fn     string // enclosing function
	member string // the ipQueue type, its constructor, or one of its methods or fields
}

// frozenJSAQueueMembers is every direct reference, outside shim_small.go where the type is
// defined, to ipQueue — the type itself, newIPQueue, and every method and field, whatever the
// type arguments, through aliases and generic helpers alike. Production has one specialization,
// ipQueue[*mqttJSPubMsg], and the JSA sender publishes whatever it holds, so this is the queue's
// whole surface: the session manager creates it, the four API requests and sendMsgReply push onto
// it, the sender drains it. Changes inside shim_small.go are outside this census.
var frozenJSAQueueMembers = map[jsaQueueMember]int{
	{"mqttJSA.newAtomicBatchRequest", "push"}:                  1,
	{"mqttJSA.purgeStream", "push"}:                            1,
	{"mqttJSA.queueRequest", "push"}:                           1,
	{"mqttJSA.deleteMsg", "push"}:                              1,
	{"mqttJSA.sendMsgReply", "push"}:                           1,
	{"mqttAccountSessionManager.sendJSAPIrequests", "ch"}:      1,
	{"mqttAccountSessionManager.sendJSAPIrequests", "pop"}:     1,
	{"mqttAccountSessionManager.sendJSAPIrequests", "recycle"}: 1,
	{"Server.mqttCreateAccountSessionManager", "newIPQueue"}:   1,
	{"<pkg-init>", "type"}:                                     1, // the sendq field of mqttJSA
}

// jsaSendCaller is one reference to a JSA no-API send entry point.
type jsaSendCaller struct {
	entry string // sendMsg, sendMsgReply or sendAck
	fn    string
}

// jsaSendChannel is what a no-API send through the JSA queue carries.
type jsaSendChannel string

const (
	// A consumer acknowledgement, negative acknowledgement or confirmed acknowledgement.
	// Broker-to-backend.
	jsaSendBackendAck jsaSendChannel = "backend-ack"
	// A no-wait JetStream API request. Broker-to-backend.
	jsaSendBackendRequest jsaSendChannel = "backend-request"
	// A publication on a user subject that peers' subscriptions read: broker-to-broker.
	jsaSendLivePublication jsaSendChannel = "live-publication"
	// One entry point forwarding to another.
	jsaSendForward jsaSendChannel = "forward"
)

// frozenJSASendCallers is every caller of the JSA queue's no-API send entry points, with
// what it sends. The derived Sparkplug certificate publication is the one live publication
// among them: a broker-to-broker channel that does not go through mqttFanOut.
var frozenJSASendCallers = map[jsaSendCaller]struct {
	n       int
	channel jsaSendChannel
}{
	{"sendMsg", "client.mqttHandlePubRetain"}:              {1, jsaSendLivePublication},
	{"sendMsg", "mqttJSA.deleteConsumer"}:                  {1, jsaSendBackendRequest},
	{"sendMsg", "mqttJSA.sendAck"}:                         {1, jsaSendForward},
	{"sendMsgReply", "mqttJSA.sendMsg"}:                    {1, jsaSendForward},
	{"sendMsgReply", "mqttPreparedAck.enqueue"}:            {1, jsaSendBackendAck},
	{"sendAck", "mqttSession.submitSourceAckLocked"}:       {1, jsaSendBackendAck},
	{"sendMsg", "client.mqttProcessPubComp"}:               {2, jsaSendBackendAck},
	{"sendMsg", "client.mqttProcessPublishReceived"}:       {2, jsaSendBackendAck},
	{"sendMsg", "mqttDeliverPubRelCb"}:                     {1, jsaSendBackendAck},
	{"sendMsg", "mqttDeliverQ2AffineCb"}:                   {1, jsaSendBackendAck},
	{"sendMsg", "mqttDeliverQoS12Offer"}:                   {1, jsaSendBackendAck},
	{"sendMsg", "mqttSession.handBackLocked"}:              {1, jsaSendBackendAck},
	{"sendMsg", "mqttSession.publishLateNAKs"}:             {1, jsaSendBackendAck},
	{"sendMsg", "mqttSession.refuseQ2AffCopy"}:             {1, jsaSendBackendAck},
	{"sendMsg", "mqttSession.releaseDeferredAfterCustody"}: {1, jsaSendBackendAck},
	{"sendMsg", "mqttSession.releaseTerminalReservation"}:  {1, jsaSendBackendAck},
	{"sendMsg", "mqttSession.resendPendingRedelivery"}:     {3, jsaSendBackendAck},
	{"sendMsg", "mqttSession.runAdmissionContinuityUnit"}:  {1, jsaSendBackendAck},
	{"sendMsg", "mqttSession.wakePubRelRedrive"}:           {1, jsaSendBackendAck},
}

// brokerListedPackage is the part of `go list -json` the census reads.
type brokerListedPackage struct {
	ImportPath string
	Dir        string
	Export     string
	GoFiles    []string
	Imports    []string
	Deps       []string
}

// brokerCensus is what the checks read: small summaries only. The parsed files and type
// information they were taken from are dropped as soon as the summaries exist, so a test
// binary running the whole suite carries no type-checked copy of the module.
type brokerCensus struct {
	clientImporters, jetStreamImporters []string
	// usedObjects maps every nats.go function or method used to one place it is used.
	usedObjects  map[string]string
	sites        map[natsTransportSite]int
	wrapperRefs  map[string]map[string]int
	queueMembers map[jsaQueueMember]int
	jsaSends     map[jsaSendCaller]int
	// headerNames is every Nmqtt-* header literal in any production package.
	headerNames map[string]bool
}

var (
	brokerCensusOnce   sync.Once
	brokerCensusResult *brokerCensus
	brokerCensusErr    error
)

// brokerProductionCensus takes the census once per test binary. The default build is what
// `go list` describes: no build tags.
func brokerProductionCensus(t *testing.T) *brokerCensus {
	t.Helper()
	brokerCensusOnce.Do(func() { brokerCensusResult, brokerCensusErr = takeBrokerCensus() })
	if brokerCensusErr != nil {
		t.Fatal(brokerCensusErr)
	}
	return brokerCensusResult
}

func takeBrokerCensus() (*brokerCensus, error) {
	cmd := exec.Command("go", "list", "-export", "-deps", "-json", brokerModulePath+"/...")
	cmd.Dir = "../.."
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("go list: %v\n%s", err, ee.Stderr)
		}
		return nil, fmt.Errorf("go list: %v", err)
	}
	exports := map[string]string{}
	var module []brokerListedPackage
	dec := json.NewDecoder(strings.NewReader(string(out)))
	for {
		var p brokerListedPackage
		if err := dec.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decoding go list output: %v", err)
		}
		exports[p.ImportPath] = p.Export
		if p.ImportPath == brokerModulePath || strings.HasPrefix(p.ImportPath, brokerModulePath+"/") {
			module = append(module, p)
		}
	}

	c := &brokerCensus{
		usedObjects:  map[string]string{},
		sites:        map[natsTransportSite]int{},
		wrapperRefs:  map[string]map[string]int{},
		queueMembers: map[jsaQueueMember]int{},
		jsaSends:     map[jsaSendCaller]int{},
		headerNames:  map[string]bool{},
	}
	for _, p := range module {
		rel := strings.TrimPrefix(p.ImportPath, brokerModulePath+"/")
		if brokerContains(p.Imports, natsClientPath) {
			c.clientImporters = append(c.clientImporters, rel)
		}
		if brokerContains(p.Imports, natsJetStreamAPI) {
			c.jetStreamImporters = append(c.jetStreamImporters, rel)
		}
		if err := c.addPackage(p, rel, exports); err != nil {
			return nil, err
		}
	}
	sort.Strings(c.clientImporters)
	sort.Strings(c.jetStreamImporters)
	return c, nil
}

// addPackage reads one production package: its header literals always, and, when it depends
// on nats.go directly or transitively, its type-checked uses.
func (c *brokerCensus) addPackage(p brokerListedPackage, rel string, exports map[string]string) error {
	fset := token.NewFileSet()
	var files []*ast.File
	for _, name := range p.GoFiles {
		f, err := parser.ParseFile(fset, filepath.Join(p.Dir, name), nil, 0)
		if err != nil {
			return fmt.Errorf("parsing %s: %v", name, err)
		}
		files = append(files, f)
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if v := strings.Trim(lit.Value, "\"`"); envelopeHeaderName.MatchString(v) {
					c.headerNames[v] = true
				}
			}
			return true
		})
	}
	if !brokerContains(p.Deps, natsClientPath) {
		return nil
	}

	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		exp, ok := exports[path]
		if !ok || exp == "" {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(exp)
	})
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
	pkg, err := (&types.Config{Importer: imp}).Check(p.ImportPath, fset, files, info)
	if err != nil {
		return fmt.Errorf("type-checking %s: %v", p.ImportPath, err)
	}
	queueMember := brokerJSAQueueMembers(pkg)

	// Each identifier's enclosing function; a use outside every function body (a
	// package-level initializer) is a site of its own.
	enclosing := map[*ast.Ident]string{}
	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok {
				continue
			}
			name := fd.Name.Name
			if recv := censusRecvType(fd); recv != "" {
				name = recv + "." + name
			}
			ast.Inspect(fd, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok {
					enclosing[id] = name
				}
				return true
			})
		}
	}
	enclosingOf := func(id *ast.Ident) string {
		if fn, ok := enclosing[id]; ok {
			return fn
		}
		return "<pkg-init>"
	}

	for id, obj := range info.Uses {
		if member, ok := queueMember(obj); ok && filepath.Base(fset.Position(id.Pos()).Filename) != "shim_small.go" {
			c.queueMembers[jsaQueueMember{enclosingOf(id), member}]++
		}
		fn, ok := obj.(*types.Func)
		if !ok || fn.Pkg() == nil {
			continue
		}
		switch path := fn.Pkg().Path(); {
		case path == natsClientPath || strings.HasPrefix(path, natsClientPath+"/"):
			name := fn.FullName()
			if _, seen := c.usedObjects[name]; !seen {
				c.usedObjects[name] = rel + " in " + enclosingOf(id)
			}
			switch classifiedNATSObjects[name] {
			case natsUsePublish, natsUseSubscribe, natsUseBackendRequest:
				c.sites[natsTransportSite{rel, enclosingOf(id), name}]++
			}
		case path == p.ImportPath:
			if _, tracked := frozenPublishWrapperRefs[fn.Name()]; tracked {
				if c.wrapperRefs[fn.Name()] == nil {
					c.wrapperRefs[fn.Name()] = map[string]int{}
				}
				c.wrapperRefs[fn.Name()][enclosingOf(id)]++
			}
			if sig, ok := fn.Type().(*types.Signature); ok && sig.Recv() != nil && brokerRecvNamed(sig, "mqttJSA") {
				switch fn.Name() {
				case "sendMsg", "sendMsgReply", "sendAck":
					c.jsaSends[jsaSendCaller{fn.Name(), enclosingOf(id)}]++
				}
			}
		}
	}
	return nil
}

// brokerJSAQueueMembers returns a classifier naming the ipQueue member an object is, if any:
// the generic type itself, its constructor, or one of its methods or fields, each normalized to
// its generic declaration (Func.Origin, Var.Origin), so an instantiation at any type argument, an
// alias of the element type or a generic helper over ipQueue[T] all name the same member. A
// package without the type classifies nothing.
func brokerJSAQueueMembers(pkg *types.Package) func(types.Object) (string, bool) {
	tn, _ := pkg.Scope().Lookup("ipQueue").(*types.TypeName)
	if tn == nil {
		return func(types.Object) (string, bool) { return "", false }
	}
	ctor := pkg.Scope().Lookup("newIPQueue")
	fields := map[*types.Var]bool{}
	if st, ok := tn.Type().Underlying().(*types.Struct); ok {
		for i := 0; i < st.NumFields(); i++ {
			fields[st.Field(i)] = true
		}
	}
	return func(obj types.Object) (string, bool) {
		switch o := obj.(type) {
		case *types.TypeName:
			return "type", o == tn
		case *types.Func:
			if ctor != nil && o.Origin() == ctor {
				return "newIPQueue", true
			}
			sig, ok := o.Origin().Type().(*types.Signature)
			if !ok || sig.Recv() == nil {
				return "", false
			}
			t := sig.Recv().Type()
			if ptr, ok := t.(*types.Pointer); ok {
				t = ptr.Elem()
			}
			if named, ok := t.(*types.Named); ok && named.Origin().Obj() == tn {
				return o.Name(), true
			}
		case *types.Var:
			if o.IsField() && fields[o.Origin()] {
				return o.Name(), true
			}
		}
		return "", false
	}
}

func brokerRecvNamed(sig *types.Signature, name string) bool {
	t := sig.Recv().Type()
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	return ok && named.Obj().Name() == name
}

func brokerContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestNATSClientUseIsClassifiedByChannel(t *testing.T) {
	c := brokerProductionCensus(t)

	if fmt.Sprint(c.clientImporters) != fmt.Sprint(natsClientImporters) {
		t.Errorf("production packages importing %s: %v, want %v — a new importer is a new place a broker can reach NATS; classify its uses below and add it here", natsClientPath, c.clientImporters, natsClientImporters)
	}
	if fmt.Sprint(c.jetStreamImporters) != fmt.Sprint(natsJetStreamImporters) {
		t.Errorf("production packages importing %s: %v, want %v — the JetStream client API is the startup probe's alone", natsJetStreamAPI, c.jetStreamImporters, natsJetStreamImporters)
	}

	for name, where := range c.usedObjects {
		if _, known := classifiedNATSObjects[name]; !known {
			t.Errorf("%s uses %s, which is not classified: say what it does on the wire in classifiedNATSObjects (docs/ARCHITECTURE.md, \"The broker-to-broker protocol\")", where, name)
		}
	}
	for name := range classifiedNATSObjects {
		if _, used := c.usedObjects[name]; !used {
			t.Errorf("classifiedNATSObjects lists %s, which the production build no longer uses: delete the entry", name)
		}
	}
	for site, n := range c.sites {
		want, ok := frozenNATSTransportSites[site]
		switch {
		case !ok:
			t.Errorf("unclassified transport site: %s in %s uses %s (%d×) — name its channel in frozenNATSTransportSites; a new broker-to-broker channel is a protocol change", site.pkg, site.fn, site.object, n)
		case want.n != n:
			t.Errorf("transport site %s in %s uses %s %d×, census says %d", site.pkg, site.fn, site.object, n, want.n)
		}
	}
	for site := range frozenNATSTransportSites {
		if c.sites[site] == 0 {
			t.Errorf("stale transport site: %s in %s no longer uses %s", site.pkg, site.fn, site.object)
		}
	}
}

// Every way a publication leaves a broker for its peers is censused. The raw publish sites
// sit behind two wrappers with one caller each. The JSA queue's sender also carries one
// live publication — the derived Sparkplug certificate — so every enqueue onto that queue,
// and every caller of its no-API send entry points, is censused with what it sends.
func TestLivePublicationWritersAreCensused(t *testing.T) {
	c := brokerProductionCensus(t)
	for wrapper, callers := range frozenPublishWrapperRefs {
		if fmt.Sprint(c.wrapperRefs[wrapper]) != fmt.Sprint(callers) {
			t.Errorf("references to %s: %v, want %v", wrapper, c.wrapperRefs[wrapper], callers)
		}
	}
	for ref, n := range c.queueMembers {
		if want := frozenJSAQueueMembers[ref]; want != n {
			t.Errorf("reference to the JSA queue's %s in %s: %d×, census says %d — the JSA sender publishes whatever the queue holds, so a new way to reach it is a new channel", ref.member, ref.fn, n, want)
		}
	}
	for ref, want := range frozenJSAQueueMembers {
		if c.queueMembers[ref] == 0 {
			t.Errorf("stale JSA queue reference: %s no longer references the queue's %s (census says %d×)", ref.fn, ref.member, want)
		}
	}
	for caller, n := range c.jsaSends {
		want, ok := frozenJSASendCallers[caller]
		switch {
		case !ok:
			t.Errorf("unclassified JSA send: %s calls %s (%d×) — say in frozenJSASendCallers whether it is an acknowledgement, a request or a live publication", caller.fn, caller.entry, n)
		case want.n != n:
			t.Errorf("JSA send: %s calls %s %d×, census says %d", caller.fn, caller.entry, n, want.n)
		}
	}
	for caller := range frozenJSASendCallers {
		if c.jsaSends[caller] == 0 {
			t.Errorf("stale JSA send: %s no longer calls %s", caller.fn, caller.entry)
		}
	}
}

// envelopeHeaderName is one Nmqtt-* header name as a whole literal.
var envelopeHeaderName = regexp.MustCompile(`^Nmqtt-[A-Za-z0-9]+$`)

// architectureHeaderRow is a row of the header table: its first cell is the name.
var architectureHeaderRow = regexp.MustCompile("^\\| `(Nmqtt-[A-Za-z0-9]+)` \\|")

func TestEnvelopeHeadersAreTheDocumentedOnes(t *testing.T) {
	// Every production package, not only those the transport census type-checks: a header
	// name defined in a package with no NATS dependency is still written by one that has.
	inCode := brokerProductionCensus(t).headerNames

	doc, err := os.ReadFile("../../docs/ARCHITECTURE.md")
	if err != nil {
		t.Fatal(err)
	}
	const heading = "### The broker-to-broker protocol"
	text := string(doc)
	start := strings.Index(text, "\n"+heading+"\n")
	if start < 0 {
		t.Fatalf("docs/ARCHITECTURE.md has no %q section", heading)
	}
	section := text[start+1:]
	if end := strings.Index(section[len(heading):], "\n#"); end >= 0 {
		section = section[:len(heading)+end]
	}
	inDoc := map[string]bool{}
	for _, line := range strings.Split(section, "\n") {
		if m := architectureHeaderRow.FindStringSubmatch(line); m != nil {
			inDoc[m[1]] = true
		}
	}

	if len(inCode) == 0 {
		t.Fatal("found no Nmqtt-* header literal in the production build: the scan reaches nothing")
	}
	for h := range inCode {
		if !inDoc[h] {
			t.Errorf("header %s is written or read by the code but missing from the header table in %q", h, heading)
		}
	}
	for h := range inDoc {
		if !inCode[h] {
			t.Errorf("header %s is in the header table in %q but not in the code", h, heading)
		}
	}
}

// The names two brokers must derive identically for one shared group, one affine
// session generation, one PUBREL generation and one topic. Literal values, never built
// from the constants: a change to any is a broker-to-broker protocol change (and, for
// the persisted ones, a persisted-format change) and bumps the format under its rules.
func TestCrossBrokerNamesAreThePinnedOnes(t *testing.T) {
	const why = " — peers of one format version must derive this identically; changing it is a protocol change (docs/ARCHITECTURE.md, \"The broker-to-broker protocol\")"
	check := func(what, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %q, want %q%s", what, got, want, why)
		}
	}

	q, d, dur := mqttSharedNames("grp", "sensors.temp", 7)
	check("shared queue group", q, "$mqttshare.JPih1A17gkOWPh2j")
	check("shared deliver subject", d, "$MQTT.qsub.JPih1A17gkOWPh2j_7")
	check("shared durable", dur, "$MQTT_SHARED_JPih1A17gkOWPh2j_7")
	check("shared raw QoS 0 queue group", mqttSharedQueueName("grp", "sensors.temp"), "$mqttshare.JPih1A17gkOWPh2j")

	f, d, dur := mqttQ2AffineNames("aBcD1234", "g5")
	check("affine copy filter", f, "$MQTT.out.q2.aBcD1234.g5.>")
	check("affine deliver subject", d, "$MQTT.deliver.q2.aBcD1234.g5")
	check("affine durable", dur, "$MQTT_Q2AFF_aBcD1234_g5")

	s, d, dur := mqttPubRelNames("aBcD1234", "g5")
	check("PUBREL subject", s, "$MQTT.out.pubrel.aBcD1234.g5")
	check("PUBREL deliver subject", d, "$MQTT.deliver.pubrel.aBcD1234.g5")
	check("PUBREL durable", dur, "$MQTT_PUBREL_aBcD1234_g5")

	for topic, want := range map[string]string{
		"a/b/c": "a.b.c",
		"/lead": "/.lead",
		"a//b":  "a./.b",
		"x.y":   "x//y",
	} {
		got, err := mqttTopicToNATSPubSubject([]byte(topic))
		if err != nil {
			t.Errorf("topic %q: %v", topic, err)
			continue
		}
		check(fmt.Sprintf("subject of topic %q", topic), string(got), want)
	}
	for filter, want := range map[string]string{
		"a/+/c":            "a.*.c",
		"#":                ">",
		"sensors/+/temp/#": "sensors.*.temp.>",
	} {
		got, err := mqttFilterToNATSSubject([]byte(filter))
		if err != nil {
			t.Errorf("filter %q: %v", filter, err)
			continue
		}
		check(fmt.Sprintf("subject of filter %q", filter), string(got), want)
	}
}
