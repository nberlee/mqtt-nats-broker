// Package operational is the one definition of the broker's operational interface: what
// /healthz and /readyz answer, the exit statuses, and the JSON log records an operator
// can alert on (docs/ARCHITECTURE.md, "The operational interface"). The probes, main and
// every producer of a frozen record read it; testdata/interface.txt pins it and
// docs/OPERATIONS.md states it, both rendered from here.
package operational

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// The probe endpoints, served on -http-addr.
const (
	HealthzPath = "/healthz"
	ReadyzPath  = "/readyz"
)

// Healthz is /healthz's one answer: the health server answers it whenever it serves, from
// its bind (before the embedded server starts and NATS is dialed) until it is shut down
// after the backend has closed.
func Healthz() (status int, body string) { return http.StatusOK, "ok" }

// Readiness is what the broker has done so far: it has not served yet, or it serves. A
// requested stop is not a stored state; the probe reads it from the signal context, so no
// write can order itself before or after it.
type Readiness int32

const (
	// Starting holds from the bind until the broker first serves, a failed start's
	// unwinding included.
	Starting Readiness = iota
	// Serving holds once the MQTT listener is up and TLS reloading has started.
	Serving
)

// readyzRule is one /readyz answer and the condition it is given under. Readyz answers
// the first rule that matches, and docs/OPERATIONS.md's readiness table is these rules.
type readyzRule struct {
	when   string
	match  func(r Readiness, stopped, connected bool) bool
	status int
	body   string
}

var readyzRules = []readyzRule{
	{"a shutdown signal has been received, whether or not the broker served",
		func(_ Readiness, stopped, _ bool) bool { return stopped },
		http.StatusServiceUnavailable, "draining"},
	{"the broker has not served yet: the JetStream wait, the dial, the stream checks, the MQTT initialization and its retries, and a failed start's unwinding",
		func(r Readiness, _, _ bool) bool { return r == Starting },
		http.StatusServiceUnavailable, "starting"},
	{"the broker serves and its NATS service connection is connected",
		func(_ Readiness, _, connected bool) bool { return connected },
		http.StatusOK, "ready"},
	{"the broker serves and its NATS service connection is down",
		func(_ Readiness, _, _ bool) bool { return true },
		http.StatusServiceUnavailable, "nats disconnected"},
}

// Readyz is /readyz's answer. stopped is a shutdown signal having been received;
// connected is consulted only once the broker serves. 200 ready claims the listener is up,
// no stop was requested and the service connection is connected, not that the service
// identity is known.
func Readyz(r Readiness, stopped, connected bool) (status int, body string) {
	for _, rule := range readyzRules {
		if rule.match(r, stopped, connected) {
			return rule.status, rule.body
		}
	}
	panic("unreachable: the last readiness rule matches everything")
}

// The exit statuses main gives, and the stderr prefix of the failure it reports.
const (
	ExitStopped = 0
	ExitFailed  = 1
	FatalPrefix = "fatal: "
)

type exitRule struct {
	status  int
	meaning string
}

var exitRules = []exitRule{
	{ExitStopped, "the broker stopped as asked: a shutdown signal, at any phase after its handlers are registered, and the shutdown or the startup's unwinding returned; also `-h`. Not a promise that every buffered publication reached NATS: the service connection's close is bounded"},
	{ExitFailed, "the broker could not start, or the embedded NATS server stopped it: `fatal: <reason>` on stderr, except the embedded server's own fatal error, which is an `ERROR` record with `component: nats-server`. A startup step that does not observe a signal (the NATS dial, the first service-identity request, both bounded) reports its own failure here"},
}

// The JSON log envelope every record carries.
const (
	KeyTime             = slog.TimeKey
	KeyLevel            = slog.LevelKey
	KeyMsg              = slog.MessageKey
	KeyComponent        = "component"
	ComponentNATSServer = "nats-server"
)

type envelopeRule struct {
	key     string
	meaning string
}

var envelopeRules = []envelopeRule{
	{KeyTime, "RFC 3339 with fractional seconds, in the host's time zone (UTC in the published images)"},
	{KeyLevel, "`DEBUG`, `INFO`, `WARN` or `ERROR`"},
	{KeyMsg, "the record's message; frozen only for the records below"},
	{KeyComponent, "`nats-server` on the embedded NATS server's records, absent otherwise"},
}

// Kind is a frozen field's JSON encoding.
type Kind int

const (
	String   Kind = iota // a JSON string
	Integer              // a JSON number
	Bool                 // a JSON boolean
	Duration             // a JSON number of nanoseconds
	Time                 // an RFC 3339 JSON string
	Text                 // a JSON string whose wording is not frozen (an error, a reason)
)

var kindNames = [...]string{String: "string", Integer: "integer", Bool: "boolean", Duration: "duration", Time: "time", Text: "text"}

var kindEncodings = [...]string{
	String:   "a JSON string",
	Integer:  "a JSON number",
	Bool:     "a JSON boolean",
	Duration: "a JSON number of nanoseconds",
	Time:     "an RFC 3339 JSON string",
	Text:     "a JSON string; its wording is not part of the interface",
}

func (k Kind) String() string { return kindNames[k] }

// Value is one field's value, built by the constructor of its kind: a producer cannot hand
// a frozen field a type its kind does not encode.
type Value struct {
	kind Kind
	v    slog.Value
}

// Str is a String value.
func Str(s string) Value { return Value{String, slog.StringValue(s)} }

// Addr is a String value: an address written as its text, never as a JSON object.
func Addr(a fmt.Stringer) Value { return Value{String, slog.StringValue(a.String())} }

// Int is an Integer value.
func Int(i int64) Value { return Value{Integer, slog.Int64Value(i)} }

// Uint is an Integer value.
func Uint(u uint64) Value { return Value{Integer, slog.Uint64Value(u)} }

// Flag is a Bool value.
func Flag(b bool) Value { return Value{Bool, slog.BoolValue(b)} }

// Dur is a Duration value.
func Dur(d time.Duration) Value { return Value{Duration, slog.DurationValue(d)} }

// At is a Time value.
func At(t time.Time) Value { return Value{Time, slog.StringValue(t.Format(time.RFC3339Nano))} }

// Reason is a Text value.
func Reason(s string) Value { return Value{Text, slog.StringValue(s)} }

// Err is a Text value: the error's text.
func Err(err error) Value {
	if err == nil {
		return Value{Text, slog.StringValue("")}
	}
	return Value{Text, slog.StringValue(err.Error())}
}

// Field is one key of a frozen record and its kind.
type Field struct {
	Key  string
	Kind Kind
}

// Record is a frozen log record: its message, level and fields are the interface, and
// When says when it is written.
type Record struct {
	Msg    string
	Level  slog.Level
	Fields []Field
	When   string
}

// Misused fields are still written, marked, so a record is never lost to a producer's
// mistake; the call-site census rejects both before a change lands.
const (
	BadValue = "!BADVALUE"
	Missing  = "!MISSING"
)

// Log writes the record to log, pairing Fields[i] with values[i].
func (r Record) Log(log *slog.Logger, values ...Value) {
	attrs := make([]slog.Attr, 0, len(r.Fields))
	for i, f := range r.Fields {
		switch {
		case i >= len(values):
			attrs = append(attrs, slog.String(f.Key, Missing))
		case values[i].kind != f.Kind:
			attrs = append(attrs, slog.String(f.Key, BadValue))
		default:
			attrs = append(attrs, slog.Attr{Key: f.Key, Value: values[i].v})
		}
	}
	log.LogAttrs(context.Background(), r.Level, r.Msg, attrs...)
}

func fields(kv ...any) []Field {
	out := make([]Field, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		out = append(out, Field{kv[i].(string), kv[i+1].(Kind)})
	}
	return out
}

// The frozen records, in docs/OPERATIONS.md's order.
var (
	BrokerStarting = Record{"starting mqtt-nats-broker", slog.LevelInfo,
		fields("version", String), "once, first"}
	HealthServerListening = Record{"health server listening", slog.LevelInfo,
		fields("addr", String), "once, when the health port is bound"}
	HealthServerError = Record{"health server error", slog.LevelError,
		fields("err", Text), "when the health server stops serving other than at shutdown"}
	ProfilerListening = Record{"profiler listening", slog.LevelInfo,
		fields("addr", String), "once, with `-pprof-addr`"}
	ProfilerBindFailed = Record{"profiler listener could not bind", slog.LevelError,
		fields("addr", String, "err", Text), "once, with `-pprof-addr`; the broker starts without the profiler"}
	ProfilerError = Record{"profiler error", slog.LevelError,
		fields("err", Text), "when the profiler stops serving other than at shutdown"}
	EmbeddedNATSReady = Record{"embedded NATS server ready", slog.LevelInfo,
		fields("server", String, "config", String), "once, in the embedded topology"}
	WaitingForJetStream = Record{"waiting for JetStream", slog.LevelInfo,
		fields("err", Text), "at most every 10 s while the embedded server's JetStream does not answer"}
	ConnectedToNATS = Record{"connected to NATS backend", slog.LevelInfo,
		fields("urls", String, "in_process", Bool, "server", String, "server_version", String,
			"jetstream_api_level", Integer, "cluster", String, "js_domain", String),
		"once, when the service connection passed the NATS floor and JetStream answered"}
	MQTTInitializationRetry = Record{"MQTT initialization did not complete; retrying", slog.LevelWarn,
		fields("attempt", Integer, "err", Text), "at most every 10 s while the MQTT initialization is retried"}
	StartupStopped = Record{"shutdown signal received during startup", slog.LevelInfo,
		fields("err", Text), "once, when a shutdown signal ends startup before the broker served"}
	MQTTListenerStarted = Record{"MQTT listener started", slog.LevelInfo,
		fields("addr", String, "node", String, "tls", Bool), "once, when the broker serves"}
	WebSocketListenerStarted = Record{"MQTT over WebSocket listener started", slog.LevelInfo,
		fields("addr", String, "tls", Bool), "once, with `-ws-port`"}
	ShutdownSignalReceived = Record{"shutdown signal received, draining", slog.LevelInfo,
		nil, "once, at the first shutdown signal after the broker served"}
	DrainDelayWaiting = Record{"draining, waiting for endpoint deregistration", slog.LevelInfo,
		fields("delay", Duration), "once, with `-drain-delay`"}
	ServiceConnectionError = Record{"NATS service connection error", slog.LevelWarn,
		fields("err", Text), "per asynchronous error the service connection reports"}
	ServiceConnectionUnidentified = Record{"MQTT: the service connection reconnected but could not be identified; no CONNECT is accepted", slog.LevelError,
		fields("err", Text), "per reconnect whose identity request fails; `/readyz` still answers `200 ready`"}
	ServiceConnectionDroppedWhileIdentifying = Record{"MQTT: the service connection dropped while it was being re-identified; the account stays unknown", slog.LevelWarn,
		nil, "per occurrence"}
	ServiceAccountChanged = Record{"MQTT: the service connection reconnected into another account; disconnecting every auth-passthrough client", slog.LevelError,
		fields("account", String, "previous_account", String), "per occurrence"}
	ConnectRefusedAccountUnknown = Record{"MQTT: refusing CONNECT: the service connection's account is not currently established", slog.LevelWarn,
		nil, "per CONNECT refused for it"}
	ConnectRefused = Record{"MQTT: refusing CONNECT", slog.LevelWarn,
		fields("client_id", String, "reason", Text), "per CONNECT whose identity cannot be established or is bound to another account"}
	TLSReloaded = Record{"MQTT TLS configuration reloaded", slog.LevelInfo,
		fields("subject", String, "serial", String, "not_after", Time, "generation", Integer), "per applied TLS reload"}
	TLSReloadRefused = Record{"MQTT TLS reload refused; keeping the active configuration", slog.LevelError,
		fields("err", Text), "once per run of identical refusals, not per attempt"}
	TLSCertificateExpired = Record{"MQTT TLS certificate has expired and no valid replacement is on disk", slog.LevelWarn,
		fields("subject", String, "serial", String, "not_after", Time), "once per served certificate, not repeated"}
	ServiceConnectionCloseTimedOut = Record{"NATS service connection did not finish closing in time; closing it now", slog.LevelWarn,
		fields("wait", Duration), "at most once, at shutdown"}
	ServiceConnectionClosed = Record{"NATS service connection closed", slog.LevelInfo,
		nil, "once, at shutdown"}
)

// Records is every frozen record, in docs/OPERATIONS.md's order. A record that is not here
// is not part of the interface, whatever it says.
var Records = []*Record{
	&BrokerStarting, &HealthServerListening, &HealthServerError,
	&ProfilerListening, &ProfilerBindFailed, &ProfilerError,
	&EmbeddedNATSReady, &WaitingForJetStream, &ConnectedToNATS,
	&MQTTInitializationRetry, &StartupStopped,
	&MQTTListenerStarted, &WebSocketListenerStarted,
	&ShutdownSignalReceived, &DrainDelayWaiting,
	&ServiceConnectionError, &ServiceConnectionUnidentified,
	&ServiceConnectionDroppedWhileIdentifying, &ServiceAccountChanged,
	&ConnectRefusedAccountUnknown, &ConnectRefused,
	&TLSReloaded, &TLSReloadRefused, &TLSCertificateExpired,
	&ServiceConnectionCloseTimedOut, &ServiceConnectionClosed,
}
