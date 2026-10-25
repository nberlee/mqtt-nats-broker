{{/*
The chart name.
*/}}
{{- define "mqtt-nats-broker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
The fully qualified name. If the release name contains the chart name, it is used alone.
*/}}
{{- define "mqtt-nats-broker.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "mqtt-nats-broker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mqtt-nats-broker.labels" -}}
helm.sh/chart: {{ include "mqtt-nats-broker.chart" . }}
{{ include "mqtt-nats-broker.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "mqtt-nats-broker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mqtt-nats-broker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end }}

{{- define "mqtt-nats-broker.embedded" -}}
{{- eq .Values.topology "embedded" | ternary "true" "" }}
{{- end }}

{{- define "mqtt-nats-broker.clustered" -}}
{{- and (eq .Values.topology "embedded") .Values.embedded.cluster.enabled | ternary "true" "" }}
{{- end }}

{{/*
The rendered name of the optional NATS dependency's client Service, computed the way
that chart computes it (its nats.fullname and nats.defaultValues): service.name, else
fullnameOverride, else the release name, suffixed with nameOverride or "nats" unless the
release name already contains it. The dependency's own nats.defaultValues fills in
service.name in the values both charts share, and in Helm's render order it runs first,
so the computed branches are the fallback for a render that reaches this helper before it.
*/}}
{{- define "mqtt-nats-broker.natsServiceName" -}}
{{- $nats := .Values.nats }}
{{- if ($nats.service).name }}
{{- $nats.service.name }}
{{- else if $nats.fullnameOverride }}
{{- $nats.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "nats" $nats.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
The broker's NATS URLs: broker.natsURLs, else the dependency's client Service, in its own
namespace when it overrides one, on its client port, tls:// when that port serves TLS.
*/}}
{{- define "mqtt-nats-broker.natsURLs" -}}
{{- if .Values.broker.natsURLs }}
{{- .Values.broker.natsURLs }}
{{- else }}
{{- $nats := .Values.nats }}
{{- $host := include "mqtt-nats-broker.natsServiceName" . }}
{{- with $nats.namespaceOverride }}
{{- if ne . $.Release.Namespace }}
{{- $host = printf "%s.%s" $host . }}
{{- end }}
{{- end }}
{{- $client := (($nats.config).nats) | default dict }}
{{- $scheme := ($client.tls).enabled | ternary "tls" "nats" }}
{{- printf "%s://%s:%v" $scheme $host ($client.port | default 4222) }}
{{- end }}
{{- end }}

{{/*
The broker image: the embedded topology's tag carries the "-embedded" suffix.
*/}}
{{- define "mqtt-nats-broker.image" -}}
{{- with .Values.image }}
{{- $image := "" }}
{{- if .fullImageName }}
{{- $image = .fullImageName }}
{{- else if .digest }}
{{- $image = printf "%s@%s" .repository .digest }}
{{- else }}
{{- $tag := .tag | default $.Chart.AppVersion }}
{{- if include "mqtt-nats-broker.embedded" $ }}
{{- $tag = printf "%s-embedded" $tag }}
{{- end }}
{{- $image = printf "%s:%s" .repository $tag }}
{{- end }}
image: {{ $image | quote }}
{{- with .pullPolicy }}
imagePullPolicy: {{ . }}
{{- end }}
{{- end }}
{{- end }}

{{/*
An env map (NAME: value, or NAME: {valueFrom: ...}) as an env list.
*/}}
{{- define "mqtt-nats-broker.env" -}}
{{- range $k, $v := . }}
{{- if kindIs "string" $v }}
- name: {{ $k | quote }}
  value: {{ $v | quote }}
{{- else if kindIs "map" $v }}
- {{ merge (dict "name" $k) $v | toYaml | nindent 2 }}
{{- else }}
{{- fail (cat "env var" $k "must be string or map, got" (kindOf $v)) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Refuse the combinations the broker or the topology cannot run.
*/}}
{{- define "mqtt-nats-broker.validate" -}}
{{- with .Values }}
{{- if not (has .topology (list "external" "embedded")) }}
{{- fail (printf "topology must be external or embedded, not %q" (toString .topology)) }}
{{- end }}
{{- if not .broker.credentials.secretName }}
{{- fail "broker.credentials.secretName: the broker needs its NATS service identity" }}
{{- end }}
{{- if and .mqtt.tls.enabled (not .mqtt.tls.secretName) }}
{{- fail "mqtt.tls.secretName: TLS needs a kubernetes.io/tls Secret" }}
{{- end }}
{{- if and .mqtt.tls.plaintext (not .mqtt.tls.enabled) }}
{{- fail "mqtt.tls.plaintext exists only beside TLS: without TLS, mqtt.port already serves plaintext" }}
{{- end }}
{{- if eq .topology "external" }}
{{- if and (not .broker.natsURLs) (not .nats.enabled) }}
{{- fail "broker.natsURLs: the external topology needs a NATS to connect to (or nats.enabled)" }}
{{- end }}
{{- if and .nats.enabled (not .broker.natsURLs) }}
{{- if eq (toString ((.nats.service).enabled)) "false" }}
{{- fail "nats.service.enabled is false: the broker reaches the dependency through its client Service; set broker.natsURLs" }}
{{- end }}
{{- if eq (toString ((((.nats.service).ports).nats).enabled)) "false" }}
{{- fail "nats.service.ports.nats.enabled is false: the broker reaches the dependency through its client port; set broker.natsURLs" }}
{{- end }}
{{- end }}
{{- if .nats.enabled }}
{{- $cluster := ((.nats.config).cluster) | default dict }}
{{- if $cluster.enabled }}
{{- $authorized := or ((($cluster.merge) | default dict).authorization) (and (($cluster.routeURLs).user) (($cluster.routeURLs).password)) (($cluster.tls).enabled) }}
{{- if not $authorized }}
{{- fail "nats.config.cluster: the dependency's routes are unauthenticated; anyone who reaches the route port joins the cluster and reads and injects account traffic past every client ACL (see ci/optional-nats-cluster-values.yaml)" }}
{{- end }}
{{- end }}
{{- range $var := list "MQTTNB_NATS_USER" "MQTTNB_NATS_PASS" "MQTTNB_DEFAULT_USER" "MQTTNB_DEFAULT_PASS" }}
{{- $ref := ((((get $.Values.nats.container.env $var) | default dict).valueFrom).secretKeyRef) | default dict }}
{{- if ne (toString $ref.name) (toString $.Values.broker.credentials.secretName) }}
{{- fail (printf "nats.container.env.%s must come from broker.credentials.secretName (%s), the Secret the broker reads" $var $.Values.broker.credentials.secretName) }}
{{- end }}
{{- end }}
{{- end }}
{{- else }}
{{- if .nats.enabled }}
{{- fail "nats.enabled: the embedded topology runs its own JetStream; the dependency is for the external topology" }}
{{- end }}
{{- if .autoscaling.enabled }}
{{- fail "autoscaling: the embedded topology's replica count is a JetStream quorum, not a load setting" }}
{{- end }}
{{- if .embedded.cluster.enabled }}
{{- if or (lt (int .replicas) 3) (eq (mod (int .replicas) 2) 0) }}
{{- fail (printf "replicas: an embedded cluster needs an odd count of at least 3, not %v" .replicas) }}
{{- end }}
{{- if not .embedded.cluster.routeSecretName }}
{{- fail "embedded.cluster.routeSecretName: the routes are authenticated" }}
{{- end }}
{{- else if ne (int .replicas) 1 }}
{{- fail (printf "replicas: an unclustered embedded server is one pod, not %v (set embedded.cluster.enabled)" .replicas) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Set default values, then resolve $tplYaml over them, once per render (the NATS chart's
nats.defaultValues). Every template includes it first.
*/}}
{{- define "mqtt-nats-broker.defaultValues" }}
{{- if not .defaultValuesSet }}
  {{- $name := include "mqtt-nats-broker.fullname" . }}
  {{- with .Values }}
    {{- $replicas := or (eq .topology "external") (.embedded.cluster.enabled) | ternary 3 1 }}
    {{- $_ := set . "replicas" (.replicas | default $replicas) }}
    {{- $_ := set .deployment          "name" (.deployment.name          | default $name) }}
    {{- $_ := set .statefulSet         "name" (.statefulSet.name         | default $name) }}
    {{- $_ := set .service             "name" (.service.name             | default $name) }}
    {{- $_ := set .headlessService     "name" (.headlessService.name     | default (printf "%s-headless" $name)) }}
    {{- $_ := set .configMap           "name" (.configMap.name           | default (printf "%s-config" $name)) }}
    {{- $_ := set .podDisruptionBudget "name" (.podDisruptionBudget.name | default $name) }}
    {{- $_ := set .autoscaling         "name" (.autoscaling.name         | default $name) }}
    {{- $_ := set .networkPolicy       "name" (.networkPolicy.name       | default $name) }}
    {{- $grace := eq .topology "embedded" | ternary 75 60 }}
    {{- $_ := set .podTemplate "terminationGracePeriodSeconds" (.podTemplate.terminationGracePeriodSeconds | default $grace) }}
  {{- end }}
  {{- include "mqtt-nats-broker.validate" . }}

  {{- /* nats: is the dependency's own values: its $tplYaml is resolved by that chart, in
  its context, so this chart's pass leaves it untouched. */}}
  {{- $values := get (include "mqtt-nats-broker.tplYaml" (dict "doc" (omit .Values "nats") "ctx" $) | fromJson) "doc" }}
  {{- $_ := set $values "nats" .Values.nats }}
  {{- $_ := set . "Values" $values }}
  {{- $_ := set . "defaultValuesSet" true }}
{{- end }}
{{- end }}

{{/*
mqtt-nats-broker.loadMergePatch
input: map with 4 keys:
- file: name of file to load
- ctx: context to pass to tpl
- merge: interface{} to merge
- patch: []interface{} valid JSON Patch document
output: the object as YAML
*/}}
{{- define "mqtt-nats-broker.loadMergePatch" -}}
{{- $doc := tpl (.ctx.Files.Get (printf "files/%s" .file)) .ctx | fromYaml | default dict -}}
{{- $doc = mergeOverwrite $doc (deepCopy (.merge | default dict)) -}}
{{- get (include "mqtt-nats-broker.jsonpatch" (dict "doc" $doc "patch" (.patch | default list)) | fromJson ) "doc" | toYaml -}}
{{- end }}
