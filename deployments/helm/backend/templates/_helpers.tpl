{{/*
backend.name: the all-in-one StatefulSet and the split gateway, and the Service
the grafana, producers, and prometheus charts point at. It is never truncated —
a silently shortened name would leave those charts pointing at nothing — so a
name over 52 characters fails the render: a StatefulSet's pods carry a
controller-revision-hash label of its name plus 11 characters, and a label
value is at most 63.
*/}}
{{- define "backend.name" -}}
{{- $name := default .Chart.Name .Values.fullnameOverride -}}
{{- if gt (len $name) 52 -}}
{{- fail (printf "fullnameOverride %q is %d characters; at most 52 are allowed (a StatefulSet name limit), and it is never truncated because other charts point at it" $name (len $name)) -}}
{{- end -}}
{{- $name | trimSuffix "-" -}}
{{- end -}}

{{- define "backend.labels" -}}
app.kubernetes.io/name: {{ include "backend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: backend
{{- end -}}

{{- define "backend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "backend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
backend.componentName: a split component's resource name. The gateway takes the
chart's own name — the Service the grafana, producers, and prometheus charts
point at — and every other component shares its prefix, truncated to 43
characters so the longest names still fit: "<prefix>-ingester" is at most the 52
a StatefulSet allows, and "<prefix>-ingester-headless" at most the 63 a Service
allows. Each component's suffix is distinct, so no two components collide; but
the gateway keeps fullnameOverride whole, so an override that is itself a
truncated prefix plus a component's suffix (43 characters, then "-store") would
give that component the gateway's name. That render fails rather than emit two
Services and two ConfigMaps of one name.
*/}}
{{- define "backend.componentName" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
{{- if eq $component "gateway" -}}
{{- include "backend.name" $root -}}
{{- else -}}
{{- $gateway := include "backend.name" $root -}}
{{- $name := printf "%s-%s" (trimSuffix "-backend" $gateway | trunc 43 | trimSuffix "-") $component -}}
{{- if eq $name $gateway -}}
{{- fail (printf "fullnameOverride %q collides with the %s component's name in split mode: its prefix, cut to 43 characters, plus \"-%s\" spells the override itself; choose another name" $gateway $component $component) -}}
{{- end -}}
{{- $name -}}
{{- end -}}
{{- end -}}

{{- define "backend.componentLabels" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
app.kubernetes.io/name: {{ include "backend.name" $root }}
app.kubernetes.io/instance: {{ $root.Release.Name }}
app.kubernetes.io/managed-by: {{ $root.Release.Service }}
app.kubernetes.io/component: {{ $component }}
{{- end -}}

{{- define "backend.componentSelectorLabels" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
app.kubernetes.io/name: {{ include "backend.name" $root }}
app.kubernetes.io/instance: {{ $root.Release.Name }}
app.kubernetes.io/component: {{ $component }}
{{- end -}}

{{/* backend.peerURL: the base URL another component reaches this one at. */}}
{{- define "backend.peerURL" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
{{- printf "http://%s:%d" (include "backend.componentName" (list $root $component)) ($root.Values.service.port | int) -}}
{{- end -}}

{{/*
backend.ingesterURLs: the ring's members, by the pod DNS names the StatefulSet's
headless Service gives them, for ordinals 0 to count-1. Called as (list $root
count). The querier reads every replica; the gateway writes to the first
backend.ingesterWriteCount of them, so its list is always a prefix of the
querier's and it never writes to an ingester the querier does not read. Both
come from this one helper (spec §7).
*/}}
{{- define "backend.ingesterURLs" -}}
{{- $root := index . 0 -}}
{{- $count := index . 1 -}}
{{- $name := include "backend.componentName" (list $root "ingester") -}}
{{- $urls := list -}}
{{- range $i := until (int $count) -}}
{{- $urls = append $urls (printf "http://%s-%d.%s-headless:%d" $name $i $name ($root.Values.service.port | int)) -}}
{{- end -}}
{{- join "," $urls -}}
{{- end -}}

{{/*
backend.ingesterWriteCount: split.ingester.writeReplicas, or every replica when
unset. A membership change raises replicas before writeReplicas (the querier
reads a new ingester before the gateway writes to it) and lowers writeReplicas
before replicas (the gateway stops writing to an ingester before the querier
stops reading it).
*/}}
{{- define "backend.ingesterWriteCount" -}}
{{- $v := .Values.split.ingester -}}
{{- if hasKey $v "writeReplicas" -}}{{ int $v.writeReplicas }}{{- else -}}{{ int $v.replicas }}{{- end -}}
{{- end -}}

{{/*
backend.querierReplicationFactor: split.querier.replicationFactor, or
split.replicationFactor when unset or null (--set ...=null drops it on a
--reuse-values upgrade). It differs from the gateway's only while an
RF change is staged: raising one, the gateway goes first; lowering one, the
querier goes first, so the querier's quorum never exceeds the gateway's.
*/}}
{{- define "backend.querierReplicationFactor" -}}
{{- $q := .Values.split.querier -}}
{{- if and (hasKey $q "replicationFactor") (not (kindIs "invalid" $q.replicationFactor)) -}}{{ int $q.replicationFactor }}{{- else -}}{{ int .Values.split.replicationFactor }}{{- end -}}
{{- end -}}

{{/* backend.quorum: a write's quorum at replication factor rf, rf/2+1, as config.Quorum computes it. */}}
{{- define "backend.quorum" -}}
{{- add (div (int .) 2) 1 -}}
{{- end -}}

{{/*
backend.splitConfigMap: one split component's ConfigMap, called as (list $root
component). split-configmaps.yaml renders all five from it, and each
workload's checksum/config hashes only its own, so a change to one
component's settings -- a ring stage changing the gateway's or the querier's
ingester list -- rolls that component alone, never the ingesters or the store.
*/}}
{{- define "backend.splitConfigMap" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
{{- $peers := dict "gateway" (list "ingester" "querier") "ingester" (list "store") "querier" (list "ingester" "store") "store" (list) "compactor" (list "store") -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "backend.componentName" (list $root $component) }}-config
  labels:
    {{- include "backend.componentLabels" (list $root $component) | nindent 4 }}
data:
  # The chart owns the target and the peer URLs (config.OBS_* refuses them),
  # rendering each peer from the Service that component's objects create.
  OBS_TARGET: {{ $component | quote }}
  OBS_HTTP_ADDR: {{ printf ":%d" ($root.Values.service.port | int) | quote }}
  {{- range $peer := index $peers $component }}
  {{- if and (eq $peer "ingester") (eq $component "gateway") }}
  OBS_INGESTER_URL: {{ include "backend.ingesterURLs" (list $root (include "backend.ingesterWriteCount" $root)) | quote }}
  {{- else if eq $peer "ingester" }}
  OBS_INGESTER_URL: {{ include "backend.ingesterURLs" (list $root $root.Values.split.ingester.replicas) | quote }}
  {{- else }}
  OBS_{{ upper $peer }}_URL: {{ include "backend.peerURL" (list $root $peer) | quote }}
  {{- end }}
  {{- end }}
  {{- if eq $component "gateway" }}
  OBS_REPLICATION_FACTOR: {{ $root.Values.split.replicationFactor | int | toString | quote }}
  {{- else if eq $component "querier" }}
  OBS_REPLICATION_FACTOR: {{ include "backend.querierReplicationFactor" $root | quote }}
  {{- end }}
  {{- range $key, $value := $root.Values.config }}
  {{ $key }}: {{ $value | quote }}
  {{- end }}
{{- end -}}

{{/*
backend.ingesterCountAnnotation: the pod-template annotation recording how many
ingesters a gateway pod writes to or a querier pod reads. It is what the running
pods loaded, which the staging check in split-configmaps.yaml compares against.
*/}}
{{- define "backend.ingesterCountAnnotation" -}}
observability-platform.dev/ingester-count
{{- end -}}

{{/*
backend.replicationFactorAnnotation: the pod-template annotation recording the
replication factor a gateway or querier pod loaded, which the staging check
compares against the same way.
*/}}
{{- define "backend.replicationFactorAnnotation" -}}
observability-platform.dev/replication-factor
{{- end -}}

{{/*
backend.ringStagingCheck: refuse a ring change that skips a stage. Called as
(list $root $live), where $live holds the live "gateway" and "querier"
Deployments and their "gatewayConfig" and "querierConfig" ConfigMaps, as
lookup returns them (empty when there is no live release).

The gateway and querier roll independently, so an old pod of one runs beside a
new pod of the other. Stay safe across that overlap: a new gateway may write
only to ingesters the old querier reads, and a new querier must still read
every ingester the old gateway writes to.

The previous lists are what the running pods loaded, not what a ConfigMap now
says: each Deployment's pod template records its ingester count
(backend.ingesterCountAnnotation), and a ring change is refused while either
Deployment is still rolling out, because its old pods may still hold an older
list. A release whose templates predate the annotation falls back to its
ConfigMaps. split.ingester.previous stands in for the live counts only where
there is no live release to look up (helm template); a real upgrade refuses it.

A replication factor change is staged the same way (spec §5.3): a read skips up
to quorum-1 ingesters, so it stays complete only while the querier's quorum is
at most the gateway's. Each pod template records its RF too
(backend.replicationFactorAnnotation), and the check refuses a new gateway
quorum below the running querier's and a new querier quorum above the running
gateway's, so an RF rises gateway first and falls querier first. An RF change
counts as a ring change for the rollout rule. A release whose templates predate
the RF annotation skips the RF part, and split.ingester.previous carries no RF:
a preview checks the ingester counts only.
*/}}
{{- define "backend.ringStagingCheck" -}}
{{- $root := index . 0 -}}
{{- $live := index . 1 -}}
{{- $replicas := int $root.Values.split.ingester.replicas -}}
{{- $writeCount := int (include "backend.ingesterWriteCount" $root) -}}
{{- $gwRF := int $root.Values.split.replicationFactor -}}
{{- $qRF := int (include "backend.querierReplicationFactor" $root) -}}
{{- $gwDep := $live.gateway -}}
{{- $qDep := $live.querier -}}
{{- $prev := dict -}}
{{- if and $gwDep $qDep -}}
{{- if $root.Values.split.ingester.previous -}}
{{- fail "split.ingester.previous is for previews only (helm template, which cannot look up the live release): remove it; an upgrade reads the running gateway and querier" -}}
{{- end -}}
{{- $key := include "backend.ingesterCountAnnotation" $root -}}
{{- $gwCount := dig "spec" "template" "metadata" "annotations" $key "" $gwDep -}}
{{- $qCount := dig "spec" "template" "metadata" "annotations" $key "" $qDep -}}
{{- $gw := $live.gatewayConfig -}}
{{- $q := $live.querierConfig -}}
{{- if and $gwCount $qCount -}}
{{- $prev = dict "replicas" (int $qCount) "writeReplicas" (int $gwCount) -}}
{{- else if and $gw $q $gw.data $q.data $gw.data.OBS_INGESTER_URL $q.data.OBS_INGESTER_URL -}}
{{- $prev = dict "replicas" (len (splitList "," $q.data.OBS_INGESTER_URL)) "writeReplicas" (len (splitList "," $gw.data.OBS_INGESTER_URL)) -}}
{{- end -}}
{{- $rfKey := include "backend.replicationFactorAnnotation" $root -}}
{{- /*
The running RF: the pod-template annotation, else the live ConfigMap's
OBS_REPLICATION_FACTOR, else 1 -- a release from before 6.3 has neither and
ran RF=1, so its heads may hold writes on one ingester only and its RF change
is staged like any other.
*/}}
{{- $liveGwRF := dig "spec" "template" "metadata" "annotations" $rfKey "" $gwDep -}}
{{- if not $liveGwRF -}}{{- $liveGwRF = dig "data" "OBS_REPLICATION_FACTOR" "1" ($gw | default dict) -}}{{- end -}}
{{- $liveQRF := dig "spec" "template" "metadata" "annotations" $rfKey "" $qDep -}}
{{- if not $liveQRF -}}{{- $liveQRF = dig "data" "OBS_REPLICATION_FACTOR" "1" ($q | default dict) -}}{{- end -}}
{{- $rfChanged := or (ne $gwRF (int $liveGwRF)) (ne $qRF (int $liveQRF)) -}}
{{- if or $rfChanged (and $prev (or (ne $writeCount (int $prev.writeReplicas)) (ne $replicas (int $prev.replicas)))) -}}
{{- range $dep := list $gwDep $qDep -}}
{{- $want := int (dig "spec" "replicas" 1 $dep) -}}
{{- $generation := int (dig "metadata" "generation" 0 $dep) -}}
{{- $observed := int (dig "status" "observedGeneration" 0 $dep) -}}
{{- $total := int (dig "status" "replicas" 0 $dep) -}}
{{- $updated := int (dig "status" "updatedReplicas" 0 $dep) -}}
{{- $available := int (dig "status" "availableReplicas" 0 $dep) -}}
{{- if or (lt $observed $generation) (ne $updated $want) (ne $total $updated) (lt $available $updated) -}}
{{- fail (printf "ring change refused: deployment/%s has not finished rolling out (%d of %d replicas updated, %d running, %d available), so its old pods may still hold an older ingester list or replication factor. Wait for `kubectl rollout status deployment/%s` to succeed, or helm rollback, then retry" $dep.metadata.name $updated $want $total $available $dep.metadata.name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $gwQuorum := int (include "backend.quorum" $gwRF) -}}
{{- $qQuorum := int (include "backend.quorum" $qRF) -}}
{{- $liveGwQuorum := int (include "backend.quorum" (int $liveGwRF)) -}}
{{- $liveQQuorum := int (include "backend.quorum" (int $liveQRF)) -}}
{{- if lt $gwQuorum $liveQQuorum -}}
{{- fail (printf "unstaged replication factor change: the gateway would write at quorum %d (split.replicationFactor=%d) while the running querier reads at quorum %d (RF %d), so a read could skip every ingester holding a write. Lower the RF querier first: set split.querier.replicationFactor=%d with split.replicationFactor=%d, and once that has rolled out, lower split.replicationFactor" $gwQuorum $gwRF $liveQQuorum (int $liveQRF) $qRF (int $liveGwRF)) -}}
{{- end -}}
{{- if gt $qQuorum $liveGwQuorum -}}
{{- fail (printf "unstaged replication factor change: the querier would read at quorum %d (split.querier.replicationFactor=%d) while the running gateway writes at quorum %d (RF %d), so a read could skip every ingester holding a write. Raise the RF gateway first: set split.replicationFactor=%d with split.querier.replicationFactor=%d, and raise the querier only once that has rolled out and the ingesters' heads have flushed (the maintenance flush interval, or a drain)" $qQuorum $qRF $liveGwQuorum (int $liveGwRF) $gwRF (int $liveQRF)) -}}
{{- end -}}
{{- else -}}
{{- with $root.Values.split.ingester.previous -}}
{{- $prev = dict "replicas" (int .replicas) "writeReplicas" (int .writeReplicas) -}}
{{- end -}}
{{- end -}}
{{- if $prev -}}
{{- if gt $writeCount (int $prev.replicas) -}}
{{- fail (printf "unstaged ring change: the gateway would write to %d ingesters while the running querier reads only %d. Add an ingester in two upgrades: first split.ingester.replicas=%d with split.ingester.writeReplicas=%d, then raise writeReplicas" $writeCount (int $prev.replicas) $replicas (int $prev.replicas)) -}}
{{- end -}}
{{- if lt $replicas (int $prev.writeReplicas) -}}
{{- fail (printf "unstaged ring change: the querier would read %d ingesters while the running gateway writes to %d. Remove an ingester in stages: first lower split.ingester.writeReplicas to %d, drain the ingester (POST /internal/v1/drain answers 200) and scale it down, then lower replicas" $replicas (int $prev.writeReplicas) $replicas) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "backend.podSecurityContext" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
{{- end -}}

{{/*
backend.splitContainer: one component's container, as the all-in-one
StatefulSet's container is written: httpGet probes (kubelet probes from
outside, so the distroless no-shell constraint does not apply), liveness on
/healthz so a full volume cannot restart-loop, and the hardened security
context. Stateful components keep the long startup budget WAL replay and
block validation can need; stateless ones start in seconds.
*/}}
{{- define "backend.splitContainer" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
{{- $stateful := index . 2 -}}
{{- $resources := index . 3 -}}
image: "{{ $root.Values.image.repository }}:{{ $root.Values.image.tag }}"
imagePullPolicy: {{ $root.Values.image.pullPolicy }}
ports:
  - name: http
    containerPort: {{ $root.Values.service.port }}
    protocol: TCP
envFrom:
  - configMapRef:
      name: {{ include "backend.componentName" (list $root $component) }}-config
startupProbe:
  httpGet:
    path: /readyz
    port: http
  periodSeconds: {{ ternary $root.Values.startupProbe.periodSeconds 2 $stateful }}
  failureThreshold: {{ ternary $root.Values.startupProbe.failureThreshold 15 $stateful }}
readinessProbe:
  httpGet:
    path: /readyz
    port: http
  periodSeconds: 10
livenessProbe:
  httpGet:
    path: /healthz
    port: http
  periodSeconds: 20
resources:
  {{- toYaml $resources | nindent 2 }}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop:
      - ALL
{{- if $stateful }}
volumeMounts:
  - name: data
    mountPath: {{ $root.Values.config.OBS_DATA_DIR }}
{{- end }}
{{- end -}}
