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
backend.ingesterURLs: the ring's members, one per ingester replica, by the pod
DNS names the StatefulSet's headless Service gives them. The gateway and querier
both render it, so their lists always match (spec §7).
*/}}
{{- define "backend.ingesterURLs" -}}
{{- $root := . -}}
{{- $name := include "backend.componentName" (list $root "ingester") -}}
{{- $urls := list -}}
{{- range $i := until (int $root.Values.split.ingester.replicas) -}}
{{- $urls = append $urls (printf "http://%s-%d.%s-headless:%d" $name $i $name ($root.Values.service.port | int)) -}}
{{- end -}}
{{- join "," $urls -}}
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
