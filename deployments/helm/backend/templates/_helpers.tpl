{{- define "backend.name" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
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
point at — and every other component shares its prefix.
*/}}
{{- define "backend.componentName" -}}
{{- $root := index . 0 -}}
{{- $component := index . 1 -}}
{{- if eq $component "gateway" -}}
{{- include "backend.name" $root -}}
{{- else -}}
{{- printf "%s-%s" (trimSuffix "-backend" (include "backend.name" $root)) $component | trunc 63 | trimSuffix "-" -}}
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
