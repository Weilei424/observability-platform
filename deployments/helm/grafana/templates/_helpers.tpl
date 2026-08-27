{{- define "grafana.name" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "grafana.labels" -}}
app.kubernetes.io/name: {{ include "grafana.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: grafana
{{- end -}}

{{- define "grafana.selectorLabels" -}}
app.kubernetes.io/name: {{ include "grafana.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- /* The Secret holding the admin password: either one the operator made, or ours. */ -}}
{{- define "grafana.secretName" -}}
{{- if .Values.admin.existingSecret -}}
{{- .Values.admin.existingSecret -}}
{{- else -}}
{{- include "grafana.name" . }}-admin
{{- end -}}
{{- end -}}
