{{/*
Standard naming + label helpers — match Bitnami / kube-prometheus-stack
conventions so customers get predictable resource names.
*/}}

{{- define "vizgrams.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "vizgrams.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "vizgrams.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "vizgrams.labels" -}}
helm.sh/chart: {{ include "vizgrams.chart" . }}
{{ include "vizgrams.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "vizgrams.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vizgrams.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "vizgrams.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "vizgrams.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image reference. Falls back to chart appVersion so a `helm install` with
no values pins to a tested image pair (chart + image at same version).
*/}}
{{- define "vizgrams.image" -}}
{{- $tag := .tag | default .root.Chart.AppVersion -}}
{{- printf "%s/%s:%s" .root.Values.image.registry .name $tag -}}
{{- end -}}

{{/*
Common env shared by API + batch — auth secrets, ClickHouse, models.
*/}}
{{- define "vizgrams.commonEnv" -}}
- name: VZ_MODELS_DIR
  value: /models
- name: VZ_SYSTEM_ADMINS
  value: {{ .Values.auth.systemAdmins | quote }}
{{- if .Values.clickhouse.enabled }}
- name: VZ_DATABASE_BACKEND
  value: clickhouse
- name: CLICKHOUSE_HOST
  value: {{ .Values.clickhouse.host | quote }}
- name: CLICKHOUSE_PORT
  value: {{ .Values.clickhouse.port | quote }}
- name: CLICKHOUSE_USER
  value: {{ .Values.clickhouse.user | quote }}
{{- if .Values.clickhouse.existingSecret }}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.clickhouse.existingSecret }}
      key: {{ .Values.clickhouse.existingSecretKey }}
{{- end }}
{{- end }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "vizgrams.commonEnvFrom" -}}
{{- if .Values.auth.existingSecret }}
- secretRef:
    name: {{ .Values.auth.existingSecret }}
{{- end }}
{{- with .Values.extraEnvFrom }}
{{ toYaml . }}
{{- end }}
{{- end -}}
