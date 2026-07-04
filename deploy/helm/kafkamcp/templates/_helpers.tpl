{{/*
Expand the name of the chart.
*/}}
{{- define "kafkamcp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "kafkamcp.fullname" -}}
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

{{/*
Whether the selected configuration requires exactly one running pod. Guarded
packs keep approval state process-local, and SQLite is a single-writer backend.
*/}}
{{- define "kafkamcp.requiresSingleton" -}}
{{- $packs := default (list "core-read") .Values.config.server.tool_packs -}}
{{- $guarded := or (has "ops-write" $packs) (has "governance" $packs) (has "ops" $packs) -}}
{{- $configPersistence := default (dict) .Values.config.server.persistence -}}
{{- $sqliteConfigured := or (eq (lower (default "" $configPersistence.backend)) "sqlite") (eq (lower (default "" $configPersistence.backend)) "sqlite3") -}}
{{- if or $guarded .Values.persistence.enabled $sqliteConfigured -}}true{{- else -}}false{{- end -}}
{{- end }}

{{/*
Fail rendering for deployment shapes that would violate approval/idempotency
or SQLite single-writer guarantees.
*/}}
{{- define "kafkamcp.validateValues" -}}
{{- $singleton := eq (include "kafkamcp.requiresSingleton" . | trim) "true" -}}
{{- $configPersistence := default (dict) .Values.config.server.persistence -}}
{{- $sqliteConfigured := or (eq (lower (default "" $configPersistence.backend)) "sqlite") (eq (lower (default "" $configPersistence.backend)) "sqlite3") -}}
{{- if and $sqliteConfigured (not .Values.persistence.enabled) -}}
{{- fail "config.server.persistence.backend=sqlite requires persistence.enabled=true so a writable PVC is mounted" -}}
{{- end -}}
{{- if and $singleton (ne (int .Values.replicaCount) 1) -}}
{{- fail "guarded tool packs and SQLite persistence require replicaCount=1" -}}
{{- end -}}
{{- if and $singleton .Values.autoscaling.enabled -}}
{{- fail "autoscaling must be disabled when guarded tool packs or SQLite persistence are enabled" -}}
{{- end -}}
{{- if and (ne .Values.service.sessionAffinity "None") (ne .Values.service.sessionAffinity "ClientIP") -}}
{{- fail "service.sessionAffinity must be None or ClientIP" -}}
{{- end -}}
{{- if ne (int .Values.service.port) (int .Values.config.server.port) -}}
{{- fail "service.port must match config.server.port" -}}
{{- end -}}
{{- if ne (int .Values.service.metricsPort) (int .Values.config.server.metrics_port) -}}
{{- fail "service.metricsPort must match config.server.metrics_port" -}}
{{- end -}}
{{- if and .Values.networkPolicy.enabled .Values.config.telemetry.enabled (not .Values.networkPolicy.telemetry.enabled) (empty .Values.networkPolicy.additionalEgress) -}}
{{- fail "config.telemetry.enabled=true requires networkPolicy.telemetry.enabled=true or an explicit additionalEgress rule" -}}
{{- end -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kafkamcp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kafkamcp.labels" -}}
helm.sh/chart: {{ include "kafkamcp.chart" . }}
{{ include "kafkamcp.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kafkamcp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafkamcp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kafkamcp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kafkamcp.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
