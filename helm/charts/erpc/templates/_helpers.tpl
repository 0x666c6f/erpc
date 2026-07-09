{{/*
Expand the name of the chart.
*/}}
{{- define "erpc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "erpc.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "erpc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "erpc.labels" -}}
helm.sh/chart: {{ include "erpc.chart" . }}
{{ include "erpc.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "erpc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "erpc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Vault creator ServiceAccount name.
*/}}
{{- define "erpc.vaultCreatorServiceAccountName" -}}
{{- $vaultCreator := .Values.serviceAccount.vaultCreator | default dict -}}
{{- $vaultCreator.name | default .Values.serviceAccount.name | default "erpc" }}
{{- end }}

{{/*
Runtime ServiceAccount name.
*/}}
{{- define "erpc.serviceAccountName" -}}
{{- if .Values.serviceAccount.runtimeName -}}
{{- .Values.serviceAccount.runtimeName -}}
{{- else if eq (toString .Values.serviceAccount.create) "false" -}}
{{- include "erpc.vaultCreatorServiceAccountName" . -}}
{{- else -}}
{{- printf "%s-runtime" (include "erpc.vaultCreatorServiceAccountName" .) -}}
{{- end -}}
{{- end }}

{{/*
Kubernetes Secrets managed by Vault creator jobs.
*/}}
{{- define "erpc.vaultSecretResourceNames" -}}
- "erpc-db-secret"
- {{ printf "%s-vault-config" (include "erpc.fullname" .) | quote }}
{{- end }}

{{/*
Common environment variables for erpc
*/}}
{{- define "erpc.commonEnv" -}}
# All env vars now come from vault-secrets via envFrom
{{- end -}}
