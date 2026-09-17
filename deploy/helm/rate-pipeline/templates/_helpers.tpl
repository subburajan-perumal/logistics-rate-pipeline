{{- define "rp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rp.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "rp.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rp.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: {{ include "rp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "rp.ingestd.selector" -}}
app.kubernetes.io/name: {{ include "rp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: ingestd
{{- end -}}

{{- define "rp.mocksources.selector" -}}
app.kubernetes.io/name: {{ include "rp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: mocksources
{{- end -}}

{{- define "rp.serviceAccountName" -}}
{{- if .Values.ingestd.serviceAccount.create -}}
{{- default (printf "%s-ingestd" (include "rp.fullname" .)) .Values.ingestd.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.ingestd.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "rp.mocksourcesURL" -}}
http://{{ include "rp.fullname" . }}-mocksources:{{ .Values.mocksources.service.port }}
{{- end -}}
