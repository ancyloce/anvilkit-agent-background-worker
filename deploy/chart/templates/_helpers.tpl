{{- define "anvilkit-agent-background-worker.name" -}}
anvilkit-agent-background-worker
{{- end -}}

{{- define "anvilkit-agent-background-worker.fullname" -}}
{{- if eq .Release.Name (include "anvilkit-agent-background-worker.name" .) -}}
{{- .Release.Name -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "anvilkit-agent-background-worker.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-background-worker.labels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-background-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: background-worker
app.kubernetes.io/part-of: anvilkit
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "anvilkit-agent-background-worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-background-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "anvilkit-agent-background-worker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "anvilkit-agent-background-worker.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-background-worker.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-background-worker.require" -}}
{{- if not .Values.queue.secret.name }}
{{- fail "queue.secret.name is required: an existing Secret holding the queue Valkey URL, mounted as the file ANVILKIT_BACKGROUND_WORKER_QUEUE_URL_FILE names" }}
{{- end }}
{{- if and (not .Values.owners.knowledge.address) (not .Values.owners.mcp.address) }}
{{- fail "at least one owner address is required: owners.knowledge.address and/or owners.mcp.address" }}
{{- end }}
{{- end -}}
