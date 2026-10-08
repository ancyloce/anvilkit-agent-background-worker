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
{{- if not (has .Values.identity.mode (list "mtls" "development")) }}
{{- fail "identity.mode must be mtls or development" }}
{{- end }}
{{- if and (eq .Values.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (a plaintext owner transport)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.trustDomain) (not .Values.development.enabled) }}
{{- fail "identity.trustDomain is required outside development (the development default anvilkit.local applies only with development.enabled: true)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") .Values.identity.certificate.create (not .Values.identity.certificate.issuerRef.name) }}
{{- fail "identity.certificate.issuerRef.name is required: the cert-manager issuer of the workload certificate (or set identity.certificate.create false and identity.secretName)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.certificate.create) (not .Values.identity.secretName) }}
{{- fail "identity.secretName is required while identity.certificate.create is false" }}
{{- end }}
{{- end -}}

{{/* The identity Secret: the rendered Certificate's or the environment's. */}}
{{- define "anvilkit-agent-background-worker.identitySecret" -}}
{{- if .Values.identity.certificate.create -}}
{{- printf "%s-identity" (include "anvilkit-agent-background-worker.fullname" .) -}}
{{- else -}}
{{- .Values.identity.secretName -}}
{{- end -}}
{{- end -}}

{{/* The rendered configuration: the reviewed sections plus the chart-owned
identity, guard and owner server names, wired to the loader's keys. */}}
{{- define "anvilkit-agent-background-worker.config" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $_ := set $cfg.bull_board "enabled" .Values.bullBoard.enabled -}}
{{- $_ = set $cfg.bull_board "read_only" .Values.bullBoard.readOnly -}}
{{- $_ = set $cfg "development" (dict "enabled" .Values.development.enabled) -}}
{{- $id := dict "mode" .Values.identity.mode "reload_interval" (dig "identity" "reload_interval" "5s" $cfg) -}}
{{- if eq .Values.identity.mode "mtls" }}
{{- $_ = set $id "cert_file" "/etc/anvilkit/identity/tls.crt" }}
{{- $_ = set $id "key_file" "/etc/anvilkit/identity/tls.key" }}
{{- $_ = set $id "ca_file" "/etc/anvilkit/identity/ca.crt" }}
{{- end -}}
{{- $_ = set $cfg "identity" $id -}}
{{- $_ = set $cfg "owners" (dict "knowledge" (dict "server_name" .Values.owners.knowledge.serverName) "mcp" (dict "server_name" .Values.owners.mcp.serverName)) -}}
{{- toYaml $cfg -}}
{{- end -}}
