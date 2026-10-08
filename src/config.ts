// The Worker's and the relay's immutable configuration (DD-09 §4):
// defaults < the reviewed, secret-free config.yaml < the allowlisted
// ANVILKIT_BACKGROUND_WORKER_* environment. Unknown keys and variables,
// missing values, out-of-range values and contradictions reject the
// candidate before anything starts. The queue Valkey URL and the relay's
// database URL are secrets: environment or mounted secret file only.
import { readFileSync } from "node:fs";
import { parse as parseYaml } from "yaml";

export const envPrefix = "ANVILKIT_BACKGROUND_WORKER_";
export const envConfigFile = "ANVILKIT_BACKGROUND_WORKER_CONFIG";
export const defaultConfigFile = "config.yaml";

export type Owner = "knowledge" | "mcp";
export const owners: Owner[] = ["knowledge", "mcp"];

/** The stable queues of each owner (architecture.md naming) plus the fixture lane. */
export const ownerQueues: Record<Owner, string[]> = {
	knowledge: ["knowledge-ingest", "knowledge-project", "memory-project", "local-check"],
	mcp: ["mcp-catalog-refresh", "local-check"],
};

/** The workload identity of the owner clients (P0.1): mtls presents the watched certificate and verifies the owner by server name; development is plaintext and admitted only under development.enabled. */
export interface IdentityConfig {
	mode: "mtls" | "development";
	certFile: string;
	keyFile: string;
	caFile: string;
	reloadIntervalMs: number;
}

export interface Config {
	/** The top-level DEVELOPMENT_ONLY guard: a plaintext owner transport needs identity.mode development and this; it downgrades nothing by itself. File-only. */
	development: { enabled: boolean };
	identity: IdentityConfig;
	health: { listen: string };
	/** Spans over OTLP/HTTP to the collector when placed (none otherwise), sampled at sampleRatio. */
	telemetry: { otlpEndpoint: string; sampleRatio: number };
	queue: { url: string; prefix: string };
	nats: { url: string };
	owners: Record<Owner, { address: string; serverName: string }>;
	contractsDir: string;
	worker: {
		concurrency: number;
		leaseSeconds: number;
		heartbeatIntervalMs: number;
		maxInputBytes: number;
		handlerTimeoutMs: number;
		attempts: number;
		backoffMs: number;
		lockDurationMs: number;
		stalledIntervalMs: number;
		maxStalledCount: number;
		submitRetries: number;
		ownerTimeoutMs: number;
		shutdownTimeoutMs: number;
	};
	bullBoard: { enabled: boolean; listen: string; readOnly: boolean };
	relay: {
		owner: Owner | "";
		databaseUrl: string;
		batch: number;
		ackWaitMs: number;
		maxDeliver: number;
		reconcileIntervalMs: number;
		reconcileAgeMs: number;
		reconcileLimit: number;
		shutdownTimeoutMs: number;
	};
}

export class ConfigError extends Error {}

export function parseDuration(text: unknown, key: string): number {
	if (typeof text === "number" && Number.isInteger(text) && text >= 0) return text;
	const m = typeof text === "string" ? /^(\d+)(ms|s|m|h)$/.exec(text) : null;
	if (!m) throw new ConfigError(`${key}: ${JSON.stringify(text)} is not a duration (e.g. 500ms, 15s, 5m)`);
	return Number(m[1]) * { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[m[2] as "ms" | "s" | "m" | "h"];
}

const envOverrides: Record<string, string> = {
	ANVILKIT_BACKGROUND_WORKER_HEALTH_LISTEN: "health.listen",
	ANVILKIT_BACKGROUND_WORKER_IDENTITY_MODE: "identity.mode",
	ANVILKIT_BACKGROUND_WORKER_IDENTITY_CERT_FILE: "identity.cert_file",
	ANVILKIT_BACKGROUND_WORKER_IDENTITY_KEY_FILE: "identity.key_file",
	ANVILKIT_BACKGROUND_WORKER_IDENTITY_CA_FILE: "identity.ca_file",
	ANVILKIT_BACKGROUND_WORKER_OWNER_SERVER_NAME: "owners.server_name",
	ANVILKIT_BACKGROUND_WORKER_TELEMETRY_OTLP_ENDPOINT: "telemetry.otlp_endpoint",
	ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "queue.url",
	ANVILKIT_BACKGROUND_WORKER_QUEUE_URL_FILE: "queue.url_file",
	ANVILKIT_BACKGROUND_WORKER_NATS_URL: "nats.url",
	ANVILKIT_BACKGROUND_WORKER_KNOWLEDGE_ADDRESS: "owners.knowledge.address",
	ANVILKIT_BACKGROUND_WORKER_MCP_ADDRESS: "owners.mcp.address",
	ANVILKIT_BACKGROUND_WORKER_CONTRACTS_DIR: "contracts.dir",
	ANVILKIT_BACKGROUND_WORKER_BULL_BOARD_LISTEN: "bull_board.listen",
	ANVILKIT_BACKGROUND_WORKER_RELAY_OWNER: "relay.owner",
	ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL: "relay.database_url",
	ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL_FILE: "relay.database_url_file",
};

const environmentOnly = [
	"telemetry.otlp_endpoint",
	"queue.url",
	"queue.url_file",
	"nats.url",
	"owners.knowledge.address",
	"owners.mcp.address",
	"contracts.dir",
	"relay.owner",
	"relay.database_url",
	"relay.database_url_file",
];

type Raw = Record<string, unknown>;

function isObject(v: unknown): v is Raw {
	return typeof v === "object" && v !== null && !Array.isArray(v);
}

function get(raw: Raw, path: string): unknown {
	let cur: unknown = raw;
	for (const part of path.split(".")) {
		if (!isObject(cur)) return undefined;
		cur = cur[part];
	}
	return cur;
}

function set(raw: Raw, path: string, value: unknown): void {
	const parts = path.split(".");
	let cur = raw;
	for (const part of parts.slice(0, -1)) {
		const next = cur[part];
		if (!isObject(next)) {
			const fresh: Raw = {};
			cur[part] = fresh;
			cur = fresh;
		} else cur = next;
	}
	cur[parts[parts.length - 1] as string] = value;
}

function leaves(raw: Raw, prefix = ""): string[] {
	const out: string[] = [];
	for (const [k, v] of Object.entries(raw)) {
		const path = prefix ? `${prefix}.${k}` : k;
		if (isObject(v)) out.push(...leaves(v, path));
		else out.push(path);
	}
	return out;
}

function merge(into: Raw, from: Raw): void {
	for (const [k, v] of Object.entries(from)) {
		const cur = into[k];
		if (isObject(v) && isObject(cur)) merge(cur, v);
		else into[k] = isObject(v) ? structuredClone(v) : v;
	}
}

const defaults: Raw = {
	development: { enabled: false },
	identity: { mode: "mtls", reload_interval: "5s" },
	health: { listen: "127.0.0.1:9127" },
	owners: { knowledge: { server_name: "anvilkit-agent-knowledge" }, mcp: { server_name: "anvilkit-agent-mcp" } },
	telemetry: { sample_ratio: 1 },
	queue: { prefix: "anvilkit" },
	worker: {
		concurrency: 4,
		lease_seconds: 60,
		heartbeat_interval: "15s",
		max_input_bytes: 65536,
		handler_timeout: "5m",
		attempts: 3,
		backoff: "5s",
		lock_duration: "60s",
		stalled_interval: "30s",
		max_stalled_count: 1,
		submit_retries: 3,
		owner_timeout: "10s",
		shutdown_timeout: "30s",
	},
	bull_board: { enabled: false, listen: "127.0.0.1:9128", read_only: true },
	relay: {
		batch: 16,
		ack_wait: "30s",
		max_deliver: 100,
		reconcile_interval: "10s",
		reconcile_age: "5s",
		reconcile_limit: 500,
		shutdown_timeout: "30s",
	},
};

const known = new Set([
	"development.enabled",
	"identity.mode",
	"identity.cert_file",
	"identity.key_file",
	"identity.ca_file",
	"identity.reload_interval",
	"owners.knowledge.server_name",
	"owners.mcp.server_name",
	"owners.server_name",
	"telemetry.otlp_endpoint",
	"telemetry.sample_ratio",
	"health.listen",
	"queue.url",
	"queue.url_file",
	"queue.prefix",
	"nats.url",
	"owners.knowledge.address",
	"owners.mcp.address",
	"contracts.dir",
	...[
		"concurrency",
		"lease_seconds",
		"heartbeat_interval",
		"max_input_bytes",
		"handler_timeout",
		"attempts",
		"backoff",
		"lock_duration",
		"stalled_interval",
		"max_stalled_count",
		"submit_retries",
		"owner_timeout",
		"shutdown_timeout",
	].map((k) => `worker.${k}`),
	"bull_board.enabled",
	"bull_board.listen",
	"bull_board.read_only",
	...[
		"owner",
		"database_url",
		"database_url_file",
		"batch",
		"ack_wait",
		"max_deliver",
		"reconcile_interval",
		"reconcile_age",
		"reconcile_limit",
		"shutdown_timeout",
	].map((k) => `relay.${k}`),
]);

export function load(environ: NodeJS.ProcessEnv = process.env): Config {
	return loadFrom(environ[envConfigFile] || defaultConfigFile, environ);
}

export function loadFrom(path: string, environ: NodeJS.ProcessEnv): Config {
	const raw: Raw = structuredClone(defaults);
	let reviewed: unknown;
	try {
		reviewed = parseYaml(readFileSync(path, "utf8")) ?? {};
	} catch (err) {
		throw new ConfigError(`config file ${path}: ${err instanceof Error ? err.message : String(err)}`);
	}
	if (!isObject(reviewed)) throw new ConfigError(`config file ${path}: not a mapping`);
	for (const key of leaves(reviewed)) {
		if (!known.has(key)) throw new ConfigError(`config file ${path}: unknown key ${key}`);
		if (environmentOnly.includes(key))
			throw new ConfigError(
				`config file ${path}: ${key} is a secret or a placement and is accepted only from the environment`,
			);
	}
	merge(raw, reviewed);
	const unknownEnv: string[] = [];
	for (const [name, value] of Object.entries(environ)) {
		if (!name.startsWith(envPrefix) || name === envConfigFile || value === undefined) continue;
		const key = envOverrides[name];
		if (!key) {
			unknownEnv.push(name);
			continue;
		}
		set(raw, key, value);
	}
	if (unknownEnv.length > 0)
		throw new ConfigError(`environment variables are not allowed overrides: ${unknownEnv.sort().join(", ")}`);
	return validate(raw);
}

function str(raw: Raw, key: string): string {
	const v = get(raw, key);
	if (v === undefined || v === null) return "";
	if (typeof v !== "string" && typeof v !== "number") throw new ConfigError(`${key}: must be a string`);
	return String(v);
}

function int(raw: Raw, key: string, min: number, max: number): number {
	const v = get(raw, key);
	const n = typeof v === "string" ? Number(v) : v;
	if (typeof n !== "number" || !Number.isInteger(n) || n < min || n > max)
		throw new ConfigError(`${key}: must be an integer within [${min}, ${max}]`);
	return n;
}

function bool(raw: Raw, key: string): boolean {
	const v = get(raw, key);
	if (v === true || v === "true") return true;
	if (v === false || v === "false") return false;
	throw new ConfigError(`${key}: must be a boolean`);
}

function duration(raw: Raw, key: string, min: number, max: number): number {
	const ms = parseDuration(get(raw, key), key);
	if (ms < min || ms > max) throw new ConfigError(`${key}: must be within [${min}ms, ${max}ms]`);
	return ms;
}

function secretFrom(raw: Raw, key: string, fileKey: string, errors: string[]): string {
	let value = "";
	try {
		value = str(raw, key);
		const file = str(raw, fileKey);
		if (!value && file) value = readFileSync(file, "utf8").trim();
	} catch (err) {
		errors.push(`${fileKey}: ${err instanceof Error ? err.message : String(err)}`);
	}
	return value;
}

const listenPattern = /^[^:\s]+:\d{1,5}$/;

function validate(raw: Raw): Config {
	const errors: string[] = [];
	const attempt = <T>(fn: () => T, fallback: T): T => {
		try {
			return fn();
		} catch (err) {
			errors.push(err instanceof Error ? err.message : String(err));
			return fallback;
		}
	};
	const otlpEndpoint = attempt(() => str(raw, "telemetry.otlp_endpoint"), "");
	if (otlpEndpoint && !/^https?:\/\/[^\s/]+(\/[^\s]*)?$/.test(otlpEndpoint))
		errors.push("telemetry.otlp_endpoint must be an http(s) URL of the collector");
	const sampleRatio = Number(get(raw, "telemetry.sample_ratio"));
	if (!(sampleRatio >= 0 && sampleRatio <= 1)) errors.push("telemetry.sample_ratio must be within [0, 1]");
	const queueUrl = secretFrom(raw, "queue.url", "queue.url_file", errors);
	if (queueUrl && !/^rediss?:\/\//.test(queueUrl))
		errors.push("queue.url must be a redis:// or rediss:// URL (the queue Valkey, never the cache instance)");
	const relayOwner = attempt(() => str(raw, "relay.owner"), "");
	if (relayOwner && !(owners as string[]).includes(relayOwner)) errors.push("relay.owner must be knowledge or mcp");
	const relayDb = secretFrom(raw, "relay.database_url", "relay.database_url_file", errors);
	if (relayDb && !/^postgres(ql)?:\/\//.test(relayDb)) errors.push("relay.database_url must be a postgres URL");
	const lease = attempt(() => int(raw, "worker.lease_seconds", 1, 3600), 60);
	const heartbeat = attempt(() => duration(raw, "worker.heartbeat_interval", 100, 3_600_000), 15_000);
	if (heartbeat >= lease * 1000) errors.push("worker.heartbeat_interval must be shorter than worker.lease_seconds");
	const lock = attempt(() => duration(raw, "worker.lock_duration", 1000, 3_600_000), 60_000);
	const stalled = attempt(() => duration(raw, "worker.stalled_interval", 1000, 3_600_000), 30_000);
	if (stalled >= lock) errors.push("worker.stalled_interval must be shorter than worker.lock_duration");
	const development = attempt(() => bool(raw, "development.enabled"), false);
	const identityMode = attempt(() => str(raw, "identity.mode"), "mtls");
	const identityFiles = {
		certFile: attempt(() => str(raw, "identity.cert_file"), ""),
		keyFile: attempt(() => str(raw, "identity.key_file"), ""),
		caFile: attempt(() => str(raw, "identity.ca_file"), ""),
	};
	if (identityMode === "mtls") {
		if (!identityFiles.certFile || !identityFiles.keyFile || !identityFiles.caFile)
			errors.push(
				"identity.cert_file, key_file and ca_file are required under identity.mode mtls (ANVILKIT_BACKGROUND_WORKER_IDENTITY_{CERT,KEY,CA}_FILE)",
			);
	} else if (identityMode === "development") {
		if (!development)
			errors.push(
				"identity.mode development (plaintext owner transport) requires development.enabled: true (DEVELOPMENT_ONLY)",
			);
	} else errors.push("identity.mode must be mtls or development");
	// ANVILKIT_BACKGROUND_WORKER_OWNER_SERVER_NAME (owners.server_name) is the
	// relay container's setting: it names the one owner the relay dials and
	// overrides both per-owner names.
	const commonServerName = attempt(() => str(raw, "owners.server_name"), "");
	const serverName = (o: Owner) => commonServerName || attempt(() => str(raw, `owners.${o}.server_name`), "");
	for (const o of owners)
		if (identityMode === "mtls" && !serverName(o)) errors.push(`owners.${o}.server_name is required`);
	const cfg: Config = {
		development: { enabled: development },
		identity: {
			mode: identityMode as IdentityConfig["mode"],
			...identityFiles,
			reloadIntervalMs: attempt(() => duration(raw, "identity.reload_interval", 100, 3_600_000), 5000),
		},
		health: { listen: attempt(() => str(raw, "health.listen"), "") },
		telemetry: { otlpEndpoint, sampleRatio },
		queue: { url: queueUrl, prefix: attempt(() => str(raw, "queue.prefix"), "anvilkit") },
		nats: { url: attempt(() => str(raw, "nats.url"), "") },
		owners: {
			knowledge: {
				address: attempt(() => str(raw, "owners.knowledge.address"), ""),
				serverName: serverName("knowledge"),
			},
			mcp: { address: attempt(() => str(raw, "owners.mcp.address"), ""), serverName: serverName("mcp") },
		},
		contractsDir: attempt(() => str(raw, "contracts.dir"), ""),
		worker: {
			concurrency: attempt(() => int(raw, "worker.concurrency", 1, 256), 4),
			leaseSeconds: lease,
			heartbeatIntervalMs: heartbeat,
			maxInputBytes: attempt(() => int(raw, "worker.max_input_bytes", 1, 65536), 65536),
			handlerTimeoutMs: attempt(() => duration(raw, "worker.handler_timeout", 100, 3_600_000), 300_000),
			attempts: attempt(() => int(raw, "worker.attempts", 1, 100), 3),
			backoffMs: attempt(() => duration(raw, "worker.backoff", 0, 3_600_000), 5000),
			lockDurationMs: lock,
			stalledIntervalMs: stalled,
			maxStalledCount: attempt(() => int(raw, "worker.max_stalled_count", 0, 100), 1),
			submitRetries: attempt(() => int(raw, "worker.submit_retries", 0, 100), 3),
			ownerTimeoutMs: attempt(() => duration(raw, "worker.owner_timeout", 100, 600_000), 10_000),
			shutdownTimeoutMs: attempt(() => duration(raw, "worker.shutdown_timeout", 100, 600_000), 30_000),
		},
		bullBoard: {
			enabled: attempt(() => bool(raw, "bull_board.enabled"), false),
			listen: attempt(() => str(raw, "bull_board.listen"), ""),
			readOnly: attempt(() => bool(raw, "bull_board.read_only"), true),
		},
		relay: {
			owner: relayOwner as Owner | "",
			databaseUrl: relayDb,
			batch: attempt(() => int(raw, "relay.batch", 1, 1000), 16),
			ackWaitMs: attempt(() => duration(raw, "relay.ack_wait", 1000, 600_000), 30_000),
			maxDeliver: attempt(() => int(raw, "relay.max_deliver", 1, 10_000), 100),
			reconcileIntervalMs: attempt(() => duration(raw, "relay.reconcile_interval", 100, 3_600_000), 10_000),
			reconcileAgeMs: attempt(() => duration(raw, "relay.reconcile_age", 0, 3_600_000), 5000),
			reconcileLimit: attempt(() => int(raw, "relay.reconcile_limit", 1, 10_000), 500),
			shutdownTimeoutMs: attempt(() => duration(raw, "relay.shutdown_timeout", 100, 600_000), 30_000),
		},
	};
	if (!listenPattern.test(cfg.health.listen)) errors.push("health.listen must be host:port");
	if (cfg.bullBoard.enabled && !listenPattern.test(cfg.bullBoard.listen))
		errors.push("bull_board.listen must be host:port while bull_board.enabled is true");
	if (!/^[a-z][a-z0-9-]*$/.test(cfg.queue.prefix))
		errors.push("queue.prefix must be a lowercase identifier (anvilkit:<owner> follows)");
	if (errors.length > 0) throw new ConfigError(`config: ${errors.join("; ")}`);
	return cfg;
}

/** Checks the inputs the worker entry needs beyond the common ones. */
export function requireWorker(cfg: Config): void {
	const errors: string[] = [];
	if (!cfg.queue.url) errors.push("queue.url is required (ANVILKIT_BACKGROUND_WORKER_QUEUE_URL or _QUEUE_URL_FILE)");
	if (!owners.some((o) => cfg.owners[o].address))
		errors.push(
			"at least one owner address is required (ANVILKIT_BACKGROUND_WORKER_KNOWLEDGE_ADDRESS, ANVILKIT_BACKGROUND_WORKER_MCP_ADDRESS)",
		);
	if (errors.length > 0) throw new ConfigError(`config: ${errors.join("; ")}`);
}

/** Checks the inputs the relay entry needs beyond the common ones. */
export function requireRelay(cfg: Config): void {
	const errors: string[] = [];
	if (!cfg.queue.url) errors.push("queue.url is required (ANVILKIT_BACKGROUND_WORKER_QUEUE_URL or _QUEUE_URL_FILE)");
	if (!cfg.nats.url) errors.push("nats.url is required (ANVILKIT_BACKGROUND_WORKER_NATS_URL)");
	if (!cfg.relay.owner) errors.push("relay.owner is required (ANVILKIT_BACKGROUND_WORKER_RELAY_OWNER)");
	if (!cfg.relay.databaseUrl)
		errors.push("relay.database_url is required (ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL or _FILE)");
	if (cfg.relay.owner && !cfg.owners[cfg.relay.owner].address)
		errors.push(`owners.${cfg.relay.owner}.address is required for the gap repair query`);
	if (errors.length > 0) throw new ConfigError(`config: ${errors.join("; ")}`);
}
