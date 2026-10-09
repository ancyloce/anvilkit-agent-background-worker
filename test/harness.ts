// Disposable PostgreSQL 17 (anvilkit_knowledge from the parent repository's
// migration source), Valkey (the queue instance) and NATS JetStream (the
// ANVILKIT_KNOWLEDGE stream) for the relay and worker tests; fixture rows
// stand in for the owners' durable requests (the owners themselves are
// exercised by the parent's integration scenario).
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { jetstreamManager } from "@nats-io/jetstream";
import { connect, type NatsConnection } from "@nats-io/transport-node";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { Registry } from "prom-client";
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers";
import { type Config, loadFrom } from "../src/config.js";
import { contractsDir } from "../src/contracts.js";
import { Metrics } from "../src/metrics.js";
import { type CA, files, issue, type Leaf, mount, newCA, spiffe, tempDir } from "./pki.js";

export function migrationsDir(): string | undefined {
	const env = process.env.ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR;
	if (env) return env;
	let d = process.cwd();
	for (;;) {
		const candidate = path.join(d, "jobs", "migration", "internal", "migrate", "sql", "knowledge");
		if (existsSync(path.join(candidate, "00001_init.sql"))) return candidate;
		const parent = path.dirname(d);
		if (parent === d) return undefined;
		d = parent;
	}
}

export interface Lab {
	pg: StartedPostgreSqlContainer;
	valkey: StartedTestContainer;
	nats: StartedTestContainer;
	appUrl: string;
	relayUrl: string;
	queueUrl: string;
	natsUrl: string;
	nc: NatsConnection;
	admin(sql: string, params?: unknown[]): Promise<pg.QueryResult>;
	stop(): Promise<void>;
}

export async function startLab(): Promise<Lab> {
	const dir = migrationsDir();
	if (!dir)
		throw new Error("UNEXECUTED: anvilkit_knowledge migrations not found (set ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR)");
	const [pgc, valkey, nats] = await Promise.all([
		new PostgreSqlContainer("postgres:17-alpine")
			.withDatabase("postgres")
			.withUsername("postgres")
			.withPassword("postgres")
			.start(),
		new GenericContainer("valkey/valkey:9.0.2-alpine")
			.withExposedPorts(6379)
			.withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
			.start(),
		new GenericContainer("nats:2.12.4-alpine")
			.withCommand(["-js"])
			.withExposedPorts(4222)
			.withWaitStrategy(Wait.forLogMessage(/Server is ready/))
			.start(),
	]);
	const root = new pg.Client({ connectionString: pgc.getConnectionUri() });
	await root.connect();
	for (const s of [
		"CREATE ROLE anvilkit_knowledge_app LOGIN PASSWORD 'app'",
		"CREATE ROLE anvilkit_knowledge_migrator LOGIN PASSWORD 'migrator'",
		"CREATE ROLE anvilkit_knowledge_relay LOGIN PASSWORD 'relay'",
		"CREATE ROLE anvilkit_knowledge_forwarder LOGIN PASSWORD 'forwarder'",
		// P17: migration 00005 lets the PostgresStore vendor identity create its schema.
		"CREATE ROLE anvilkit_knowledge_store_migrator LOGIN PASSWORD 'storemig'",
		"CREATE DATABASE anvilkit_knowledge OWNER anvilkit_knowledge_migrator",
	])
		await root.query(s);
	await root.end();
	const url = (role: string, pw: string) =>
		`postgres://${role}:${pw}@${pgc.getHost()}:${pgc.getPort()}/anvilkit_knowledge`;
	const migrator = new pg.Client({ connectionString: url("anvilkit_knowledge_migrator", "migrator") });
	await migrator.connect();
	await migrator.query("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
	for (const file of readdirSync(dir)
		.filter((f) => /^\d+_.*\.sql$/.test(f))
		.sort()) {
		const text = readFileSync(path.join(dir, file), "utf8");
		await migrator.query(text.split("-- +goose Down")[0]?.replace("-- +goose Up", "") ?? "");
	}
	const natsUrl = `nats://${nats.getHost()}:${nats.getMappedPort(4222)}`;
	const nc = await connect({ servers: natsUrl });
	const jsm = await jetstreamManager(nc);
	await jsm.streams.add({
		name: "ANVILKIT_KNOWLEDGE",
		subjects: ["anvilkit.knowledge.>"],
		duplicate_window: 120_000_000_000,
	});
	return {
		pg: pgc,
		valkey,
		nats,
		appUrl: url("anvilkit_knowledge_app", "app"),
		relayUrl: url("anvilkit_knowledge_relay", "relay"),
		queueUrl: `redis://${valkey.getHost()}:${valkey.getMappedPort(6379)}`,
		natsUrl,
		nc,
		admin: (sql, params) => migrator.query(sql, params),
		stop: async () => {
			await nc.drain().catch(() => undefined);
			await migrator.end();
			await Promise.all([pgc.stop(), valkey.stop(), nats.stop()]);
		},
	};
}

export function freePort(): Promise<number> {
	return new Promise((resolve) => {
		const s = createServer();
		s.listen(0, "127.0.0.1", () => {
			const port = (s.address() as { port: number }).port;
			s.close(() => resolve(port));
		});
	});
}

/**
 * The lab PKI (P0.1): one throwaway CA, the worker's own identity
 * (spiffe://anvilkit.local/ns/anvilkit-apps/sa/anvilkit-agent-background-worker)
 * mounted the way a Secret is, and the owner double's server leaf.
 */
export interface LabPki {
	ca: CA;
	workerDir: string;
	ownerLeaf: Leaf;
}

let labPki: LabPki | undefined;

export function pki(): LabPki {
	if (labPki) return labPki;
	const ca = newCA("lab");
	const workerDir = tempDir();
	mount(
		workerDir,
		issue(ca, "anvilkit-agent-background-worker", [
			spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-background-worker"),
		]),
		ca.pem,
	);
	labPki = {
		ca,
		workerDir,
		ownerLeaf: issue(
			ca,
			"anvilkit-agent-knowledge",
			[spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-knowledge")],
			["anvilkit-agent-knowledge"],
		),
	};
	return labPki;
}

/** The identity environment of the worker under the lab PKI. */
export function identityEnv(): Record<string, string> {
	const f = files(pki().workerDir);
	return {
		ANVILKIT_BACKGROUND_WORKER_IDENTITY_CERT_FILE: f.certFile,
		ANVILKIT_BACKGROUND_WORKER_IDENTITY_KEY_FILE: f.keyFile,
		ANVILKIT_BACKGROUND_WORKER_IDENTITY_CA_FILE: f.caFile,
	};
}

/**
 * A configuration for the tests: the reviewed file plus the lab's placements
 * and the lab PKI. The lab's PostgreSQL, Valkey and NATS are plaintext
 * without credentials, admitted only under the DEVELOPMENT_ONLY guard (P0.6).
 */
export function labConfig(lab: Lab, overrides: Record<string, string> = {}, fileContent = ""): Config {
	const dir = mkdtempSync(path.join(tmpdir(), "bg-worker-"));
	const file = path.join(dir, "config.yaml");
	writeFileSync(
		file,
		"development:\n  enabled: true\nnats:\n  tls:\n    mode: development\n" +
			(fileContent ||
				"relay:\n  reconcile_interval: 1s\n  reconcile_age: 0ms\nworker:\n  heartbeat_interval: 200ms\n  handler_timeout: 2s\n  submit_retries: 2\n  shutdown_timeout: 5s\n"),
	);
	return loadFrom(file, {
		ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: lab.queueUrl,
		ANVILKIT_BACKGROUND_WORKER_NATS_URL: lab.natsUrl,
		ANVILKIT_BACKGROUND_WORKER_RELAY_OWNER: "knowledge",
		ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL: lab.relayUrl,
		ANVILKIT_BACKGROUND_WORKER_KNOWLEDGE_ADDRESS: "127.0.0.1:1",
		ANVILKIT_BACKGROUND_WORKER_CONTRACTS_DIR: contractsDir(),
		...identityEnv(),
		...overrides,
	});
}

export function newMetrics(): { metrics: Metrics; registry: Registry } {
	const registry = new Registry();
	return { metrics: new Metrics(registry), registry };
}

export async function metricValue(
	registry: Registry,
	name: string,
	labels: Record<string, string> = {},
): Promise<number> {
	const m = (await registry.getMetricsAsJSON()).find((x) => x.name === name);
	if (!m) return 0;
	const v = (m.values as { labels: Record<string, string>; value: number }[]).find((x) =>
		Object.entries(labels).every(([k, val]) => x.labels[k] === val),
	);
	return v?.value ?? 0;
}

const digest = (s: string) => `sha256:${createHash("sha256").update(s).digest("hex")}`;

/** A durable request row (as the owner would have committed it) with its source. */
export async function requestFixture(
	lab: Lab,
	taskId: string,
	generation: number,
	state: string,
	tenant = "tenant_a",
	input = '{"schemaVersion":1,"computation":"local-check-v1","bytes":"aGVsbG8="}',
): Promise<{ inputDigest: string }> {
	await lab.admin(
		`INSERT INTO sources (source_id, tenant_id, kind, locator, command_id, request_digest) VALUES ('src_1', 'tenant_a', 'document', 'file://x', 'cmd_src', 'sha256:0000000000000000000000000000000000000000000000000000000000000000') ON CONFLICT DO NOTHING`,
	);
	const inputDigest = digest(input);
	await lab.admin(
		`INSERT INTO background_requests (task_id, generation, tenant_id, task_kind, input_digest, input, state, max_attempts, result_profile, effects, authorization_ref, revision, correlation_id, updated_at)
		 VALUES ($1, $2, $3, 'local-check', $4, $5::jsonb, $6, 3, 'local-check-v1', 'reconstructible', 'source:src_1', 1, $7, now() - interval '1 minute')`,
		[taskId, generation, tenant, inputDigest, input, state, `req_${taskId}`],
	);
	return { inputDigest };
}

export function requestedEvent(
	taskId: string,
	generation: number,
	inputDigest: string,
	overrides: Record<string, unknown> = {},
): Uint8Array {
	const env = {
		eventId: crypto.randomUUID(),
		eventType: "background.requested",
		schemaVersion: 1,
		producer: "anvilkit-agent-knowledge",
		subject: "anvilkit.knowledge.background.requested",
		tenantId: "tenant_a",
		aggregateType: "background_request",
		aggregateId: taskId,
		aggregateRevision: "1",
		occurredAt: new Date().toISOString(),
		correlationId: `req_${taskId}`,
		payload: {
			kind: "background.requested",
			taskId,
			generation: String(generation),
			taskKind: "local-check",
			inputDigest,
		},
		...overrides,
	};
	return Buffer.from(JSON.stringify(env));
}
