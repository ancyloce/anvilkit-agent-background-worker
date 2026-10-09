// The owner queue relay (DD-09 §1/§2, event catalog "background.requested
// → owner queue relay"): a fixed durable pull consumer on the owner's
// background.requested subject; every message is validated against the
// events schema, checked against the durable request it names (tenant,
// generation, state) and recorded in the owner's inbox (event_id,
// consumer) with its outcome — the inbox row commits before the ACK, a
// redelivered duplicate returns the recorded outcome — then the queue
// entry is added under the task generation's job id (idempotent). The
// reconcile loop rebuilds queue entries for due durable requests that lost
// theirs (Sentinel write loss, a crash between commit and add) and never
// re-admits a terminal or superseded generation: the durable request,
// never the queue, is the authority. The relay runs with the owner's relay
// role (SELECT on background_requests, INSERT/SELECT on inbox) and
// touches no domain fact.
import { AckPolicy, type ConsumerMessages, DeliverPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";
import { credsAuthenticator, type NatsConnection, nanos, nkeyAuthenticator } from "@nats-io/nats-core";
import { connect, type NodeConnectionOptions } from "@nats-io/transport-node";
import type { Queue } from "bullmq";
import type { Redis } from "ioredis";
import pg from "pg";
import type { Config, Owner } from "../config.js";
import { ContractViolation, type Envelope, EventContract } from "../contracts.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import type { OwnerClient } from "../owner.js";
import { type JobPayload, jobIdOf, jobOptions, openQueue } from "../queue.js";

export const streamOf: Record<Owner, string> = { knowledge: "ANVILKIT_KNOWLEDGE", mcp: "ANVILKIT_MCP" };

/**
 * The relay's NATS connection options (P0.6): under nats.tls.mode tls or
 * mtls a TLS connection is required and verified (caFile, else Node's
 * default roots with NODE_EXTRA_CA_CERTS; serverName, else the URL host;
 * the files are read again on every reconnect), mtls presents the client
 * certificate, and the credential read at load authenticates (a user .creds
 * through credsAuthenticator, a bare NKey seed through nkeyAuthenticator).
 * Development leaves the transport plaintext (DEVELOPMENT_ONLY).
 */
export function natsOptions(cfg: Config, name: string): NodeConnectionOptions {
	const options: NodeConnectionOptions = { servers: cfg.nats.url, name };
	const t = cfg.nats.tls;
	if (t.mode !== "development") {
		// The transport copies every key into tls.connect: servername included.
		const tls: NonNullable<NodeConnectionOptions["tls"]> & { servername?: string } = { rejectUnauthorized: true };
		if (t.caFile) tls.caFile = t.caFile;
		if (t.mode === "mtls") {
			tls.certFile = t.certFile;
			tls.keyFile = t.keyFile;
		}
		if (t.serverName) tls.servername = t.serverName;
		options.tls = tls;
	}
	const c = cfg.nats.credential;
	if (c) options.authenticator = c.kind === "creds" ? credsAuthenticator(c.bytes) : nkeyAuthenticator(c.bytes);
	return options;
}
export const consumerOf: Record<Owner, string> = {
	knowledge: "anvilkit-agent-knowledge-background-relay",
	mcp: "anvilkit-agent-mcp-background-relay",
};

interface RequestRow {
	task_id: string;
	generation: string;
	tenant_id: string;
	task_kind: string;
	input_digest: string;
	state: string;
	correlation_id: string;
	retry_at: Date | null;
}

const dueStates = ["pending", "retry_scheduled"];

export type InboxOutcome =
	| "enqueue"
	| "ignored_terminal"
	| "ignored_stale"
	| "rejected_tenant"
	| "rejected_kind"
	| "gap_enqueue"
	| "gap_ignored";

export class Relay {
	private nc: NatsConnection | null = null;
	private messages: ConsumerMessages | null = null;
	private readonly queues = new Map<string, Queue<JobPayload>>();
	private readonly pool: pg.Pool;
	private readonly contract: EventContract;
	private reconcileTimer: NodeJS.Timeout | undefined;
	private reconciling: Promise<void> = Promise.resolve();
	private consuming: Promise<void> = Promise.resolve();
	private stopped = false;
	readonly owner: Owner;
	readonly consumerName: string;

	constructor(
		private readonly cfg: Config,
		owner: Owner,
		private readonly ownerClient: OwnerClient,
		private readonly conn: Redis,
		private readonly metrics: Metrics,
		private readonly log: Logger,
	) {
		this.owner = owner;
		this.consumerName = consumerOf[owner];
		this.pool = new pg.Pool({ connectionString: cfg.relay.databaseUrl, max: 2 });
		this.contract = new EventContract(cfg.contractsDir || undefined);
	}

	private queue(name: string): Queue<JobPayload> {
		let q = this.queues.get(name);
		if (!q) {
			q = openQueue(this.cfg, this.owner, name, this.conn);
			this.queues.set(name, q);
		}
		return q;
	}

	/** Probes the database, the queue and NATS, ensures the fixed durable consumer, then consumes and reconciles. */
	async start(): Promise<void> {
		const c = await this.pool.connect();
		try {
			await c.query("SELECT 1 FROM background_requests LIMIT 1");
			await c.query("SELECT 1 FROM inbox LIMIT 1");
		} finally {
			c.release();
		}
		await this.conn.ping();
		this.nc = await connect(natsOptions(this.cfg, `anvilkit-${this.owner}-background-relay`));
		const jsm = await jetstreamManager(this.nc);
		const stream = streamOf[this.owner];
		const subject = `anvilkit.${this.owner}.background.requested`;
		const wanted = {
			durable_name: this.consumerName,
			filter_subject: subject,
			ack_policy: AckPolicy.Explicit,
			ack_wait: nanos(this.cfg.relay.ackWaitMs),
			max_deliver: this.cfg.relay.maxDeliver,
			deliver_policy: DeliverPolicy.All,
			max_ack_pending: this.cfg.relay.batch * 4,
		};
		try {
			await jsm.consumers.info(stream, this.consumerName);
		} catch {
			await jsm.consumers.add(stream, wanted);
		}
		const consumer = await jetstream(this.nc).consumers.get(stream, this.consumerName);
		this.messages = await consumer.consume({ max_messages: this.cfg.relay.batch });
		const messages = this.messages;
		this.consuming = (async () => {
			for await (const m of messages) {
				try {
					await this.handle(
						m.data,
						() => m.ack(),
						() => m.nak(),
						() => m.term(),
					);
				} catch (err) {
					this.log.warn("relay message not handled; redelivery", { error: String(err) });
					m.nak(this.cfg.relay.reconcileAgeMs);
				}
			}
		})();
		this.reconcileTimer = setInterval(() => {
			this.reconciling = this.reconciling.then(async () => {
				try {
					await this.reconcile();
				} catch (err) {
					this.log.warn("reconcile failed", { error: String(err) });
				}
			});
		}, this.cfg.relay.reconcileIntervalMs);
		this.log.info("relay running", { owner: this.owner, stream, consumer: this.consumerName, subject });
	}

	/** One message: contract, inbox transaction, queue add, ack. Exposed for the tests. */
	async handle(
		data: Uint8Array,
		ack: (outcome: InboxOutcome) => void,
		nak: () => void,
		term: () => void,
	): Promise<void> {
		let env: Envelope;
		try {
			env = this.contract.parse(data);
		} catch (err) {
			// Not a contract message: never enters the inbox, never redelivered.
			this.metrics.inbox.inc({ outcome: "invalid_schema" });
			this.log.warn("relay rejected a message that violates the events schema", {
				error: err instanceof ContractViolation ? err.message : String(err),
			});
			term();
			return;
		}
		if (
			env.eventType !== "background.requested" ||
			env.subject !== `anvilkit.${this.owner}.background.requested` ||
			env.producer !== `anvilkit-agent-${this.owner}`
		) {
			this.metrics.inbox.inc({ outcome: "invalid_schema" });
			term();
			return;
		}
		const payload = env.payload as
			| { taskId: string; generation: string; taskKind: string; inputDigest: string }
			| undefined;
		if (!payload) {
			this.metrics.inbox.inc({ outcome: "invalid_schema" });
			term();
			return;
		}
		const decided = await this.decide(env, payload);
		if (decided.duplicate) {
			this.metrics.inboxDuplicates.inc();
			this.metrics.inbox.inc({ outcome: "duplicate" });
		} else this.metrics.inbox.inc({ outcome: decided.outcome });
		if (decided.outcome === "enqueue" || decided.outcome === "gap_enqueue") {
			try {
				await this.enqueue(
					decided.row ?? {
						task_id: payload.taskId,
						generation: payload.generation,
						tenant_id: env.tenantId,
						task_kind: payload.taskKind,
						input_digest: payload.inputDigest,
						state: "pending",
						correlation_id: env.correlationId,
						retry_at: null,
					},
				);
			} catch (err) {
				this.log.warn("queue add failed; redelivery", { error: String(err) });
				nak();
				return;
			}
		}
		ack(decided.outcome);
	}

	/** The inbox transaction on the relay role: decide from the durable request, record the outcome, commit. */
	private async decide(
		env: Envelope,
		payload: { taskId: string; generation: string; taskKind: string; inputDigest: string },
	): Promise<{ outcome: InboxOutcome; duplicate: boolean; row?: RequestRow }> {
		const c = await this.pool.connect();
		try {
			await c.query("BEGIN");
			const existing = await c.query<{ outcome: string }>(
				"SELECT outcome FROM inbox WHERE event_id = $1 AND consumer = $2",
				[env.eventId, this.consumerName],
			);
			if (existing.rows[0]) {
				await c.query("ROLLBACK");
				const outcome = existing.rows[0].outcome as InboxOutcome;
				const row =
					outcome === "enqueue" || outcome === "gap_enqueue"
						? await this.request(payload.taskId, payload.generation)
						: undefined;
				return { outcome, duplicate: true, row };
			}
			const r = await c.query<RequestRow>(
				"SELECT task_id, generation::text AS generation, tenant_id, task_kind, input_digest, state, correlation_id, retry_at FROM background_requests WHERE task_id = $1 ORDER BY generation DESC LIMIT 1",
				[payload.taskId],
			);
			const latest = r.rows[0];
			let outcome: InboxOutcome;
			let row: RequestRow | undefined;
			if (!latest) {
				// The event is ahead of the durable request (a restored database):
				// repair through the owner's query, never by trusting the event.
				this.metrics.inboxGaps.inc();
				outcome = (await this.repairGap(payload)) ? "gap_enqueue" : "gap_ignored";
			} else if (latest.tenant_id !== env.tenantId) {
				outcome = "rejected_tenant";
			} else if (Number(payload.generation) < Number(latest.generation)) {
				outcome = "ignored_stale";
			} else if (Number(payload.generation) > Number(latest.generation)) {
				this.metrics.inboxGaps.inc();
				outcome = (await this.repairGap(payload)) ? "gap_enqueue" : "gap_ignored";
			} else if (latest.task_kind !== payload.taskKind || latest.input_digest !== payload.inputDigest) {
				outcome = "rejected_kind";
			} else if (!dueStates.includes(latest.state)) {
				outcome = "ignored_terminal";
			} else {
				outcome = "enqueue";
				row = latest;
			}
			await c.query("INSERT INTO inbox (event_id, consumer, outcome, aggregate_revision) VALUES ($1, $2, $3, $4)", [
				env.eventId,
				this.consumerName,
				outcome,
				Number(env.aggregateRevision),
			]);
			await c.query("COMMIT");
			return { outcome, duplicate: false, row };
		} catch (err) {
			await c.query("ROLLBACK").catch(() => undefined);
			throw err;
		} finally {
			c.release();
		}
	}

	private async repairGap(payload: { taskId: string; generation: string }): Promise<boolean> {
		try {
			const t = await this.ownerClient.get(payload.taskId);
			return t.generation === payload.generation && dueStates.includes(t.state);
		} catch (err) {
			this.log.warn("gap repair query failed; the event is ignored until the reconcile loop sees the request", {
				error: String(err),
			});
			return false;
		}
	}

	private async request(taskId: string, generation: string): Promise<RequestRow | undefined> {
		const r = await this.pool.query<RequestRow>(
			"SELECT task_id, generation::text AS generation, tenant_id, task_kind, input_digest, state, correlation_id, retry_at FROM background_requests WHERE task_id = $1 AND generation = $2",
			[taskId, Number(generation)],
		);
		return r.rows[0];
	}

	/** Adds the job under its task-generation id; a finished job with the same id is removed first (BullMQ keeps ids of completed/failed jobs). */
	private async enqueue(row: RequestRow): Promise<"added" | "present"> {
		const q = this.queue(row.task_kind);
		const id = jobIdOf(row.task_id, row.generation);
		const current = await q.getJob(id);
		if (current) {
			const state = await current.getState();
			if (state === "completed" || state === "failed") await current.remove();
			else return "present";
		}
		const payload: JobPayload = {
			owner: this.owner,
			taskId: row.task_id,
			generation: row.generation,
			taskKind: row.task_kind,
			tenantId: row.tenant_id,
			inputDigest: row.input_digest,
			correlationId: row.correlation_id,
		};
		const opts = jobOptions(this.cfg, row.task_id, row.generation);
		if (row.retry_at && row.retry_at.getTime() > Date.now())
			Object.assign(opts, { delay: row.retry_at.getTime() - Date.now() });
		await q.add(row.task_kind, payload, opts);
		this.metrics.relayed.inc();
		return "added";
	}

	/** Reconstruction: due durable requests older than the reconcile age get their queue entry back. */
	async reconcile(): Promise<{ examined: number; rebuilt: number }> {
		const r = await this.pool.query<RequestRow>(
			`SELECT task_id, generation::text AS generation, tenant_id, task_kind, input_digest, state, correlation_id, retry_at
			 FROM background_requests
			 WHERE state = ANY($1) AND updated_at < now() - ($2::text || ' milliseconds')::interval
			 ORDER BY updated_at LIMIT $3`,
			[dueStates, String(this.cfg.relay.reconcileAgeMs), this.cfg.relay.reconcileLimit],
		);
		let rebuilt = 0;
		for (const row of r.rows) {
			if (this.stopped) break;
			if ((await this.enqueue(row)) === "added") {
				rebuilt++;
				this.metrics.reconstructed.inc();
			}
		}
		this.metrics.withoutQueueEntry.set(rebuilt);
		return { examined: r.rows.length, rebuilt };
	}

	/** Stops consuming (in-flight messages finish or are redelivered), the reconcile loop, then closes the clients. */
	async stop(): Promise<boolean> {
		this.stopped = true;
		if (this.reconcileTimer) clearInterval(this.reconcileTimer);
		this.messages?.stop();
		let forced = false;
		const bound = new Promise<void>((resolve) =>
			setTimeout(() => {
				forced = true;
				resolve();
			}, this.cfg.relay.shutdownTimeoutMs).unref(),
		);
		await Promise.race([Promise.all([this.consuming, this.reconciling]), bound]);
		await this.nc?.drain().catch(() => undefined);
		for (const q of this.queues.values()) await q.close();
		await this.pool.end();
		return forced;
	}
}
