// The queue side of the lane (DD-09 §1): the owner's BullMQ queues on the
// queue Valkey under the anvilkit:<owner> prefix, the job identity that
// deduplicates scheduling (one job per task generation, encoded within
// BullMQ's custom-id rules: never an integer, never a bare colon) and the
// payload, which carries identity, generation, digest and trace references
// only — never an input body, a result or a credential.
import { Queue, type QueueOptions } from "bullmq";
import { Redis } from "ioredis";
import type { Config, Owner } from "./config.js";

export interface JobPayload {
	owner: Owner;
	taskId: string;
	generation: string;
	taskKind: string;
	tenantId: string;
	inputDigest: string;
	correlationId: string;
}

const idPattern = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const sequencePattern = /^(0|[1-9][0-9]{0,19})$/;
const digestPattern = /^sha256:[0-9a-f]{64}$/;

/** Checks a payload's shape before anything is done with it. */
export function checkPayload(raw: unknown): JobPayload {
	if (typeof raw !== "object" || raw === null) throw new Error("payload is not an object");
	const p = raw as Record<string, unknown>;
	const keys = Object.keys(p).sort().join(",");
	if (keys !== "correlationId,generation,inputDigest,owner,taskId,taskKind,tenantId")
		throw new Error(`payload fields ${keys}`);
	if (p.owner !== "knowledge" && p.owner !== "mcp") throw new Error("payload owner");
	for (const k of ["taskId", "tenantId", "correlationId", "taskKind"])
		if (typeof p[k] !== "string" || !idPattern.test(p[k] as string)) throw new Error(`payload ${k}`);
	if (typeof p.generation !== "string" || !sequencePattern.test(p.generation) || p.generation === "0")
		throw new Error("payload generation");
	if (typeof p.inputDigest !== "string" || !digestPattern.test(p.inputDigest)) throw new Error("payload inputDigest");
	return p as unknown as JobPayload;
}

/** The BullMQ job id of a task generation: "<taskId>/<generation>" with ":" and "%" escaped. */
export function jobIdOf(taskId: string, generation: string): string {
	return `${taskId.replaceAll("%", "%25").replaceAll(":", "%3A")}/${generation}`;
}

export function prefixOf(cfg: Config, owner: Owner): string {
	return `${cfg.queue.prefix}:${owner}`;
}

export function connection(url: string): Redis {
	// BullMQ requires maxRetriesPerRequest: null on its connections.
	return new Redis(url, {
		maxRetriesPerRequest: null,
		enableReadyCheck: false,
		lazyConnect: false,
		// Explicit disconnect is the shutdown deadline's interrupt, including
		// BullMQ's duplicated blocking clients. Do not add ioredis's default
		// socket half-close grace when the server cannot answer.
		disconnectTimeout: 0,
	});
}

export function openQueue(cfg: Config, owner: Owner, name: string, conn: Redis): Queue<JobPayload> {
	const opts: QueueOptions = { connection: conn, prefix: prefixOf(cfg, owner) };
	return new Queue<JobPayload>(name, opts);
}

/** Queue-level delivery options of one job (the owner's attempt bound is the business limit). */
export function jobOptions(cfg: Config, taskId: string, generation: string) {
	return {
		jobId: jobIdOf(taskId, generation),
		attempts: cfg.worker.attempts,
		backoff: { type: "fixed" as const, delay: cfg.worker.backoffMs },
		removeOnComplete: true,
		removeOnFail: { count: 1000 },
	};
}
