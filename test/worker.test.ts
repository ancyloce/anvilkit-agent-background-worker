import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { TaskState } from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { type Job, Queue } from "bullmq";
import { GenericContainer, Wait } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Config } from "../src/config.js";
import { silentLogger } from "../src/log.js";
import { startWorker } from "../src/main.js";
import { connectOwner, OwnerTransports } from "../src/owner.js";
import { connection, type JobPayload, jobIdOf, jobOptions } from "../src/queue.js";
import { BackgroundWorker } from "../src/worker/worker.js";
import { FakeOwner } from "./fakeowner.js";
import { freePort, type Lab, labConfig, metricValue, newMetrics, startLab } from "./harness.js";

let lab: Lab;
let owner: FakeOwner;
let cfg: Config;
let transports: OwnerTransports;
const conns: ReturnType<typeof connection>[] = [];

const digest = (b: Buffer) => `sha256:${createHash("sha256").update(b).digest("hex")}`;
const localCheck = (extra: Record<string, unknown> = {}) =>
	Buffer.from(
		JSON.stringify({
			schemaVersion: 1,
			computation: "local-check-v1",
			bytes: Buffer.from("hello").toString("base64"),
			...extra,
		}),
	);

function script(taskId: string, input: Buffer, extra: Partial<Parameters<FakeOwner["tasks"]["set"]>[1]> = {}) {
	owner.tasks.set(taskId, {
		taskKind: "local-check",
		inputDigest: digest(input),
		input,
		state: TaskState.TASK_STATE_PENDING,
		...extra,
	});
	return {
		owner: "knowledge" as const,
		taskId,
		generation: "1",
		taskKind: "local-check",
		tenantId: "tenant_a",
		inputDigest: digest(input),
		correlationId: `req_${taskId}`,
	};
}

function newWorker() {
	const conn = connection(cfg.queue);
	conns.push(conn);
	const { metrics, registry } = newMetrics();
	const client = connectOwner("knowledge", owner.address, 2000, transports.for("knowledge"));
	const worker = new BackgroundWorker(cfg, { knowledge: client }, conn, metrics, silentLogger);
	return { worker, registry, conn, client };
}

/** A delivery as BullMQ would hand it to the processor (no Redis round trip needed for process()). */
function delivery(payload: JobPayload, attemptsMade = 0): Job<JobPayload> {
	return {
		id: jobIdOf(payload.taskId, payload.generation),
		data: payload,
		attemptsMade,
		queueName: "local-check",
	} as unknown as Job<JobPayload>;
}

beforeAll(async () => {
	lab = await startLab();
	owner = new FakeOwner();
	await owner.start();
	cfg = labConfig(lab, { ANVILKIT_BACKGROUND_WORKER_KNOWLEDGE_ADDRESS: owner.address });
	transports = new OwnerTransports(cfg);
});

afterAll(async () => {
	transports.close();
	owner.stop();
	for (const c of conns) c.disconnect();
	await lab.stop();
});

describe("background worker", () => {
	it("claims, heartbeats, computes the fixture and submits an accepted result", async () => {
		const { worker, registry } = newWorker();
		const input = localCheck({ holdMs: 500 });
		const r = await worker.process(delivery(script("t_ok", input)));
		expect(r.outcome).toBe("accepted");
		expect(owner.calls.filter((c) => c.method === "claim" && c.req.taskId === "t_ok")).toHaveLength(1);
		expect(
			owner.calls.filter((c) => c.method === "heartbeat" && c.req.taskId === "t_ok").length,
		).toBeGreaterThanOrEqual(1);
		const submit = owner.calls.find((c) => c.method === "submit" && c.req.taskId === "t_ok");
		expect(submit?.req.resultDigest).toBe(`sha256:${createHash("sha256").update("hello").digest("hex")}`);
		expect(submit?.req.resultRef).toBe("local-check:t_ok:1");
		expect(await metricValue(registry, "anvilkit_background_worker_jobs_total", { outcome: "accepted" })).toBe(1);
	});

	it("lets the owner decide duplicate deliveries: two workers, one claimant, one result", async () => {
		const a = newWorker();
		const b = newWorker();
		const payload = script("t_dup", localCheck({ holdMs: 300 }));
		const [ra, rb] = await Promise.all([a.worker.process(delivery(payload)), b.worker.process(delivery(payload, 1))]);
		expect([ra.outcome, rb.outcome].sort()).toEqual(["accepted", "refused_claim"]);
		expect(owner.calls.filter((c) => c.method === "submit" && c.req.taskId === "t_dup")).toHaveLength(1);
	});

	it("stops the handler after losing the lease and submits nothing", async () => {
		const { worker, registry } = newWorker();
		const r = await worker.process(
			delivery(script("t_lease", localCheck({ holdMs: 3000 }), { refuseHeartbeats: true })),
		);
		expect(r.outcome).toBe("lease_lost");
		expect(owner.calls.filter((c) => c.method === "submit" && c.req.taskId === "t_lease")).toHaveLength(0);
		expect(await metricValue(registry, "anvilkit_background_worker_lease_lost_total")).toBe(1);
	});

	it("controls unknown handlers, oversized inputs, timeouts and handler failures as submitted failures", async () => {
		const { worker, registry } = newWorker();
		const unknown = script("t_kind", localCheck());
		(owner.tasks.get("t_kind") as { taskKind: string }).taskKind = "knowledge-ingest";
		expect((await worker.process(delivery({ ...unknown, taskKind: "knowledge-ingest" }))).outcome).toBe(
			"unknown_handler",
		);
		expect(owner.calls.find((c) => c.method === "submit" && c.req.taskId === "t_kind")?.req.failureCode).toBe(
			"HANDLER_UNKNOWN",
		);
		const big = Buffer.alloc(70_000, 0x61);
		expect((await worker.process(delivery(script("t_big", big)))).outcome).toBe("input_too_large");
		expect((await worker.process(delivery(script("t_slow", localCheck({ holdMs: 10_000 }))))).outcome).toBe(
			"handler_timeout",
		);
		expect(owner.calls.find((c) => c.method === "submit" && c.req.taskId === "t_slow")?.req.failureCode).toBe(
			"HANDLER_TIMEOUT",
		);
		expect(await metricValue(registry, "anvilkit_background_worker_handler_timeouts_total")).toBe(1);
		expect((await worker.process(delivery(script("t_fail", localCheck({ fail: true }))))).outcome).toBe(
			"handler_failed",
		);
		expect(owner.calls.find((c) => c.method === "submit" && c.req.taskId === "t_fail")?.req.succeeded).toBe(false);
	});

	it("answers a lost submission receipt by resubmitting the identical result, then by the owner query", async () => {
		const { worker, registry } = newWorker();
		const r = await worker.process(delivery(script("t_receipt", localCheck(), { unavailableSubmits: 1 })));
		expect(r.outcome).toBe("existing");
		expect(owner.calls.filter((c) => c.method === "submit" && c.req.taskId === "t_receipt")).toHaveLength(2);
		expect(await metricValue(registry, "anvilkit_background_worker_submit_retries_total")).toBe(1);
		// Every receipt lost: the owner query settles it.
		const r2 = await worker.process(delivery(script("t_receipt2", localCheck(), { unavailableSubmits: 10 })));
		expect(r2.outcome).toBe("accepted");
		expect(owner.calls.filter((c) => c.method === "get" && c.req.taskId === "t_receipt2")).toHaveLength(1);
	});

	it("runs as BullMQ Workers on the queue Valkey and serves a read-only Bull Board whose retry is refused", async () => {
		const boardPort = await freePort();
		const healthPort = await freePort();
		const running = await startWorker(
			labConfig(
				lab,
				{
					ANVILKIT_BACKGROUND_WORKER_KNOWLEDGE_ADDRESS: owner.address,
					ANVILKIT_BACKGROUND_WORKER_HEALTH_LISTEN: `127.0.0.1:${healthPort}`,
					ANVILKIT_BACKGROUND_WORKER_BULL_BOARD_LISTEN: `127.0.0.1:${boardPort}`,
				},
				"bull_board:\n  enabled: true\n  read_only: true\nworker:\n  heartbeat_interval: 200ms\n  handler_timeout: 2s\n  shutdown_timeout: 5s\n",
			),
			silentLogger,
		);
		const conn = connection(cfg.queue);
		conns.push(conn);
		const queue = new Queue<JobPayload>("local-check", { connection: conn, prefix: "anvilkit:knowledge" });
		const payload = script("t_queue", localCheck());
		await queue.add("local-check", payload, jobOptions(cfg, "t_queue", "1"));
		const deadline = Date.now() + 15_000;
		while (Date.now() < deadline && owner.tasks.get("t_queue")?.state !== TaskState.TASK_STATE_ACCEPTED)
			await new Promise((r) => setTimeout(r, 100));
		expect(owner.tasks.get("t_queue")?.state).toBe(TaskState.TASK_STATE_ACCEPTED);
		expect((await fetch(`http://127.0.0.1:${healthPort}/readyz`)).status).toBe(200);
		const page = await fetch(`http://127.0.0.1:${boardPort}/`);
		expect(page.status).toBe(200);
		// A canceled task's delivery: the owner refuses the claim, no work happens.
		const canceled = script("t_canceled", localCheck(), { state: TaskState.TASK_STATE_CANCELED });
		await queue.add("local-check", canceled, jobOptions(cfg, "t_canceled", "1"));
		await new Promise((r) => setTimeout(r, 1500));
		expect(owner.calls.filter((c) => c.method === "claim" && c.req.taskId === "t_canceled")).toHaveLength(1);
		expect(owner.calls.filter((c) => c.method === "submit" && c.req.taskId === "t_canceled")).toHaveLength(0);
		// The read-only board refuses a retry.
		const retry = await fetch(
			`http://127.0.0.1:${boardPort}/api/queues/local-check/${encodeURIComponent(jobIdOf("t_canceled", "1"))}/retry/failed`,
			{ method: "PUT" },
		);
		expect(retry.status).not.toBe(204);
		await running.stop();
		expect((await fetch(`http://127.0.0.1:${healthPort}/readyz`).catch(() => ({ status: 0 }))).status).toBe(0);
		await queue.close();
	});
});

it("F06 recreated deliveries in one live worker receive distinct identities", async () => {
	const { worker, client } = newWorker();
	const payload = script("t_recreated", localCheck());
	const first = worker.workerIdOf(delivery(payload));
	const second = worker.workerIdOf(delivery(payload));
	expect(second).not.toBe(first);
	client.close();
});

it("F12 bounds shutdown of a handler that ignores its abort signal", async () => {
	const conn = connection(cfg.queue);
	const monitor = connection(cfg.queue);
	const client = connectOwner("knowledge", owner.address, 2000, transports.for("knowledge"));
	const { metrics } = newMetrics();
	const bounded = {
		...cfg,
		queue: { ...cfg.queue, prefix: `shutdown-${randomUUID()}` },
		worker: { ...cfg.worker, shutdownTimeoutMs: 200, lockDurationMs: 500, stalledIntervalMs: 500 },
	};
	await monitor.ping();
	await conn.ping();
	const connectionsBefore = String(await monitor.client("LIST"))
		.trim()
		.split("\n").length;
	let began!: () => void;
	const started = new Promise<void>((r) => {
		began = r;
	});
	let finish!: () => void;
	const ignoredAbort = new Promise<void>((r) => {
		finish = r;
	});
	const worker = new BackgroundWorker(bounded, { knowledge: client }, conn, metrics, silentLogger, {
		"local-check": async () => {
			began();
			await ignoredAbort;
			return { succeeded: true, resultRef: "local-check:t_shutdown:1", resultDigest: digest(Buffer.from("hello")) };
		},
	});
	let replacement: BackgroundWorker | undefined;
	let recoveryConn: ReturnType<typeof connection> | undefined;
	let guard: NodeJS.Timeout | undefined;
	try {
		worker.start();
		const q = worker.queueHandles.find((q) => q.name === "local-check");
		if (!q) throw new Error("local-check queue missing");
		const payload = script("t_shutdown", localCheck());
		await q.add("local-check", payload, jobOptions(cfg, payload.taskId, "1"));
		await started;
		const before = performance.now();
		const outcome = await Promise.race([
			worker.stop(),
			new Promise((resolve) => {
				guard = setTimeout(() => resolve("hung"), 1200);
			}),
		]);
		clearTimeout(guard);
		expect(outcome).toBe(true);
		expect(performance.now() - before).toBeLessThan(1000);
		expect(conn.status).toBe("end");
		await expect
			.poll(
				async () =>
					String(await monitor.client("LIST"))
						.trim()
						.split("\n").length,
			)
			.toBe(connectionsBefore - 1);
		expect(owner.calls.filter((c) => c.method === "submit" && c.req.taskId === payload.taskId)).toHaveLength(0);
		// Model the durable owner's independent lease expiry. Recovery retains the
		// task identity; the owner grants a new delivery claimant, never a success.
		const task = owner.tasks.get(payload.taskId);
		if (!task) throw new Error("owner task missing");
		const priorIdentity = task.workerId;
		task.state = TaskState.TASK_STATE_PENDING;
		recoveryConn = connection(cfg.queue);
		replacement = new BackgroundWorker(bounded, { knowledge: client }, recoveryConn, metrics, silentLogger);
		replacement.start();
		await expect
			.poll(() => owner.tasks.get(payload.taskId)?.state, { timeout: 10_000 })
			.toBe(TaskState.TASK_STATE_ACCEPTED);
		expect(owner.tasks.get(payload.taskId)?.workerId).not.toBe(priorIdentity);
		finish();
		await expect
			.poll(async () => {
				await new Promise((r) => setImmediate(r));
				return owner.calls.filter((c) => c.method === "submit" && c.req.taskId === payload.taskId).length;
			})
			.toBe(1);
	} finally {
		clearTimeout(guard);
		finish();
		try {
			await Promise.all([worker.stop(), replacement?.stop()]);
		} finally {
			conn.disconnect();
			recoveryConn?.disconnect();
			monitor.disconnect();
			client.close();
		}
	}
});

it("F06 network retries of one claim retain the same delivery identity", async () => {
	const { worker, client } = newWorker();
	try {
		const payload = script("t_claim_retry", localCheck(), { unavailableClaims: 1 });
		expect((await worker.process(delivery(payload))).outcome).toBe("accepted");
		const claims = owner.calls.filter((c) => c.method === "claim" && c.req.taskId === payload.taskId);
		expect(claims).toHaveLength(2);
		expect(claims[0]?.req.workerId).toBe(claims[1]?.req.workerId);
	} finally {
		client.close();
	}
});

it("F12 interrupts unresponsive queue finalization after owner acceptance and recovers the original delivery", async () => {
	// This test alone owns this container. Never inject faults into lab/shared Valkey.
	const valkey = await new GenericContainer("valkey/valkey:9.0.2-alpine")
		.withExposedPorts(6379)
		.withWaitStrategy(Wait.forLogMessage(/Ready to accept connections/))
		.start();
	const url = `redis://${valkey.getHost()}:${valkey.getMappedPort(6379)}`;
	const conn = connection({ ...cfg.queue, url });
	const monitor = connection({ ...cfg.queue, url });
	const client = connectOwner("knowledge", owner.address, 2000, transports.for("knowledge"));
	const { metrics, registry } = newMetrics();
	const bounded = {
		...cfg,
		queue: { ...cfg.queue, url, prefix: `finalization-${randomUUID()}` },
		worker: { ...cfg.worker, shutdownTimeoutMs: 200, lockDurationMs: 500, stalledIntervalMs: 500 },
	};
	let began!: () => void;
	const started = new Promise<void>((resolve) => {
		began = resolve;
	});
	let finish!: () => void;
	const finished = new Promise<void>((resolve) => {
		finish = resolve;
	});
	const payload = script(`t_finalize_${randomUUID()}`, localCheck());
	const worker = new BackgroundWorker(bounded, { knowledge: client }, conn, metrics, silentLogger, {
		"local-check": async () => {
			began();
			await finished;
			return {
				succeeded: true,
				resultRef: `local-check:${payload.taskId}:1`,
				resultDigest: digest(Buffer.from("hello")),
			};
		},
	});
	let paused = false;
	let stopping: Promise<boolean> | undefined;
	let replacement: BackgroundWorker | undefined;
	let recoveryConn: ReturnType<typeof connection> | undefined;
	let guard: NodeJS.Timeout | undefined;
	try {
		await monitor.ping();
		worker.start();
		const q = worker.queueHandles.find((queue) => queue.name === "local-check");
		if (!q) throw new Error("local-check queue missing");
		await q.add("local-check", payload, jobOptions(bounded, payload.taskId, "1"));
		await started;
		execFileSync("docker", ["pause", valkey.getId()]);
		paused = true;
		finish();
		await expect.poll(() => owner.tasks.get(payload.taskId)?.state).toBe(TaskState.TASK_STATE_ACCEPTED);
		await expect
			.poll(() => metricValue(registry, "anvilkit_background_worker_jobs_total", { outcome: "accepted" }))
			.toBe(1);
		const before = performance.now();
		stopping = worker.stop();
		const outcome = await Promise.race([
			stopping,
			new Promise<string>((resolve) => {
				guard = setTimeout(() => resolve("hung"), 1200);
			}),
		]);
		clearTimeout(guard);
		expect(outcome).toBe(true);
		expect(performance.now() - before).toBeLessThan(1000);
		expect(conn.status).toBe("end");
		expect(await worker.stop()).toBe(true);
		execFileSync("docker", ["unpause", valkey.getId()]);
		paused = false;
		await expect
			.poll(
				async () =>
					String(await monitor.client("LIST"))
						.trim()
						.split("\n").length,
			)
			.toBe(1);
		// The owner has accepted, but BullMQ's original active job still needs
		// recovery. A new worker must not execute or submit that result again.
		recoveryConn = connection({ ...cfg.queue, url });
		replacement = new BackgroundWorker(bounded, { knowledge: client }, recoveryConn, metrics, silentLogger);
		replacement.start();
		const recoveredQueue = replacement.queueHandles.find((queue) => queue.name === "local-check");
		if (!recoveredQueue) throw new Error("local-check queue missing");
		await expect.poll(() => recoveredQueue.getJob(jobIdOf(payload.taskId, "1")), { timeout: 10_000 }).toBeUndefined();
		expect(owner.calls.filter((c) => c.method === "submit" && c.req.taskId === payload.taskId)).toHaveLength(1);
		const next = script(`t_after_${randomUUID()}`, localCheck());
		await recoveredQueue.add("local-check", next, jobOptions(bounded, next.taskId, "1"));
		await expect.poll(() => owner.tasks.get(next.taskId)?.state).toBe(TaskState.TASK_STATE_ACCEPTED);
		expect(await replacement.stop()).toBe(false);
		await expect
			.poll(
				async () =>
					String(await monitor.client("LIST"))
						.trim()
						.split("\n").length,
			)
			.toBe(1);
	} finally {
		clearTimeout(guard);
		finish();
		try {
			if (paused) execFileSync("docker", ["unpause", valkey.getId()]);
			await Promise.all([stopping ?? worker.stop(), replacement?.stop()]);
		} finally {
			conn.disconnect();
			recoveryConn?.disconnect();
			monitor.disconnect();
			client.close();
			await valkey.stop();
		}
	}
});
