// The Background Worker (DD-09 §1): BullMQ Workers on each configured
// owner's queues; a job is only identity, generation, digest and trace
// references, so every step goes through the owner: ClaimTask (one
// claimant, the bounded input, the task kind), HeartbeatTask while the
// handler runs (losing the lease aborts the handler; nothing is
// submitted), SubmitTaskResult (the owner's CAS decides acceptance; a lost
// receipt is answered by resubmitting the identical result, then by the
// GetTask query). Duplicate or stalled deliveries reach the owner as
// claims the owner refuses; a refused claim is not work. Bounds: input
// size, concurrency, handler duration, queue attempts, lock and stall.

import { randomBytes, randomUUID } from "node:crypto";
import { once } from "node:events";
import { hostname } from "node:os";
import { type Job, type Queue, Worker } from "bullmq";
import type { Redis } from "ioredis";
import { type Config, type Owner, ownerQueues, owners } from "../config.js";
import type { Logger } from "../log.js";
import type { Metrics } from "../metrics.js";
import { type OwnerClient, OwnerRefused, OwnerUnavailable, type TaskView } from "../owner.js";
import { checkPayload, type JobPayload, openQueue, prefixOf } from "../queue.js";
import type { DeliveryObserver } from "../telemetry.js";
import { handlers as defaultHandlers, type Handler, HandlerError } from "./handlers.js";

export interface Processed {
	outcome:
		| "accepted"
		| "existing"
		| "refused_claim"
		| "not_accepted"
		| "lease_lost"
		| "unknown_handler"
		| "input_too_large"
		| "handler_timeout"
		| "handler_failed"
		| "unknown_receipt";
	state?: TaskView["state"];
}

export class BackgroundWorker {
	readonly instanceId: string;
	private readonly workers: Worker<JobPayload>[] = [];
	private readonly workerRuns: Promise<void>[] = [];
	private readonly queues: Queue<JobPayload>[] = [];
	private observe: NodeJS.Timeout | undefined;
	private readonly running = new Set<Promise<Processed>>();
	private readonly abortHandlers = new Set<() => void>();
	private forcing = false;
	private stopping = false;
	private stopPromise: Promise<boolean> | undefined;

	constructor(
		private readonly cfg: Config,
		private readonly clients: Partial<Record<Owner, OwnerClient>>,
		private readonly conn: Redis,
		private readonly metrics: Metrics,
		private readonly log: Logger,
		private readonly handlers: Record<string, Handler> = defaultHandlers,
		private readonly observer?: DeliveryObserver,
	) {
		this.instanceId = `${hostname().slice(0, 24)}-${randomBytes(4).toString("hex")}`;
	}

	/** A delivery identity is independent of BullMQ's resettable attemptsMade. */
	workerIdOf(_job: Job<JobPayload>): string {
		return `${this.instanceId}/${randomUUID()}`;
	}

	/** Processes one delivery; exposed for the tests. */
	process(job: Job<JobPayload>): Promise<Processed> {
		if (this.stopping) return Promise.resolve({ outcome: "refused_claim" });
		const work = this.processDelivery(job);
		const end = this.observer?.delivery(job.queueName);
		void work.then(
			(p) => end?.(p.outcome),
			() => end?.("error"),
		);
		this.running.add(work);
		void work.then(
			() => this.running.delete(work),
			() => this.running.delete(work),
		);
		return work;
	}

	private async processDelivery(job: Job<JobPayload>): Promise<Processed> {
		const payload = checkPayload(job.data);
		const client = this.clients[payload.owner];
		if (!client) throw new Error(`owner ${payload.owner} is not configured`);
		const workerId = this.workerIdOf(job);
		const queue = job.queueName;
		let claimed: { task: TaskView; input: Buffer };
		try {
			// Transport retries of this claim retain the delivery identity.
			for (let retry = 0; ; retry++) {
				try {
					claimed = await client.claim(payload.taskId, payload.generation, workerId, this.cfg.worker.leaseSeconds);
					break;
				} catch (err) {
					if (!(err instanceof OwnerUnavailable) || retry >= this.cfg.worker.submitRetries || this.forcing) throw err;
				}
			}
		} catch (err) {
			if (err instanceof OwnerRefused) {
				// The owner said no (leased elsewhere, terminal, superseded, not
				// due): the delivery is not work. NOT_FOUND and the like are
				// also final for this delivery.
				this.metrics.jobs.inc({ queue, outcome: "refused_claim" });
				this.log.info("claim refused", { taskId: payload.taskId, generation: payload.generation, reason: err.reason });
				return { outcome: "refused_claim" };
			}
			throw err; // unavailable: BullMQ retries the delivery within its attempts
		}
		if (this.forcing) return { outcome: "lease_lost" };
		const { task, input } = claimed;
		const fail = async (code: string, outcome: Processed["outcome"]): Promise<Processed> => {
			const r = await this.submitWithRetries(client, {
				taskId: task.taskId,
				generation: task.generation,
				workerId,
				inputDigest: task.inputDigest,
				succeeded: false,
				resultRef: "",
				resultDigest: "",
				failureCode: code,
			});
			this.metrics.jobs.inc({ queue, outcome });
			return { outcome, state: r?.task.state };
		};
		if (input.byteLength > this.cfg.worker.maxInputBytes) return fail("INPUT_TOO_LARGE", "input_too_large");
		if (task.inputDigest !== payload.inputDigest) {
			// The queue entry named another input than the owner froze: the
			// owner's digest is the truth; the entry is stale.
			this.metrics.jobs.inc({ queue, outcome: "refused_claim" });
			await this.submitWithRetries(client, {
				taskId: task.taskId,
				generation: task.generation,
				workerId,
				inputDigest: task.inputDigest,
				succeeded: false,
				resultRef: "",
				resultDigest: "",
				failureCode: "QUEUE_ENTRY_STALE",
			});
			return { outcome: "refused_claim" };
		}
		const handler = this.handlers[task.taskKind];
		if (!handler) return fail("HANDLER_UNKNOWN", "unknown_handler");

		const abort = new AbortController();
		let leaseLost = false;
		const heartbeat = setInterval(() => {
			client.heartbeat(task.taskId, task.generation, workerId).catch((err) => {
				if (err instanceof OwnerRefused) {
					leaseLost = true;
					this.metrics.leaseLost.inc();
					abort.abort(new Error("lease lost"));
				} else this.log.warn("heartbeat failed", { taskId: task.taskId, error: String(err) });
			});
		}, this.cfg.worker.heartbeatIntervalMs);
		let timedOut = false;
		const timeout = setTimeout(() => {
			timedOut = true;
			abort.abort(new Error("handler timeout"));
		}, this.cfg.worker.handlerTimeoutMs);
		const stopHandler = () => {
			clearInterval(heartbeat);
			clearTimeout(timeout);
			abort.abort(new Error("worker shutdown"));
		};
		this.abortHandlers.add(stopHandler);
		let result: Awaited<ReturnType<Handler>>;
		try {
			result = await handler(input, {
				taskId: task.taskId,
				generation: task.generation,
				workerId,
				inputDigest: task.inputDigest,
				signal: abort.signal,
			});
		} catch (err) {
			this.abortHandlers.delete(stopHandler);
			if (this.forcing) return { outcome: "lease_lost" };
			clearInterval(heartbeat);
			clearTimeout(timeout);
			if (leaseLost) {
				// Nothing is submitted on a lost lease: the owner has moved on.
				this.metrics.jobs.inc({ queue, outcome: "lease_lost" });
				this.log.warn("handler stopped after the lease was lost", { taskId: task.taskId, generation: task.generation });
				return { outcome: "lease_lost" };
			}
			if (timedOut) {
				this.metrics.handlerTimeouts.inc();
				return fail("HANDLER_TIMEOUT", "handler_timeout");
			}
			return fail(err instanceof HandlerError ? err.code : "HANDLER_FAILED", "handler_failed");
		}
		this.abortHandlers.delete(stopHandler);
		clearInterval(heartbeat);
		clearTimeout(timeout);
		if (leaseLost || this.forcing) {
			this.metrics.jobs.inc({ queue, outcome: "lease_lost" });
			return { outcome: "lease_lost" };
		}
		const submission = result.succeeded
			? {
					taskId: task.taskId,
					generation: task.generation,
					workerId,
					inputDigest: task.inputDigest,
					succeeded: true,
					resultRef: result.resultRef,
					resultDigest: result.resultDigest,
				}
			: {
					taskId: task.taskId,
					generation: task.generation,
					workerId,
					inputDigest: task.inputDigest,
					succeeded: false,
					resultRef: "",
					resultDigest: "",
					failureCode: result.failureCode,
				};
		const r = await this.submitWithRetries(client, submission);
		if (!r) {
			// The receipt is lost after the bounded resubmissions: ask the owner
			// instead of guessing; whatever it says is the outcome.
			try {
				const current = await client.get(task.taskId);
				const outcome: Processed["outcome"] =
					current.state === "accepted" && current.generation === task.generation ? "accepted" : "unknown_receipt";
				this.metrics.jobs.inc({ queue, outcome });
				return { outcome, state: current.state };
			} catch {
				this.metrics.jobs.inc({ queue, outcome: "unknown_receipt" });
				return { outcome: "unknown_receipt" };
			}
		}
		const outcome: Processed["outcome"] = r.existing
			? "existing"
			: r.accepted
				? "accepted"
				: result.succeeded
					? "not_accepted"
					: "handler_failed";
		this.metrics.jobs.inc({ queue, outcome });
		return { outcome, state: r.task.state };
	}

	/** Submits; an unavailable owner gets the identical submission again within the bound (idempotent: the owner returns the existing acceptance). */
	private async submitWithRetries(
		client: OwnerClient,
		sub: Parameters<OwnerClient["submit"]>[0],
	): Promise<Awaited<ReturnType<OwnerClient["submit"]>> | undefined> {
		for (let i = 0; i <= this.cfg.worker.submitRetries; i++) {
			try {
				return await client.submit(sub);
			} catch (err) {
				if (err instanceof OwnerRefused) {
					// A refused submission (stale claimant, superseded generation,
					// digest mismatch) is final: the owner keeps its state.
					this.log.info("submission refused", { taskId: sub.taskId, generation: sub.generation, reason: err.reason });
					return undefined;
				}
				if (!(err instanceof OwnerUnavailable)) throw err;
				this.metrics.submitRetries.inc();
				await new Promise((r) => setTimeout(r, Math.min(1000 * (i + 1), 5000)));
			}
		}
		return undefined;
	}

	/** Starts one BullMQ Worker per queue of each configured owner. */
	start(): void {
		for (const owner of owners) {
			if (!this.clients[owner]) continue;
			for (const name of ownerQueues[owner]) {
				const w = new Worker<JobPayload>(name, (job) => this.process(job).then(() => undefined), {
					connection: this.conn,
					prefix: prefixOf(this.cfg, owner),
					concurrency: this.cfg.worker.concurrency,
					lockDuration: this.cfg.worker.lockDurationMs,
					stalledInterval: this.cfg.worker.stalledIntervalMs,
					maxStalledCount: this.cfg.worker.maxStalledCount,
					autorun: false,
				});
				w.on("stalled", () => this.metrics.stalled.inc({ queue: name }));
				w.on("failed", (job, err) =>
					this.log.warn("delivery failed", {
						queue: name,
						jobId: String(job?.id),
						attempts: job?.attemptsMade ?? 0,
						error: err.message,
					}),
				);
				w.on("error", (err) => this.log.error("worker error", { queue: name, error: err.message }));
				this.workers.push(w);
				this.workerRuns.push(
					w.run().catch((err) => {
						w.emit("error", err);
					}),
				);
				this.queues.push(openQueue(this.cfg, owner, name, this.conn));
			}
		}
		this.observe = setInterval(() => void this.observeQueues(), 5000);
		this.log.info("background worker running", {
			instanceId: this.instanceId,
			queues: this.workers.map((w) => w.name).join(","),
			concurrency: this.cfg.worker.concurrency,
		});
	}

	/** The queue gauges: waiting/failed/delayed counts and the oldest waiting job's age. */
	async observeQueues(): Promise<void> {
		for (const q of this.queues) {
			try {
				const counts = await q.getJobCounts("waiting", "failed", "delayed", "active");
				this.metrics.queueWaiting.set({ queue: q.name }, counts.waiting ?? 0);
				this.metrics.queueFailed.set({ queue: q.name }, counts.failed ?? 0);
				this.metrics.queueDelayed.set({ queue: q.name }, counts.delayed ?? 0);
				this.metrics.active.set({ queue: q.name }, counts.active ?? 0);
				const oldest = await q.getJobs(["waiting"], 0, 0, true);
				this.metrics.queueLag.set({ queue: q.name }, oldest[0] ? (Date.now() - oldest[0].timestamp) / 1000 : 0);
			} catch (err) {
				this.log.warn("queue observation failed", { queue: q.name, error: String(err) });
			}
		}
	}

	/** One deadline covers intake, processors, queue finalization and connections. */
	stop(): Promise<boolean> {
		this.stopPromise ??= this.stopWithinDeadline();
		return this.stopPromise;
	}

	private async stopWithinDeadline(): Promise<boolean> {
		this.stopping = true;
		if (this.observe) clearInterval(this.observe);
		let timer: NodeJS.Timeout | undefined;
		let interruption: Promise<void> | undefined;
		const deadline = new Promise<void>((resolve, reject) => {
			timer = setTimeout(() => {
				this.forcing = true;
				for (const abort of this.abortHandlers) abort();
				// disconnect() interrupts both command and blocking connections;
				// it does not wait for Redis to answer QUIT. Await the actual closes.
				this.conn.disconnect();
				interruption = Promise.all(this.workers.map((w) => w.disconnect())).then(() => undefined);
				void interruption.then(resolve, reject);
			}, this.cfg.worker.shutdownTimeoutMs);
		});
		try {
			// Own run()'s promises so drain includes moveToFinished. pause(false)
			// would reconnect its blocking client after draining, even if a forced
			// close had meanwhile completed. Stop intake without that reconnect.
			const drain = (async () => {
				await Promise.all(this.workers.map((w) => w.pause(true)));
				await Promise.all(this.workers.map((w) => w.getBackend().disconnectBlocking()));
				await Promise.all([...this.workerRuns, ...this.running]);
			})();
			await Promise.race([drain, deadline]);
			// Choose close's mode once, after drain or deadline. If Redis stops
			// answering during graceful close, the same timer disconnects it;
			// never try to upgrade BullMQ's cached close with a second close(true).
			await Promise.all(this.workers.map((w) => w.close(this.forcing)));
			await Promise.all(this.queues.map((q) => q.close()));
			await interruption;
			const ended = this.conn.status === "end" ? Promise.resolve() : once(this.conn, "end");
			this.conn.disconnect();
			await ended;
			return this.forcing;
		} finally {
			clearTimeout(timer);
		}
	}

	get queueHandles(): Queue<JobPayload>[] {
		return this.queues;
	}
}
