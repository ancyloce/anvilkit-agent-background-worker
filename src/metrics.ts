// The lane's Prometheus signals on the queue side (DD-09 §6): queue lag,
// failures and stalls, inbox duplicates and gaps seen by the relay,
// reconstructed entries, durable-request differences, handler outcomes,
// lease losses and the shutdown drain. No identifier or body is a label.
import { Counter, Gauge, type Registry } from "prom-client";

export class Metrics {
	readonly jobs: Counter<"queue" | "outcome">;
	readonly active: Gauge<"queue">;
	readonly queueWaiting: Gauge<"queue">;
	readonly queueFailed: Gauge<"queue">;
	readonly queueDelayed: Gauge<"queue">;
	readonly queueLag: Gauge<"queue">;
	readonly stalled: Counter<"queue">;
	readonly leaseLost: Counter;
	readonly handlerTimeouts: Counter;
	readonly submitRetries: Counter;
	readonly inbox: Counter<"outcome">;
	readonly inboxDuplicates: Counter;
	readonly inboxGaps: Counter;
	readonly relayed: Counter;
	readonly reconstructed: Counter;
	readonly withoutQueueEntry: Gauge;
	readonly drainSeconds: Gauge;
	readonly forcedStop: Gauge;
	readonly configGeneration: Gauge;

	constructor(registry: Registry) {
		const r = [registry];
		this.jobs = new Counter({
			name: "anvilkit_background_worker_jobs_total",
			help: "Jobs processed by queue and outcome.",
			labelNames: ["queue", "outcome"],
			registers: r,
		});
		this.active = new Gauge({
			name: "anvilkit_background_worker_active_jobs",
			help: "Jobs being processed.",
			labelNames: ["queue"],
			registers: r,
		});
		this.queueWaiting = new Gauge({
			name: "anvilkit_background_queue_waiting",
			help: "Waiting jobs.",
			labelNames: ["queue"],
			registers: r,
		});
		this.queueFailed = new Gauge({
			name: "anvilkit_background_queue_failed",
			help: "Failed jobs kept for inspection.",
			labelNames: ["queue"],
			registers: r,
		});
		this.queueDelayed = new Gauge({
			name: "anvilkit_background_queue_delayed",
			help: "Delayed jobs (backoff).",
			labelNames: ["queue"],
			registers: r,
		});
		this.queueLag = new Gauge({
			name: "anvilkit_background_queue_oldest_waiting_seconds",
			help: "Age of the oldest waiting job.",
			labelNames: ["queue"],
			registers: r,
		});
		this.stalled = new Counter({
			name: "anvilkit_background_worker_stalled_total",
			help: "Stalled jobs reported by BullMQ.",
			labelNames: ["queue"],
			registers: r,
		});
		this.leaseLost = new Counter({
			name: "anvilkit_background_worker_lease_lost_total",
			help: "Handlers stopped because the owner refused the heartbeat.",
			registers: r,
		});
		this.handlerTimeouts = new Counter({
			name: "anvilkit_background_worker_handler_timeouts_total",
			help: "Handlers stopped by the handler timeout.",
			registers: r,
		});
		this.submitRetries = new Counter({
			name: "anvilkit_background_worker_submit_retries_total",
			help: "Identical resubmissions after a lost receipt.",
			registers: r,
		});
		this.inbox = new Counter({
			name: "anvilkit_background_relay_inbox_total",
			help: "Inbox decisions by outcome.",
			labelNames: ["outcome"],
			registers: r,
		});
		this.inboxDuplicates = new Counter({
			name: "anvilkit_background_relay_inbox_duplicates_total",
			help: "Redelivered events answered from the inbox.",
			registers: r,
		});
		this.inboxGaps = new Counter({
			name: "anvilkit_background_relay_inbox_gaps_total",
			help: "Events ahead of the durable request (repaired through the owner query).",
			registers: r,
		});
		this.relayed = new Counter({
			name: "anvilkit_background_relay_enqueued_total",
			help: "Queue entries added by the relay.",
			registers: r,
		});
		this.reconstructed = new Counter({
			name: "anvilkit_background_relay_reconstructed_total",
			help: "Queue entries rebuilt from durable requests.",
			registers: r,
		});
		this.withoutQueueEntry = new Gauge({
			name: "anvilkit_background_relay_requests_without_queue_entry",
			help: "Durable requests due for work that had no queue entry at the last reconcile.",
			registers: r,
		});
		this.drainSeconds = new Gauge({
			name: "anvilkit_background_worker_shutdown_drain_seconds",
			help: "Seconds the last shutdown spent draining.",
			registers: r,
		});
		this.forcedStop = new Gauge({
			name: "anvilkit_background_worker_shutdown_forced",
			help: "1 when the last shutdown had to force-stop.",
			registers: r,
		});
		this.configGeneration = new Gauge({
			name: "anvilkit_background_worker_config_generation",
			help: "Number of the active configuration generation.",
			registers: r,
		});
	}
}
