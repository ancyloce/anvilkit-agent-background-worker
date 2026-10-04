import { jetstream } from "@nats-io/jetstream";
import { Queue } from "bullmq";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Config } from "../src/config.js";
import { silentLogger } from "../src/log.js";
import type { OwnerClient, TaskView } from "../src/owner.js";
import { connection, jobIdOf } from "../src/queue.js";
import { type InboxOutcome, Relay } from "../src/relay/relay.js";
import { type Lab, labConfig, metricValue, newMetrics, requestedEvent, requestFixture, startLab } from "./harness.js";

let lab: Lab;
let cfg: Config;

/** The owner's query for gap repair: scripted answers. */
class FakeOwner implements OwnerClient {
	readonly owner = "knowledge" as const;
	answers = new Map<string, TaskView>();
	claim(): Promise<never> {
		throw new Error("not used");
	}
	heartbeat(): Promise<never> {
		throw new Error("not used");
	}
	submit(): Promise<never> {
		throw new Error("not used");
	}
	get(taskId: string): Promise<TaskView> {
		const t = this.answers.get(taskId);
		return t ? Promise.resolve(t) : Promise.reject(new Error("NOT_FOUND"));
	}
	close(): void {}
}

const conns: ReturnType<typeof connection>[] = [];
const relays: Relay[] = [];

function newRelay(owner = new FakeOwner()) {
	const conn = connection(cfg.queue.url);
	conns.push(conn);
	const { metrics, registry } = newMetrics();
	const relay = new Relay(cfg, "knowledge", owner, conn, metrics, silentLogger);
	relays.push(relay);
	return { relay, registry, conn, owner };
}

async function outcomes(): Promise<InboxOutcome[]> {
	const r = await lab.admin(
		"SELECT outcome FROM inbox WHERE consumer = 'anvilkit-agent-knowledge-background-relay' ORDER BY accepted_at",
	);
	return r.rows.map((x) => x.outcome as InboxOutcome);
}

function handleOnce(relay: Relay, data: Uint8Array): Promise<{ acked?: InboxOutcome; nak: boolean; term: boolean }> {
	const out: { acked?: InboxOutcome; nak: boolean; term: boolean } = { nak: false, term: false };
	return relay
		.handle(
			data,
			(o) => {
				out.acked = o;
			},
			() => {
				out.nak = true;
			},
			() => {
				out.term = true;
			},
		)
		.then(() => out);
}

beforeAll(async () => {
	lab = await startLab();
	cfg = labConfig(lab);
});

afterAll(async () => {
	for (const r of relays) await r.stop().catch(() => undefined);
	for (const c of conns) c.disconnect();
	await lab.stop();
});

describe("owner queue relay", () => {
	it("commits the inbox before the ack, enqueues once, answers duplicates from the inbox", async () => {
		const { relay, registry, conn } = newRelay();
		const queue = new Queue("local-check", { connection: conn, prefix: "anvilkit:knowledge" });
		const { inputDigest } = await requestFixture(lab, "task_1", 1, "pending");
		const event = requestedEvent("task_1", 1, inputDigest);
		// Crash before the ACK: the inbox row and the queue entry exist, the message is redelivered.
		const first = await handleOnce(relay, event);
		expect(first.acked).toBe("enqueue");
		const job = await queue.getJob(jobIdOf("task_1", "1"));
		expect(job).toBeDefined();
		expect((job as NonNullable<typeof job>).data).toEqual({
			owner: "knowledge",
			taskId: "task_1",
			generation: "1",
			taskKind: "local-check",
			tenantId: "tenant_a",
			inputDigest,
			correlationId: "req_task_1",
		});
		const again = await handleOnce(relay, event);
		expect(again.acked).toBe("enqueue");
		expect(await metricValue(registry, "anvilkit_background_relay_inbox_duplicates_total")).toBe(1);
		expect((await queue.getJobCounts("waiting")).waiting).toBe(1);
		expect(await outcomes()).toEqual(["enqueue"]);
		await queue.close();
	});

	it("ignores stale generations and terminal requests, rejects cross-tenant and invalid messages without touching facts", async () => {
		const { relay, registry, conn } = newRelay();
		const queue = new Queue("local-check", { connection: conn, prefix: "anvilkit:knowledge" });
		const { inputDigest: d1 } = await requestFixture(lab, "task_2", 1, "stale");
		const { inputDigest: d2 } = await requestFixture(lab, "task_2", 2, "pending");
		expect((await handleOnce(relay, requestedEvent("task_2", 1, d1))).acked).toBe("ignored_stale");
		expect(await queue.getJob(jobIdOf("task_2", "1"))).toBeUndefined();
		expect((await handleOnce(relay, requestedEvent("task_2", 2, d2, { tenantId: "tenant_b" }))).acked).toBe(
			"rejected_tenant",
		);
		expect(await queue.getJob(jobIdOf("task_2", "2"))).toBeUndefined();
		const { inputDigest: d3 } = await requestFixture(lab, "task_3", 1, "accepted");
		expect((await handleOnce(relay, requestedEvent("task_3", 1, d3))).acked).toBe("ignored_terminal");
		// Invalid schema: terminated, no inbox row.
		const before = (await outcomes()).length;
		const invalid = await handleOnce(
			relay,
			Buffer.from(JSON.stringify({ eventId: "x", eventType: "background.requested", token: "secret" })),
		);
		expect(invalid.term).toBe(true);
		expect(invalid.acked).toBeUndefined();
		expect((await outcomes()).length).toBe(before);
		expect(await metricValue(registry, "anvilkit_background_relay_inbox_total", { outcome: "invalid_schema" })).toBe(1);
		// A message of another owner's subject is not this relay's.
		const foreign = await handleOnce(
			relay,
			requestedEvent("task_3", 1, d3, { subject: "anvilkit.mcp.background.requested", producer: "anvilkit-agent-mcp" }),
		);
		expect(foreign.term).toBe(true);
		const facts = await lab.admin(
			"SELECT state FROM background_requests WHERE task_id IN ('task_2','task_3') ORDER BY task_id, generation",
		);
		expect(facts.rows.map((r) => r.state)).toEqual(["stale", "pending", "accepted"]);
		await queue.close();
	});

	it("repairs a revision gap through the owner query and reconstructs lost queue entries from durable requests", async () => {
		const owner = new FakeOwner();
		const { relay, registry, conn } = newRelay(owner);
		const queue = new Queue("local-check", { connection: conn, prefix: "anvilkit:knowledge" });
		// Gap: the event names a generation the relay's database does not have; the owner says it is pending.
		const { inputDigest } = await requestFixture(lab, "task_4", 1, "pending");
		owner.answers.set("task_4", {
			taskId: "task_4",
			generation: "2",
			taskKind: "local-check",
			inputDigest,
			state: "pending",
			attemptCount: "0",
		});
		expect((await handleOnce(relay, requestedEvent("task_4", 2, inputDigest))).acked).toBe("gap_enqueue");
		expect(await metricValue(registry, "anvilkit_background_relay_inbox_gaps_total")).toBe(1);
		expect(await queue.getJob(jobIdOf("task_4", "2"))).toBeDefined();
		owner.answers.set("task_5", {
			taskId: "task_5",
			generation: "1",
			taskKind: "local-check",
			inputDigest,
			state: "canceled",
			attemptCount: "0",
		});
		expect((await handleOnce(relay, requestedEvent("task_5", 1, inputDigest))).acked).toBe("gap_ignored");
		expect(await queue.getJob(jobIdOf("task_5", "1"))).toBeUndefined();
		// Lost queue writes: the durable requests rebuild their entries; terminal ones never regain admission.
		const { inputDigest: d6 } = await requestFixture(lab, "task_6", 1, "retry_scheduled");
		await requestFixture(lab, "task_7", 1, "dead");
		expect((await handleOnce(relay, requestedEvent("task_6", 1, d6))).acked).toBe("enqueue");
		await queue.obliterate({ force: true });
		expect(await queue.getJob(jobIdOf("task_6", "1"))).toBeUndefined();
		const r = await relay.reconcile();
		expect(r.rebuilt).toBeGreaterThanOrEqual(2);
		expect(await queue.getJob(jobIdOf("task_6", "1"))).toBeDefined();
		expect(await queue.getJob(jobIdOf("task_1", "1"))).toBeDefined();
		expect(await queue.getJob(jobIdOf("task_7", "1"))).toBeUndefined();
		expect(await queue.getJob(jobIdOf("task_3", "1"))).toBeUndefined();
		const second = await relay.reconcile();
		expect(second.rebuilt).toBe(0);
		// A finished job with the same id does not block a re-request of the generation.
		const stuck = await queue.getJob(jobIdOf("task_6", "1"));
		await stuck?.moveToFailed(new Error("delivery failed"), "token", false).catch(() => undefined);
		await queue.close();
	});

	it("consumes the fixed durable consumer on a real JetStream and acks after the inbox commit", async () => {
		const { relay, conn } = newRelay();
		const queue = new Queue("local-check", { connection: conn, prefix: "anvilkit:knowledge" });
		await relay.start();
		const { inputDigest } = await requestFixture(lab, "task_8", 1, "pending");
		const js = jetstream(lab.nc);
		const event = requestedEvent("task_8", 1, inputDigest);
		await js.publish("anvilkit.knowledge.background.requested", event);
		await js.publish("anvilkit.knowledge.background.requested", event);
		const deadline = Date.now() + 15_000;
		while (Date.now() < deadline && !(await queue.getJob(jobIdOf("task_8", "1"))))
			await new Promise((r) => setTimeout(r, 100));
		expect(await queue.getJob(jobIdOf("task_8", "1"))).toBeDefined();
		await new Promise((r) => setTimeout(r, 500));
		const inbox = await lab.admin(
			"SELECT count(*)::int AS n FROM inbox WHERE consumer = 'anvilkit-agent-knowledge-background-relay'",
		);
		expect(inbox.rows[0]?.n).toBeGreaterThanOrEqual(1);
		await relay.stop();
		await queue.close();
	});
});
