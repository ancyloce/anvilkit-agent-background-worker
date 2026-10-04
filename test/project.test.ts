import { status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";
import { type IndexClient, type IndexProgress, OwnerRefused, OwnerUnavailable } from "../src/owner.js";
import { HandlerError, handlersFor, knowledgeProject, memoryProject } from "../src/worker/handlers.js";

const input = Buffer.from(JSON.stringify({ schemaVersion: 1, computation: "knowledge-index-v1", sourceId: "src-1" }));
const digest = `sha256:${"b".repeat(64)}`;

function scripted(answers: (IndexProgress | Error)[]): IndexClient & { calls: unknown[][] } {
	const calls: unknown[][] = [];
	return {
		calls,
		async advanceIndex(...args) {
			calls.push(args);
			const next = answers.shift();
			if (!next) throw new Error("no more answers");
			if (next instanceof Error) throw next;
			return next;
		},
		close() {},
	};
}

const ctx = (signal = new AbortController().signal) => ({
	taskId: "index-src-1-r1-g1",
	generation: "1",
	workerId: "w1",
	inputDigest: digest,
	signal,
});

describe("knowledge-project handler", () => {
	it("advances batch after batch until Knowledge records the manifest and reports exactly its reference and digest", async () => {
		const client = scripted([
			{ state: "running", retryAfterMs: 0 },
			{ state: "running", retryAfterMs: 0 },
			new OwnerUnavailable("UNAVAILABLE"),
			{ state: "running", retryAfterMs: 1 },
			{ state: "materialized", resultRef: "index:1:src-1@1", resultDigest: digest },
		]);
		const r = await knowledgeProject(client)(input, ctx());
		expect(r).toEqual({ succeeded: true, resultRef: "index:1:src-1@1", resultDigest: digest });
		expect(client.calls).toHaveLength(5);
		expect(client.calls[0]).toEqual(["index-src-1-r1-g1", "1", "w1", digest]);
	}, 20_000);

	it("reports a failed index step as a failed attempt, ends on a refusal and stops when the lease is lost", async () => {
		expect(
			await knowledgeProject(scripted([{ state: "failed", failureCode: "PROFILE_UNQUALIFIED" }]))(input, ctx()),
		).toEqual({ succeeded: false, failureCode: "PROFILE_UNQUALIFIED" });
		const refused = scripted([new OwnerRefused(status.FAILED_PRECONDITION, "STALE_EXECUTION", "stale")]);
		await expect(knowledgeProject(refused)(input, ctx())).rejects.toEqual(new HandlerError("STALE_EXECUTION"));
		const ac = new AbortController();
		const pending = knowledgeProject(scripted([{ state: "running", retryAfterMs: 60_000 }]))(input, ctx(ac.signal));
		setTimeout(() => ac.abort(new Error("lease lost")), 50);
		await expect(pending).rejects.toEqual(new HandlerError("ABORTED"));
		const bad = Buffer.from(JSON.stringify({ schemaVersion: 1, computation: "knowledge-ingest-v1" }));
		await expect(knowledgeProject(scripted([]))(bad, ctx())).rejects.toEqual(new HandlerError("INPUT_MALFORMED"));
	});

	it("registers knowledge-project only with a Knowledge index client", () => {
		expect(Object.keys(handlersFor())).toEqual(["local-check"]);
		expect(Object.keys(handlersFor(undefined, scripted([])))).toEqual(["local-check", "knowledge-project"]);
	});
});

describe("memory-project handler", () => {
	const memInput = Buffer.from(
		JSON.stringify({ schemaVersion: 1, computation: "memory-project-v1", factId: "mem-1", factRevision: 2 }),
	);
	const projection = (answers: (IndexProgress | Error)[]) => {
		const calls: unknown[][] = [];
		return {
			calls,
			async advanceProjection(...args: unknown[]) {
				calls.push(args);
				const next = answers.shift();
				if (!next) throw new Error("no more answers");
				if (next instanceof Error) throw next;
				return next;
			},
			close() {},
		};
	};

	it("steps until Knowledge verified the projection and reports exactly its reference and digest", async () => {
		const client = projection([
			{ state: "running", retryAfterMs: 1 },
			new OwnerUnavailable("UNAVAILABLE"),
			{ state: "materialized", resultRef: "memory:0:mem-1@2", resultDigest: digest },
		]);
		const r = await memoryProject(client)(memInput, ctx());
		expect(r).toEqual({ succeeded: true, resultRef: "memory:0:mem-1@2", resultDigest: digest });
		expect(client.calls[0]).toEqual(["index-src-1-r1-g1", "1", "w1", digest]);
	}, 20_000);

	it("fails on a superseded projection, ends on a refusal and refuses another computation", async () => {
		expect(await memoryProject(projection([{ state: "failed", failureCode: "SUPERSEDED" }]))(memInput, ctx())).toEqual({
			succeeded: false,
			failureCode: "SUPERSEDED",
		});
		const refused = projection([new OwnerRefused(status.FAILED_PRECONDITION, "STALE_EXECUTION", "stale")]);
		await expect(memoryProject(refused)(memInput, ctx())).rejects.toEqual(new HandlerError("STALE_EXECUTION"));
		await expect(memoryProject(projection([]))(input, ctx())).rejects.toEqual(new HandlerError("INPUT_MALFORMED"));
		expect(Object.keys(handlersFor(undefined, undefined, projection([])))).toEqual(["local-check", "memory-project"]);
	});
});
