import { status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";
import { type IngestClient, OwnerRefused, OwnerUnavailable, type ParseProgress } from "../src/owner.js";
import { HandlerError, handlersFor, knowledgeIngest } from "../src/worker/handlers.js";

const input = Buffer.from(JSON.stringify({ schemaVersion: 1, computation: "knowledge-ingest-v1", sourceId: "src-1" }));
const digest = `sha256:${"a".repeat(64)}`;

function scripted(answers: (ParseProgress | Error)[]): IngestClient & { calls: unknown[][] } {
	const calls: unknown[][] = [];
	return {
		calls,
		async advance(...args) {
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
	taskId: "ingest-src-1-r1",
	generation: "1",
	workerId: "w1",
	inputDigest: digest,
	signal,
});

describe("knowledge-ingest handler", () => {
	it("polls the claim's parse until Knowledge completes it and reports exactly Knowledge's reference and digest", async () => {
		const client = scripted([
			{ state: "launched", retryAfterMs: 1 },
			new OwnerUnavailable("UNAVAILABLE"),
			{ state: "running", retryAfterMs: 1 },
			{ state: "completed", resultRef: "parse:parse-abc", resultDigest: digest },
		]);
		const r = await knowledgeIngest(client)(input, ctx());
		expect(r).toEqual({ succeeded: true, resultRef: "parse:parse-abc", resultDigest: digest });
		expect(client.calls).toHaveLength(4);
		expect(client.calls[0]).toEqual(["ingest-src-1-r1", "1", "w1", digest]);
	}, 20_000);

	it("reports a failed parse as a failed attempt and ends on a refusal", async () => {
		expect(
			await knowledgeIngest(scripted([{ state: "failed", failureCode: "PARSE_RESULT_INVALID" }]))(input, ctx()),
		).toEqual({ succeeded: false, failureCode: "PARSE_RESULT_INVALID" });
		const refused = scripted([new OwnerRefused(status.FAILED_PRECONDITION, "STALE_EXECUTION", "stale")]);
		await expect(knowledgeIngest(refused)(input, ctx())).rejects.toEqual(new HandlerError("STALE_EXECUTION"));
	});

	it("stops when the lease is lost and refuses input that is not a knowledge-ingest input", async () => {
		const ac = new AbortController();
		const pending = knowledgeIngest(scripted([{ state: "running", retryAfterMs: 60_000 }]))(input, ctx(ac.signal));
		setTimeout(() => ac.abort(new Error("lease lost")), 50);
		await expect(pending).rejects.toEqual(new HandlerError("ABORTED"));
		const bad = Buffer.from(JSON.stringify({ schemaVersion: 1, computation: "local-check-v1" }));
		await expect(knowledgeIngest(scripted([]))(bad, ctx())).rejects.toEqual(new HandlerError("INPUT_MALFORMED"));
		await expect(knowledgeIngest(scripted([]))(Buffer.from('{"a":1,"a":1}'), ctx())).rejects.toEqual(
			new HandlerError("INPUT_MALFORMED"),
		);
	});

	it("registers knowledge-ingest only with a Knowledge IngestService client", () => {
		expect(Object.keys(handlersFor())).toEqual(["local-check"]);
		expect(Object.keys(handlersFor(scripted([])))).toEqual(["local-check", "knowledge-ingest"]);
	});
});
