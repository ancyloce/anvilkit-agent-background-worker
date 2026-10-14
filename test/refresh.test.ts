// P2.5: the mcp-catalog-refresh handler over MCP's CatalogRefreshService.
import { status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";
import { OwnerRefused, OwnerUnavailable, type RefreshClient, type RefreshProgress } from "../src/owner.js";
import { HandlerError, handlersFor, mcpCatalogRefresh } from "../src/worker/handlers.js";

const digest = `sha256:${"b".repeat(64)}`;
const input = Buffer.from(
	JSON.stringify({ schemaVersion: 1, serverId: "srv-1", revision: 2, descriptorDigest: digest }),
);

function scripted(answers: (RefreshProgress | Error)[]): RefreshClient & { calls: unknown[][] } {
	const calls: unknown[][] = [];
	return {
		calls,
		async advanceRefresh(...args) {
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
	taskId: "refresh-srv-1-r2-487",
	generation: "1",
	workerId: "w1",
	inputDigest: digest,
	signal,
});

describe("mcp-catalog-refresh handler", () => {
	it("reports exactly MCP's recorded reference and digest, drift or not", async () => {
		const client = scripted([
			{ state: "completed", resultRef: "mcp-catalog-refresh:refresh-srv-1-r2-487:1", resultDigest: digest },
		]);
		expect(await mcpCatalogRefresh(client)(input, ctx())).toEqual({
			succeeded: true,
			resultRef: "mcp-catalog-refresh:refresh-srv-1-r2-487:1",
			resultDigest: digest,
		});
		expect(client.calls).toEqual([["refresh-srv-1-r2-487", "1", "w1", digest]]);
	});

	it("asks again after an unreachable owner, reports a failed read and ends on a refusal", async () => {
		const retried = scripted([
			new OwnerUnavailable("UNAVAILABLE"),
			{ state: "completed", resultRef: "mcp-catalog-refresh:refresh-srv-1-r2-487:1", resultDigest: digest },
		]);
		expect((await mcpCatalogRefresh(retried)(input, ctx())).succeeded).toBe(true);
		expect(retried.calls).toHaveLength(2);
		expect(
			await mcpCatalogRefresh(scripted([{ state: "failed", failureCode: "REFRESH_UNAVAILABLE" }]))(input, ctx()),
		).toEqual({
			succeeded: false,
			failureCode: "REFRESH_UNAVAILABLE",
		});
		const refused = scripted([new OwnerRefused(status.FAILED_PRECONDITION, "STALE_EXECUTION", "stale")]);
		await expect(mcpCatalogRefresh(refused)(input, ctx())).rejects.toEqual(new HandlerError("STALE_EXECUTION"));
	}, 20_000);

	it("refuses input that is not a catalog-refresh input and registers only with MCP's refresh client", async () => {
		const bad = Buffer.from(JSON.stringify({ schemaVersion: 1, computation: "local-check-v1" }));
		await expect(mcpCatalogRefresh(scripted([]))(bad, ctx())).rejects.toEqual(new HandlerError("INPUT_MALFORMED"));
		expect(Object.keys(handlersFor())).toEqual(["local-check"]);
		expect(Object.keys(handlersFor(undefined, undefined, undefined, scripted([])))).toEqual([
			"local-check",
			"mcp-catalog-refresh",
		]);
	});
});
