// The client side of Knowledge's index and projection steps: each answer in
// knowledge.IndexState becomes the handler's progress, a FAILED answer
// without a code fails with the client's own code and an unknown state
// names the client's RPC. A scripted IngestService on loopback, no
// containers.
import {
	AdvanceIndexResponse,
	AdvanceProjectionResponse,
	IndexState,
	IngestServiceService,
} from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { credentials, Server, ServerCredentials } from "@grpc/grpc-js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { connectIndex, connectProjection, type IndexProgress, OwnerUnavailable } from "../src/owner.js";

interface Answer {
	state: IndexState;
	resultRef?: string;
	resultDigest?: string;
	failureCode?: string;
	retryAfterMs?: number;
}

const digest = `sha256:${"a".repeat(64)}`;
const transport = { creds: credentials.createInsecure(), options: {} };
const server = new Server();
let answer: Answer = { state: IndexState.INDEX_STATE_RUNNING };
let address = "";

beforeAll(async () => {
	server.addService(IngestServiceService, {
		advanceParse: () => {},
		advanceIndex: (_call: unknown, cb: (e: null, r: AdvanceIndexResponse) => void) =>
			cb(null, AdvanceIndexResponse.fromPartial(answer)),
		advanceProjection: (_call: unknown, cb: (e: null, r: AdvanceProjectionResponse) => void) =>
			cb(null, AdvanceProjectionResponse.fromPartial(answer)),
	});
	const port = await new Promise<number>((resolve, reject) =>
		server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (err, p) => (err ? reject(err) : resolve(p))),
	);
	address = `127.0.0.1:${port}`;
});

afterAll(() => server.forceShutdown());

const clients: { rpc: string; failedCode: string; advance: () => Promise<IndexProgress> }[] = [
	{
		rpc: "AdvanceIndex",
		failedCode: "INDEX_FAILED",
		advance: async () => {
			const c = connectIndex(address, 2000, transport);
			try {
				return await c.advanceIndex("task-1", "1", "worker-1", digest);
			} finally {
				c.close();
			}
		},
	},
	{
		rpc: "AdvanceProjection",
		failedCode: "PROJECTION_FAILED",
		advance: async () => {
			const c = connectProjection(address, 2000, transport);
			try {
				return await c.advanceProjection("task-1", "1", "worker-1", digest);
			} finally {
				c.close();
			}
		},
	},
];

describe.each(clients)("$rpc progress", ({ rpc, failedCode, advance }) => {
	it("reports a materialized step with the owner's reference and digest", async () => {
		answer = { state: IndexState.INDEX_STATE_MATERIALIZED, resultRef: "ref-1", resultDigest: digest };
		expect(await advance()).toEqual({ state: "materialized", resultRef: "ref-1", resultDigest: digest });
	});

	it("reports a running step with the owner's suggested wait", async () => {
		answer = { state: IndexState.INDEX_STATE_RUNNING, retryAfterMs: 250 };
		expect(await advance()).toEqual({ state: "running", retryAfterMs: 250 });
	});

	it("keeps the owner's failure code, or uses its own when none is given", async () => {
		answer = { state: IndexState.INDEX_STATE_FAILED, failureCode: "OWNER_CODE" };
		expect(await advance()).toEqual({ state: "failed", failureCode: "OWNER_CODE" });
		answer = { state: IndexState.INDEX_STATE_FAILED };
		expect(await advance()).toEqual({ state: "failed", failureCode: failedCode });
	});

	it("treats an unknown state as an unavailable owner, naming the call", async () => {
		answer = { state: IndexState.INDEX_STATE_UNSPECIFIED };
		const err = await advance().catch((e: unknown) => e);
		expect(err).toBeInstanceOf(OwnerUnavailable);
		expect((err as Error).message).toBe(`${rpc} answered state ${IndexState.INDEX_STATE_UNSPECIFIED}`);
	});
});
