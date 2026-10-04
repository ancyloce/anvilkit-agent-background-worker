// The Worker's and the relay's clients of the owners' BackgroundTaskService
// (anvilkit.knowledge.v1 and anvilkit.mcp.v1, the identical owner contract):
// every request passes the contract's explicit TypeScript validation before
// it is sent; a refusal keeps the owner's public code. The Worker never
// touches a domain table: these calls are its only path to durable state.
import * as knowledge from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import * as mcp from "@anvilkit/generated-clients/proto/anvilkit/mcp/v1/mcp";
import { validateJson } from "@anvilkit/generated-clients/validation/rpc";
import { type ChannelCredentials, credentials, Metadata, type ServiceError, status } from "@grpc/grpc-js";
import type { Owner } from "./config.js";

export type TaskStateName =
	| "pending"
	| "leased"
	| "result_submitted"
	| "accepted"
	| "retry_scheduled"
	| "dead"
	| "stale"
	| "canceled"
	| "unspecified";

export interface TaskView {
	taskId: string;
	generation: string;
	taskKind: string;
	inputDigest: string;
	state: TaskStateName;
	workerId?: string;
	leaseUntil?: Date;
	attemptCount: string;
}

/** The owner refused on a precondition; code is the gRPC status, reason the public code in front of the message. */
export class OwnerRefused extends Error {
	constructor(
		readonly code: status,
		readonly reason: string,
		message: string,
	) {
		super(message);
	}
}

/** The owner could not be asked or did not answer; nothing is established. */
export class OwnerUnavailable extends Error {}

export interface SubmitInput {
	taskId: string;
	generation: string;
	workerId: string;
	inputDigest: string;
	succeeded: boolean;
	resultRef: string;
	resultDigest: string;
	failureCode?: string;
}

export interface OwnerClient {
	readonly owner: Owner;
	claim(
		taskId: string,
		generation: string,
		workerId: string,
		leaseSeconds: number,
	): Promise<{ task: TaskView; input: Buffer }>;
	heartbeat(taskId: string, generation: string, workerId: string): Promise<TaskView>;
	submit(input: SubmitInput): Promise<{ task: TaskView; accepted: boolean; existing: boolean }>;
	get(taskId: string): Promise<TaskView>;
	close(): void;
}

const stateNames: Record<number, TaskStateName> = {
	0: "unspecified",
	1: "pending",
	2: "leased",
	3: "result_submitted",
	4: "accepted",
	5: "retry_scheduled",
	6: "dead",
	7: "stale",
	8: "canceled",
};

type ProtoTask = {
	taskId: string;
	generation: string;
	taskKind: string;
	inputDigest: string;
	state: number;
	workerId?: string;
	leaseUntil?: Date;
	attemptCount: string;
};

function view(t: ProtoTask | undefined): TaskView {
	if (!t) throw new OwnerUnavailable("owner answered without a task");
	return {
		taskId: t.taskId,
		generation: t.generation,
		taskKind: t.taskKind,
		inputDigest: t.inputDigest,
		state: stateNames[t.state] ?? "unspecified",
		workerId: t.workerId,
		leaseUntil: t.leaseUntil,
		attemptCount: t.attemptCount,
	};
}

function mapError(err: ServiceError): Error {
	switch (err.code) {
		case status.UNAVAILABLE:
		case status.DEADLINE_EXCEEDED:
		case status.CANCELLED:
		case status.UNKNOWN:
		case status.INTERNAL:
		case status.RESOURCE_EXHAUSTED:
			return new OwnerUnavailable(`${status[err.code]}: ${err.details}`);
		default: {
			const reason = /^([A-Z_]+)/.exec(err.details)?.[1] ?? status[err.code] ?? "REFUSED";
			return new OwnerRefused(err.code, reason, err.details);
		}
	}
}

function validated(typeName: string, json: unknown): void {
	const v = validateJson(typeName, JSON.stringify(json));
	if (!v.valid)
		throw new Error(
			`${typeName} invalid before send: ${v.reason === "invalid" ? v.violations.map((x) => x.message).join("; ") : v.error.message}`,
		);
}

interface RawClaim {
	task?: ProtoTask;
	input: Buffer;
}
interface RawTask {
	task?: ProtoTask;
}
interface RawSubmit {
	task?: ProtoTask;
	accepted: boolean;
	existing: boolean;
}

/** The typed grpc-js calls of one owner's package, validated before the send. */
interface Binding {
	claim(taskId: string, generation: string, workerId: string, leaseSeconds: number): Promise<RawClaim>;
	heartbeat(taskId: string, generation: string, workerId: string): Promise<RawTask>;
	submit(input: SubmitInput): Promise<RawSubmit>;
	get(taskId: string): Promise<RawTask>;
	close(): void;
}

function promised<Res>(
	timeoutMs: number,
	fn: (md: Metadata, opts: { deadline: number }, cb: (err: ServiceError | null, res: Res) => void) => unknown,
): Promise<Res> {
	return new Promise((resolve, reject) =>
		fn(new Metadata(), { deadline: Date.now() + timeoutMs }, (err, res) =>
			err ? reject(mapError(err)) : resolve(res),
		),
	);
}

function knowledgeBinding(address: string, timeoutMs: number, creds: ChannelCredentials): Binding {
	const c = new knowledge.BackgroundTaskServiceClient(address, creds);
	const ns = "anvilkit.knowledge.v1";
	return {
		claim(taskId, generation, workerId, leaseSeconds) {
			const req = knowledge.ClaimTaskRequest.fromPartial({ taskId, generation, workerId, leaseSeconds });
			validated(`${ns}.ClaimTaskRequest`, knowledge.ClaimTaskRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.claimTask(req, md, o, cb));
		},
		heartbeat(taskId, generation, workerId) {
			const req = knowledge.HeartbeatTaskRequest.fromPartial({ taskId, generation, workerId });
			validated(`${ns}.HeartbeatTaskRequest`, knowledge.HeartbeatTaskRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.heartbeatTask(req, md, o, cb));
		},
		submit(input) {
			const req = knowledge.SubmitTaskResultRequest.fromPartial({
				...input,
				failureCode: input.failureCode || undefined,
			});
			validated(`${ns}.SubmitTaskResultRequest`, knowledge.SubmitTaskResultRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.submitTaskResult(req, md, o, cb));
		},
		get(taskId) {
			const req = knowledge.GetTaskRequest.fromPartial({ taskId });
			validated(`${ns}.GetTaskRequest`, knowledge.GetTaskRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.getTask(req, md, o, cb));
		},
		close: () => c.close(),
	};
}

function mcpBinding(address: string, timeoutMs: number, creds: ChannelCredentials): Binding {
	const c = new mcp.BackgroundTaskServiceClient(address, creds);
	const ns = "anvilkit.mcp.v1";
	return {
		claim(taskId, generation, workerId, leaseSeconds) {
			const req = mcp.ClaimTaskRequest.fromPartial({ taskId, generation, workerId, leaseSeconds });
			validated(`${ns}.ClaimTaskRequest`, mcp.ClaimTaskRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.claimTask(req, md, o, cb));
		},
		heartbeat(taskId, generation, workerId) {
			const req = mcp.HeartbeatTaskRequest.fromPartial({ taskId, generation, workerId });
			validated(`${ns}.HeartbeatTaskRequest`, mcp.HeartbeatTaskRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.heartbeatTask(req, md, o, cb));
		},
		submit(input) {
			const req = mcp.SubmitTaskResultRequest.fromPartial({ ...input, failureCode: input.failureCode || undefined });
			validated(`${ns}.SubmitTaskResultRequest`, mcp.SubmitTaskResultRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.submitTaskResult(req, md, o, cb));
		},
		get(taskId) {
			const req = mcp.GetTaskRequest.fromPartial({ taskId });
			validated(`${ns}.GetTaskRequest`, mcp.GetTaskRequest.toJSON(req));
			return promised(timeoutMs, (md, o, cb) => c.getTask(req, md, o, cb));
		},
		close: () => c.close(),
	};
}

/** One owner's client over grpc-js; plaintext (DEVELOPMENT_ONLY; workload mTLS is ENV-03). */
export function connectOwner(
	owner: Owner,
	address: string,
	timeoutMs: number,
	creds: ChannelCredentials = credentials.createInsecure(),
): OwnerClient {
	const b = owner === "knowledge" ? knowledgeBinding(address, timeoutMs, creds) : mcpBinding(address, timeoutMs, creds);
	return {
		owner,
		async claim(taskId, generation, workerId, leaseSeconds) {
			const res = await b.claim(taskId, generation, workerId, leaseSeconds);
			return { task: view(res.task), input: Buffer.from(res.input) };
		},
		async heartbeat(taskId, generation, workerId) {
			return view((await b.heartbeat(taskId, generation, workerId)).task);
		},
		async submit(input) {
			const res = await b.submit(input);
			return { task: view(res.task), accepted: res.accepted, existing: res.existing };
		},
		async get(taskId) {
			return view((await b.get(taskId)).task);
		},
		close: () => b.close(),
	};
}

/** The parse step's progress as Knowledge reports it (IngestService.AdvanceParse). */
export type ParseProgress =
	| { state: "launched" | "running"; retryAfterMs: number }
	| { state: "completed"; resultRef: string; resultDigest: string }
	| { state: "failed"; failureCode: string };

export interface IngestClient {
	advance(taskId: string, generation: string, workerId: string, inputDigest: string): Promise<ParseProgress>;
	close(): void;
}

/**
 * Knowledge's IngestService: the current claimant asks Knowledge to launch
 * and observe the parser Job of its claim. The answer never carries a key,
 * a URL or document text; Knowledge decides what the result is.
 */
export function connectIngest(
	address: string,
	timeoutMs: number,
	creds: ChannelCredentials = credentials.createInsecure(),
): IngestClient {
	const c = new knowledge.IngestServiceClient(address, creds);
	return {
		async advance(taskId, generation, workerId, inputDigest) {
			const req = knowledge.AdvanceParseRequest.fromPartial({ taskId, generation, workerId, inputDigest });
			validated("anvilkit.knowledge.v1.AdvanceParseRequest", knowledge.AdvanceParseRequest.toJSON(req));
			const res = await promised<knowledge.AdvanceParseResponse>(timeoutMs, (md, o, cb) =>
				c.advanceParse(req, md, o, cb),
			);
			switch (res.state) {
				case knowledge.ParseState.PARSE_STATE_COMPLETED:
					return { state: "completed", resultRef: res.resultRef, resultDigest: res.resultDigest };
				case knowledge.ParseState.PARSE_STATE_FAILED:
					return { state: "failed", failureCode: res.failureCode || "PARSE_FAILED" };
				case knowledge.ParseState.PARSE_STATE_LAUNCHED:
					return { state: "launched", retryAfterMs: res.retryAfterMs };
				case knowledge.ParseState.PARSE_STATE_RUNNING:
					return { state: "running", retryAfterMs: res.retryAfterMs };
				default:
					throw new OwnerUnavailable(`AdvanceParse answered state ${res.state}`);
			}
		},
		close: () => c.close(),
	};
}

/** The index step's progress as Knowledge reports it (IngestService.AdvanceIndex, P16). */
export type IndexProgress =
	| { state: "running"; retryAfterMs: number }
	| { state: "materialized"; resultRef: string; resultDigest: string }
	| { state: "failed"; failureCode: string };

export interface IndexClient {
	advanceIndex(taskId: string, generation: string, workerId: string, inputDigest: string): Promise<IndexProgress>;
	close(): void;
}

/**
 * Knowledge's IngestService for knowledge-project claims: the current
 * claimant asks Knowledge to write and verify the next batch of its index
 * entry. No text, vector, key or collection name crosses.
 */
export function connectIndex(
	address: string,
	timeoutMs: number,
	creds: ChannelCredentials = credentials.createInsecure(),
): IndexClient {
	const c = new knowledge.IngestServiceClient(address, creds);
	return {
		async advanceIndex(taskId, generation, workerId, inputDigest) {
			const req = knowledge.AdvanceIndexRequest.fromPartial({ taskId, generation, workerId, inputDigest });
			validated("anvilkit.knowledge.v1.AdvanceIndexRequest", knowledge.AdvanceIndexRequest.toJSON(req));
			const res = await promised<knowledge.AdvanceIndexResponse>(timeoutMs, (md, o, cb) =>
				c.advanceIndex(req, md, o, cb),
			);
			switch (res.state) {
				case knowledge.IndexState.INDEX_STATE_MATERIALIZED:
					return { state: "materialized", resultRef: res.resultRef, resultDigest: res.resultDigest };
				case knowledge.IndexState.INDEX_STATE_FAILED:
					return { state: "failed", failureCode: res.failureCode || "INDEX_FAILED" };
				case knowledge.IndexState.INDEX_STATE_RUNNING:
					return { state: "running", retryAfterMs: res.retryAfterMs };
				default:
					throw new OwnerUnavailable(`AdvanceIndex answered state ${res.state}`);
			}
		},
		close: () => c.close(),
	};
}

export interface ProjectionClient {
	advanceProjection(taskId: string, generation: string, workerId: string, inputDigest: string): Promise<IndexProgress>;
	close(): void;
}

/**
 * Knowledge's IngestService for memory-project claims (P17): the current
 * claimant asks Knowledge to apply the fact's current state to the task's
 * projection target and verify it. No content, vector, key or collection
 * name crosses; the progress has the index step's shape.
 */
export function connectProjection(
	address: string,
	timeoutMs: number,
	creds: ChannelCredentials = credentials.createInsecure(),
): ProjectionClient {
	const c = new knowledge.IngestServiceClient(address, creds);
	return {
		async advanceProjection(taskId, generation, workerId, inputDigest) {
			const req = knowledge.AdvanceProjectionRequest.fromPartial({ taskId, generation, workerId, inputDigest });
			validated("anvilkit.knowledge.v1.AdvanceProjectionRequest", knowledge.AdvanceProjectionRequest.toJSON(req));
			const res = await promised<knowledge.AdvanceProjectionResponse>(timeoutMs, (md, o, cb) =>
				c.advanceProjection(req, md, o, cb),
			);
			switch (res.state) {
				case knowledge.IndexState.INDEX_STATE_MATERIALIZED:
					return { state: "materialized", resultRef: res.resultRef, resultDigest: res.resultDigest };
				case knowledge.IndexState.INDEX_STATE_FAILED:
					return { state: "failed", failureCode: res.failureCode || "PROJECTION_FAILED" };
				case knowledge.IndexState.INDEX_STATE_RUNNING:
					return { state: "running", retryAfterMs: res.retryAfterMs };
				default:
					throw new OwnerUnavailable(`AdvanceProjection answered state ${res.state}`);
			}
		},
		close: () => c.close(),
	};
}
