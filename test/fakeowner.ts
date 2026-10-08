// A scripted owner (anvilkit.knowledge.v1.BackgroundTaskService over
// grpc-js) for the Worker tests — a TEST DOUBLE, never imported by src/: it
// serves mTLS under the lab PKI (client certificates required and verified
// against the lab CA), records every call and answers as the
// scenario says — one claimant per generation, heartbeats that can refuse,
// submissions that can be unavailable once. The real owners' rules are
// proven in their own repositories and by the parent's integration scenario.
import {
	BackgroundTask,
	type BackgroundTaskServiceServer,
	BackgroundTaskServiceService,
	type ClaimTaskRequest,
	type GetTaskRequest,
	type HeartbeatTaskRequest,
	type SubmitTaskResultRequest,
	TaskState,
} from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { Server, ServerCredentials, type ServiceError, status } from "@grpc/grpc-js";
import { pki } from "./harness.js";

export interface ScriptedTask {
	taskKind: string;
	inputDigest: string;
	input: Buffer;
	state: TaskState;
	workerId?: string;
	refuseHeartbeats?: boolean;
	unavailableSubmits?: number;
	unavailableClaims?: number;
	resultDigest?: string;
}

export interface Call {
	method: "claim" | "heartbeat" | "submit" | "get";
	req: Record<string, unknown>;
}

function refused(code: status, message: string): ServiceError {
	return Object.assign(new Error(message), { code, details: message, metadata: undefined }) as unknown as ServiceError;
}

export class FakeOwner {
	readonly tasks = new Map<string, ScriptedTask>();
	readonly calls: Call[] = [];
	private readonly server = new Server();
	address = "";

	private view(taskId: string, t: ScriptedTask): BackgroundTask {
		return BackgroundTask.fromPartial({
			taskId,
			generation: "1",
			taskKind: t.taskKind,
			inputDigest: t.inputDigest,
			state: t.state,
			workerId: t.workerId,
			attemptCount: "1",
		});
	}

	async start(): Promise<string> {
		const impl: BackgroundTaskServiceServer = {
			claimTask: (call, cb) => {
				const req = call.request as ClaimTaskRequest;
				this.calls.push({ method: "claim", req: { ...req } });
				const t = this.tasks.get(req.taskId);
				if (!t) return cb(refused(status.NOT_FOUND, "NOT_FOUND"), null);
				if (t.unavailableClaims && t.unavailableClaims > 0) {
					t.unavailableClaims--;
					return cb(refused(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE"), null);
				}
				if (t.state !== TaskState.TASK_STATE_PENDING)
					return cb(refused(status.FAILED_PRECONDITION, `STALE_EXECUTION: state ${TaskState[t.state]}`), null);
				t.state = TaskState.TASK_STATE_LEASED;
				t.workerId = req.workerId;
				cb(null, { $type: "anvilkit.knowledge.v1.ClaimTaskResponse", task: this.view(req.taskId, t), input: t.input });
			},
			heartbeatTask: (call, cb) => {
				const req = call.request as HeartbeatTaskRequest;
				this.calls.push({ method: "heartbeat", req: { ...req } });
				const t = this.tasks.get(req.taskId);
				if (!t || t.refuseHeartbeats || t.workerId !== req.workerId || t.state !== TaskState.TASK_STATE_LEASED)
					return cb(refused(status.FAILED_PRECONDITION, "STALE_EXECUTION: not the current claimant"), null);
				cb(null, { $type: "anvilkit.knowledge.v1.HeartbeatTaskResponse", task: this.view(req.taskId, t) });
			},
			submitTaskResult: (call, cb) => {
				const req = call.request as SubmitTaskResultRequest;
				this.calls.push({ method: "submit", req: { ...req } });
				const t = this.tasks.get(req.taskId);
				if (!t) return cb(refused(status.NOT_FOUND, "NOT_FOUND"), null);
				if (t.unavailableSubmits && t.unavailableSubmits > 0) {
					t.unavailableSubmits--;
					// The owner accepted but the receipt is lost (the CAS happened).
					if (req.succeeded && t.state === TaskState.TASK_STATE_LEASED && t.workerId === req.workerId) {
						t.state = TaskState.TASK_STATE_ACCEPTED;
						t.resultDigest = req.resultDigest;
					}
					return cb(refused(status.UNAVAILABLE, "DEPENDENCY_UNAVAILABLE"), null);
				}
				if (t.state === TaskState.TASK_STATE_ACCEPTED) {
					const existing = t.workerId === req.workerId && t.resultDigest === req.resultDigest;
					if (!existing) return cb(refused(status.FAILED_PRECONDITION, "STALE_EXECUTION: already accepted"), null);
					return cb(null, {
						$type: "anvilkit.knowledge.v1.SubmitTaskResultResponse",
						task: this.view(req.taskId, t),
						accepted: true,
						existing: true,
					});
				}
				if (t.state !== TaskState.TASK_STATE_LEASED || t.workerId !== req.workerId)
					return cb(refused(status.FAILED_PRECONDITION, "STALE_EXECUTION: not the current claimant"), null);
				if (req.succeeded) {
					t.state = TaskState.TASK_STATE_ACCEPTED;
					t.resultDigest = req.resultDigest;
					return cb(null, {
						$type: "anvilkit.knowledge.v1.SubmitTaskResultResponse",
						task: this.view(req.taskId, t),
						accepted: true,
						existing: false,
					});
				}
				t.state = TaskState.TASK_STATE_DEAD;
				cb(null, {
					$type: "anvilkit.knowledge.v1.SubmitTaskResultResponse",
					task: this.view(req.taskId, t),
					accepted: false,
					existing: false,
				});
			},
			getTask: (call, cb) => {
				const req = call.request as GetTaskRequest;
				this.calls.push({ method: "get", req: { ...req } });
				const t = this.tasks.get(req.taskId);
				if (!t) return cb(refused(status.NOT_FOUND, "NOT_FOUND"), null);
				cb(null, { $type: "anvilkit.knowledge.v1.GetTaskResponse", task: this.view(req.taskId, t) });
			},
		};
		this.server.addService(BackgroundTaskServiceService, impl);
		const port = await new Promise<number>((resolve, reject) =>
			this.server.bindAsync(
				"127.0.0.1:0",
				ServerCredentials.createSsl(
					pki().ca.pem,
					[{ private_key: pki().ownerLeaf.keyPem, cert_chain: pki().ownerLeaf.certPem }],
					true,
				),
				(err, p) => (err ? reject(err) : resolve(p)),
			),
		);
		this.address = `127.0.0.1:${port}`;
		return this.address;
	}

	stop(): void {
		this.server.forceShutdown();
	}
}
