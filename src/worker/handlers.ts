// The bounded handlers of this build, selected by the task kind the owner
// returned with the claim (never by the queue entry alone): the
// local-check fixture of P14 (a DEVELOPMENT_ONLY fixed computation, the
// SHA-256 of declared bytes, whose input can make it hold, fail or answer
// wrongly so the lane's fences are verifiable) and, since P15,
// knowledge-ingest (the parse step Knowledge's launcher runs for the
// claim) and, since P16, knowledge-project (the index step of one source
// revision in one generation) and, since P17, memory-project (the
// application of one fact's current state to one projection target).
// Catalog refresh arrives with P18; an unknown kind is a controlled failure,
// never a guess.
import { createHash } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { parseStrictJson } from "@anvilkit/generated-clients/validation/json";
import {
	type IndexClient,
	type IndexProgress,
	type IngestClient,
	OwnerRefused,
	OwnerUnavailable,
	type ProjectionClient,
} from "../owner.js";

/** The claim a handler runs under: identities and the input digest come from the owner, never the queue. */
export interface HandlerContext {
	taskId: string;
	generation: string;
	workerId: string;
	inputDigest: string;
	signal: AbortSignal;
}

export type HandlerResult =
	| { succeeded: true; resultRef: string; resultDigest: string }
	| { succeeded: false; failureCode: string };

export type Handler = (input: Buffer, ctx: HandlerContext) => Promise<HandlerResult>;

export class HandlerError extends Error {
	constructor(readonly code: string) {
		super(code);
	}
}

interface LocalCheckInput {
	schemaVersion: number;
	computation: string;
	bytes: string;
	holdMs?: number;
	fail?: boolean;
	wrongDigest?: boolean;
}

export const localCheck: Handler = async (input, ctx) => {
	let raw: unknown;
	try {
		raw = parseStrictJson(input.toString("utf8"));
	} catch {
		throw new HandlerError("INPUT_MALFORMED");
	}
	if (typeof raw !== "object" || raw === null) throw new HandlerError("INPUT_MALFORMED");
	const p = raw as Partial<LocalCheckInput>;
	if (p.schemaVersion !== 1 || p.computation !== "local-check-v1" || typeof p.bytes !== "string")
		throw new HandlerError("INPUT_MALFORMED");
	if (p.holdMs && p.holdMs > 0) {
		await sleep(p.holdMs, undefined, { signal: ctx.signal });
	}
	if (ctx.signal.aborted) throw new HandlerError("ABORTED");
	if (p.fail) return { succeeded: false, failureCode: "HANDLER_FAILED" };
	let digest = createHash("sha256").update(Buffer.from(p.bytes, "base64")).digest("hex");
	if (p.wrongDigest) digest = `${digest[0] === "0" ? "1" : "0"}${digest.slice(1)}`;
	return {
		succeeded: true,
		resultRef: `local-check:${ctx.taskId}:${ctx.generation}`,
		resultDigest: `sha256:${digest}`,
	};
};

/** Polling bounds of the parse step: the owner's suggestion within [min, max]. */
const pollMinMs = 200;
const pollMaxMs = 5000;

/**
 * knowledge-ingest: the claimant asks Knowledge to launch and observe the
 * parser Job of this claim until Knowledge has read and verified its
 * output, then reports exactly the reference and digest Knowledge
 * recorded. The Worker never sees the document, the result bytes, a
 * storage key or a URL; an unreachable owner is asked again within the
 * lease (the call is idempotent), a refusal ends the handler.
 */
export function knowledgeIngest(client: IngestClient): Handler {
	return async (input, ctx) => {
		let raw: unknown;
		try {
			raw = parseStrictJson(input.toString("utf8"));
		} catch {
			throw new HandlerError("INPUT_MALFORMED");
		}
		const p = raw as { schemaVersion?: unknown; computation?: unknown } | null;
		if (p?.schemaVersion !== 1 || p.computation !== "knowledge-ingest-v1") throw new HandlerError("INPUT_MALFORMED");
		for (;;) {
			if (ctx.signal.aborted) throw new HandlerError("ABORTED");
			let wait = pollMinMs;
			try {
				const a = await client.advance(ctx.taskId, ctx.generation, ctx.workerId, ctx.inputDigest);
				if (a.state === "completed") return { succeeded: true, resultRef: a.resultRef, resultDigest: a.resultDigest };
				if (a.state === "failed") return { succeeded: false, failureCode: a.failureCode };
				wait = Math.min(pollMaxMs, Math.max(pollMinMs, a.retryAfterMs));
			} catch (err) {
				if (err instanceof OwnerRefused) throw new HandlerError(err.reason);
				if (!(err instanceof OwnerUnavailable)) throw err;
				wait = pollMaxMs;
			}
			try {
				await sleep(wait, undefined, { signal: ctx.signal });
			} catch {
				throw new HandlerError("ABORTED");
			}
		}
	};
}

/**
 * knowledge-project: the claimant asks Knowledge to write the next batch of
 * its index entry until Knowledge has read every point back and recorded
 * the manifest, then reports exactly that reference and digest. The same
 * bounded poll and refusal rules as knowledge-ingest.
 */
export function knowledgeProject(client: IndexClient): Handler {
	return stepUntilMaterialized("knowledge-index-v1", (ctx) =>
		client.advanceIndex(ctx.taskId, ctx.generation, ctx.workerId, ctx.inputDigest),
	);
}

/**
 * memory-project: the claimant asks Knowledge to apply the fact's current
 * state to the task's target (the Store or one vector generation) until
 * Knowledge has verified it, then reports exactly that reference and
 * digest. The Worker never sees the fact's content. Same bounded poll and
 * refusal rules as knowledge-project.
 */
export function memoryProject(client: ProjectionClient): Handler {
	return stepUntilMaterialized("memory-project-v1", (ctx) =>
		client.advanceProjection(ctx.taskId, ctx.generation, ctx.workerId, ctx.inputDigest),
	);
}

function stepUntilMaterialized(computation: string, step: (ctx: HandlerContext) => Promise<IndexProgress>): Handler {
	return async (input, ctx) => {
		let raw: unknown;
		try {
			raw = parseStrictJson(input.toString("utf8"));
		} catch {
			throw new HandlerError("INPUT_MALFORMED");
		}
		const p = raw as { schemaVersion?: unknown; computation?: unknown } | null;
		if (p?.schemaVersion !== 1 || p.computation !== computation) throw new HandlerError("INPUT_MALFORMED");
		for (;;) {
			if (ctx.signal.aborted) throw new HandlerError("ABORTED");
			let wait = 0;
			try {
				const a = await step(ctx);
				if (a.state === "materialized")
					return { succeeded: true, resultRef: a.resultRef, resultDigest: a.resultDigest };
				if (a.state === "failed") return { succeeded: false, failureCode: a.failureCode };
				// A written batch answers 0: the next batch follows at once.
				wait = Math.min(pollMaxMs, a.retryAfterMs);
			} catch (err) {
				if (err instanceof OwnerRefused) throw new HandlerError(err.reason);
				if (!(err instanceof OwnerUnavailable)) throw err;
				wait = pollMaxMs;
			}
			if (wait > 0)
				try {
					await sleep(wait, undefined, { signal: ctx.signal });
				} catch {
					throw new HandlerError("ABORTED");
				}
		}
	};
}

/** The handlers of this build; the Knowledge kinds exist when Knowledge's IngestService is reachable. */
export function handlersFor(
	ingest?: IngestClient,
	index?: IndexClient,
	projection?: ProjectionClient,
): Record<string, Handler> {
	const out: Record<string, Handler> = { "local-check": localCheck };
	if (ingest) out["knowledge-ingest"] = knowledgeIngest(ingest);
	if (index) out["knowledge-project"] = knowledgeProject(index);
	if (projection) out["memory-project"] = memoryProject(projection);
	return out;
}

export const handlers: Record<string, Handler> = handlersFor();
