// The event contract boundary of the relay: every NATS message is parsed
// strictly and validated against contracts/events/events.schema.json (the
// same document the contracts repository's checks and fixtures validate)
// before anything is read from it. The document comes from the contracts
// directory named by the environment or found beside this package in the
// parent checkout.
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseStrictJson } from "@anvilkit/generated-clients/validation/json";
import type { ValidateFunction } from "ajv";
import { Ajv2020 } from "ajv/dist/2020.js";

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

export function contractsDir(configured?: string): string {
	const env = configured || process.env.ANVILKIT_BACKGROUND_WORKER_CONTRACTS_DIR;
	if (env) return env;
	for (const candidate of [
		path.join(packageRoot, "contracts"),
		path.resolve(packageRoot, "..", "..", "..", "contracts"),
	]) {
		if (existsSync(path.join(candidate, "events", "events.schema.json"))) return candidate;
	}
	throw new Error(
		"contracts not found: set ANVILKIT_BACKGROUND_WORKER_CONTRACTS_DIR to a directory holding events/events.schema.json",
	);
}

export interface Envelope {
	eventId: string;
	eventType: string;
	schemaVersion: 1;
	producer: string;
	subject: string;
	tenantId: string;
	aggregateType: string;
	aggregateId: string;
	aggregateRevision: string;
	occurredAt: string;
	correlationId: string;
	payload?: Record<string, unknown>;
	payloadRef?: string;
}

export class ContractViolation extends Error {}

/** The compiled envelope validator of the events schema. */
export class EventContract {
	private readonly validate: ValidateFunction;

	constructor(dir?: string) {
		const file = path.join(contractsDir(dir), "events", "events.schema.json");
		const schema = JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
		// strictRequired is off: the envelope states "payloadRef or payload" as a
		// oneOf of required lists, which Ajv's strict mode cannot pair with the
		// properties declaration; every other strict check stays on.
		const ajv = new Ajv2020({ strict: true, strictRequired: false, allErrors: true, allowUnionTypes: true });
		ajv.addSchema(schema);
		const v = ajv.getSchema(`${schema.$id as string}#/$defs/envelope`);
		if (!v) throw new Error("events schema has no envelope definition");
		this.validate = v;
	}

	/** Strictly parses and validates one message; anything else is a violation. */
	parse(data: Uint8Array): Envelope {
		let raw: unknown;
		try {
			raw = parseStrictJson(Buffer.from(data).toString("utf8"));
		} catch (err) {
			throw new ContractViolation(`malformed envelope: ${err instanceof Error ? err.message : String(err)}`);
		}
		if (!this.validate(raw))
			throw new ContractViolation(
				`envelope: ${(this.validate.errors ?? []).map((e) => `${e.instancePath || "/"} ${e.message ?? ""}`).join("; ")}`,
			);
		return raw as Envelope;
	}
}
