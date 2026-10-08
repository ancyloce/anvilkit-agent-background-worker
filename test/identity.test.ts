// P0.1 owner transport of the Background Worker: the owner clients present
// the watched workload certificate and verify the owner by server name;
// rotation (leaf swap, CA transition and retirement, invalid updates) and
// the configuration guard.
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { BackgroundTaskServiceService } from "@anvilkit/generated-clients/proto/anvilkit/knowledge/v1/knowledge";
import { Server, ServerCredentials, status } from "@grpc/grpc-js";
import { describe, expect, it } from "vitest";
import { loadFrom } from "../src/config.js";
import { IdentityWatcher, parseMaterial } from "../src/identity.js";
import { connectOwner, OwnerTransports, OwnerUnavailable } from "../src/owner.js";
import { type CA, files, issue, type Leaf, mount, newCA, spiffe, tempDir } from "./pki.js";

const workerURI = spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-background-worker");
const ownerURI = spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-knowledge");
const ownerLeaf = (ca: CA) => issue(ca, "anvilkit-agent-knowledge", [ownerURI], ["anvilkit-agent-knowledge"]);

/** A NOT_FOUND-answering owner under the given CA bundle and leaf; setSecureContext rotates it. */
async function owner(caPem: Buffer, leaf: Leaf): Promise<{ address: string; stop(): void }> {
	const server = new Server();
	server.addService(BackgroundTaskServiceService, {
		getTask: (_call: unknown, cb: (e: Error, r: null) => void) =>
			cb(Object.assign(new Error("NOT_FOUND"), { code: status.NOT_FOUND }), null),
		claimTask: () => {},
		heartbeatTask: () => {},
		submitTaskResult: () => {},
	});
	const port = await new Promise<number>((resolve, reject) =>
		server.bindAsync(
			"127.0.0.1:0",
			ServerCredentials.createSsl(caPem, [{ private_key: leaf.keyPem, cert_chain: leaf.certPem }], true),
			(err, p) => (err ? reject(err) : resolve(p)),
		),
	);
	return { address: `127.0.0.1:${port}`, stop: () => server.forceShutdown() };
}

function workerConfig(dir: string, mode = "mtls", development = false) {
	const cfgDir = mkdtempSync(path.join(tmpdir(), "bg-identity-"));
	const file = path.join(cfgDir, "config.yaml");
	writeFileSync(file, `development:\n  enabled: ${development}\nidentity:\n  mode: ${mode}\n`);
	const f = files(dir);
	return loadFrom(file, {
		ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "redis://127.0.0.1:1",
		ANVILKIT_BACKGROUND_WORKER_KNOWLEDGE_ADDRESS: "127.0.0.1:1",
		ANVILKIT_BACKGROUND_WORKER_IDENTITY_CERT_FILE: f.certFile,
		ANVILKIT_BACKGROUND_WORKER_IDENTITY_KEY_FILE: f.keyFile,
		ANVILKIT_BACKGROUND_WORKER_IDENTITY_CA_FILE: f.caFile,
	});
}

/** The outcome of one GetTask: "not_found" means the owner answered (the transport works). */
async function probe(address: string, t: OwnerTransports): Promise<string> {
	const c = connectOwner("knowledge", address, 2000, t.for("knowledge"));
	try {
		await c.get("missing");
		return "ok";
	} catch (err) {
		if (err instanceof OwnerUnavailable) return "unavailable";
		const code = (err as { code?: unknown }).code;
		return typeof code === "number" ? (status[code] ?? String(code)) : String(code ?? (err as Error).message);
	} finally {
		c.close();
	}
}

describe("owner transport identity", () => {
	it("presents the workload certificate, verifies the owner by server name and follows rotation", async () => {
		const ca1 = newCA("ca1");
		const ca2 = newCA("ca2");
		const dir = tempDir();
		mount(dir, issue(ca1, "anvilkit-agent-background-worker", [workerURI]), ca1.pem);
		const cfg = workerConfig(dir);
		cfg.identity.reloadIntervalMs = 50;
		const t = new OwnerTransports(cfg);
		try {
			const o1 = await owner(ca1.pem, ownerLeaf(ca1));
			expect(await probe(o1.address, t)).toBe("NOT_FOUND");
			// wrong server name: standard hostname verification refuses
			const wrong = new OwnerTransports({
				...cfg,
				owners: { ...cfg.owners, knowledge: { ...cfg.owners.knowledge, serverName: "anvilkit-agent-mcp" } },
			});
			expect(await probe(o1.address, wrong)).toBe("unavailable");
			wrong.close();
			// an owner of a CA the worker does not trust
			const foreign = await owner(ca1.pem, ownerLeaf(ca2));
			expect(await probe(foreign.address, t)).toBe("unavailable");
			foreign.stop();
			// client leaf replaced under the same CA
			mount(dir, issue(ca1, "anvilkit-agent-background-worker", [workerURI]), ca1.pem);
			await new Promise((r) => setTimeout(r, 150));
			expect(await probe(o1.address, t)).toBe("NOT_FOUND");
			// transition: the worker trusts both CAs, a ca2 owner is accepted
			mount(dir, issue(ca1, "anvilkit-agent-background-worker", [workerURI]), Buffer.concat([ca1.pem, ca2.pem]));
			await new Promise((r) => setTimeout(r, 150));
			const o2 = await owner(Buffer.concat([ca1.pem, ca2.pem]), ownerLeaf(ca2));
			expect(await probe(o2.address, t)).toBe("NOT_FOUND");
			expect(await probe(o1.address, t)).toBe("NOT_FOUND");
			// an invalid update (mismatched key) keeps the valid material
			const bad = issue(ca2, "anvilkit-agent-background-worker", [workerURI]);
			bad.keyPem = issue(ca2, "x", [workerURI]).keyPem;
			mount(dir, bad, ca2.pem);
			await new Promise((r) => setTimeout(r, 200));
			expect(await probe(o2.address, t)).toBe("NOT_FOUND");
			// ca1 retired on the worker: the ca1 owner is refused, ca2 keeps working
			mount(dir, issue(ca2, "anvilkit-agent-background-worker", [workerURI]), ca2.pem);
			await new Promise((r) => setTimeout(r, 200));
			expect(await probe(o1.address, t)).toBe("unavailable");
			expect(await probe(o2.address, t)).toBe("NOT_FOUND");
			o1.stop();
			o2.stop();
		} finally {
			t.close();
		}
	});

	it("validates material as a whole", () => {
		const ca = newCA("ca");
		const leaf = issue(ca, "x", [workerURI]);
		expect(() => parseMaterial(leaf.certPem, issue(ca, "y", []).keyPem, ca.pem)).toThrow(/key values mismatch/);
		expect(() => parseMaterial(leaf.certPem, leaf.keyPem, Buffer.from("x"))).toThrow(/no certificate/);
		const dir = tempDir();
		mount(dir, leaf, ca.pem);
		const w = new IdentityWatcher(files(dir), 50);
		expect(w.current().cas).toHaveLength(1);
		w.stop();
	});

	it("admits a plaintext owner transport only under the top-level guard", () => {
		const ca = newCA("ca");
		const dir = tempDir();
		mount(dir, issue(ca, "x", [workerURI]), ca.pem);
		expect(workerConfig(dir).identity.mode).toBe("mtls");
		expect(() => workerConfig(dir, "development")).toThrow(/requires development.enabled/);
		expect(workerConfig(dir, "development", true).identity.mode).toBe("development");
		expect(() => workerConfig(dir, "plaintext")).toThrow(/identity.mode must be/);
		const cfgDir = mkdtempSync(path.join(tmpdir(), "bg-identity-"));
		const file = path.join(cfgDir, "config.yaml");
		writeFileSync(file, "{}\n");
		expect(() => loadFrom(file, { ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "redis://127.0.0.1:1" })).toThrow(
			/identity.cert_file, key_file and ca_file are required/,
		);
		const f = files(dir);
		const relay = loadFrom(file, {
			ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "redis://127.0.0.1:1",
			ANVILKIT_BACKGROUND_WORKER_IDENTITY_MODE: "mtls",
			ANVILKIT_BACKGROUND_WORKER_IDENTITY_CERT_FILE: f.certFile,
			ANVILKIT_BACKGROUND_WORKER_IDENTITY_KEY_FILE: f.keyFile,
			ANVILKIT_BACKGROUND_WORKER_IDENTITY_CA_FILE: f.caFile,
			ANVILKIT_BACKGROUND_WORKER_OWNER_SERVER_NAME: "anvilkit-agent-mcp-fullname",
		});
		expect(relay.owners.mcp.serverName).toBe("anvilkit-agent-mcp-fullname");
		expect(relay.owners.knowledge.serverName).toBe("anvilkit-agent-mcp-fullname");
		expect(() =>
			loadFrom(file, {
				ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "redis://127.0.0.1:1",
				ANVILKIT_BACKGROUND_WORKER_DEVELOPMENT_ENABLED: "true",
			}),
		).toThrow(/not allowed overrides/);
	});
});
