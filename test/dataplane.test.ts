// P0.6 data plane of the Background Worker: the configuration rules outside
// development (a rediss:// queue with an ACL user, NATS under tls or mtls
// with a credential, sslmode=verify-full for the relay database), the two
// NATS credential forms, and the client options every queue and NATS
// connection is built from, exercised against TLS doubles instead of Valkey
// or NATS containers: the queue client verifies the server and sends its ACL
// user; the relay's NATS client verifies the server, presents its
// certificate under mtls and signs the server's nonce with the credential.
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer as createTcpServer, type Server as TcpServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { createServer as createTlsServer, TLSSocket, type Server as TlsServer } from "node:tls";
import { nkeys } from "@nats-io/nats-core";
import { connect } from "@nats-io/transport-node";
import { afterAll, describe, expect, it } from "vitest";
import { type Config, loadFrom } from "../src/config.js";
import { connection, connectionOptions } from "../src/queue.js";
import { natsOptions } from "../src/relay/relay.js";
import { issue, newCA, spiffe, tempDir } from "./pki.js";

const dir = tempDir();
const user = nkeys.createUser();
const seed = new TextDecoder().decode(user.getSeed());
const seedFile = path.join(dir, "relay.nk");
writeFileSync(seedFile, `${seed}\n`);
const credsFile = path.join(dir, "relay.creds");
writeFileSync(
	credsFile,
	`-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJzdWIiOiJ1In0.c2ln\n------END NATS USER JWT------\n\n************************* IMPORTANT *************************\nNKEY Seed printed below can be used to sign and prove identity.\n\n-----BEGIN USER NKEY SEED-----\n${seed}\n------END USER NKEY SEED------\n`,
);

/** The verified data plane outside development (the identity files are placements the loader does not read). */
const secure: Record<string, string> = {
	ANVILKIT_BACKGROUND_WORKER_IDENTITY_CERT_FILE: "/etc/anvilkit/identity/tls.crt",
	ANVILKIT_BACKGROUND_WORKER_IDENTITY_KEY_FILE: "/etc/anvilkit/identity/tls.key",
	ANVILKIT_BACKGROUND_WORKER_IDENTITY_CA_FILE: "/etc/anvilkit/identity/ca.crt",
	ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "rediss://background-worker:queue-secret@valkey.anvilkit-data.svc:6379",
	ANVILKIT_BACKGROUND_WORKER_NATS_URL: "nats://nats.anvilkit-data.svc:4222",
	ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: seedFile,
	ANVILKIT_BACKGROUND_WORKER_RELAY_OWNER: "knowledge",
	ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL:
		"postgres://relay:db-secret@pg.anvilkit-data.svc:5432/anvilkit_knowledge?sslmode=verify-full",
};

function load(content: string, environ: Record<string, string>): Config {
	const file = path.join(mkdtempSync(path.join(tmpdir(), "bg-dataplane-")), "config.yaml");
	writeFileSync(file, content);
	return loadFrom(file, environ);
}

function refusal(content: string, environ: Record<string, string>): string {
	try {
		load(content, environ);
	} catch (err) {
		return (err as Error).message;
	}
	return "";
}

describe("data-plane configuration (P0.6)", () => {
	it("loads the verified data plane outside development", () => {
		const cfg = load("{}\n", secure);
		expect(cfg.development.enabled).toBe(false);
		expect(cfg.nats.tls).toEqual({ mode: "tls", caFile: "", certFile: "", keyFile: "", serverName: "" });
		expect(cfg.nats.credential?.kind).toBe("nkey");
		expect(cfg.queue.tls).toEqual({ caFile: "", serverName: "" });
	});

	it("refuses plaintext and unauthenticated settings outside development without echoing a secret", () => {
		const caFile = path.join(dir, "missing-ca.crt");
		const cases: [string, Record<string, string>, RegExp][] = [
			[
				"{}\n",
				{ ...secure, ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "redis://background-worker:queue-secret@valkey:6379" },
				/queue.url must be rediss:\/\/<user>:<password>@host:port outside development/,
			],
			[
				"{}\n",
				{ ...secure, ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "rediss://:queue-secret@valkey:6379" },
				/queue.url must be rediss:\/\/<user>:<password>@host:port/,
			],
			[
				"{}\n",
				{ ...secure, ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "rediss://background-worker@valkey:6379" },
				/queue.url must be rediss:\/\/<user>:<password>@host:port/,
			],
			["{}\n", { ...secure, ANVILKIT_BACKGROUND_WORKER_QUEUE_TLS_CA_FILE: caFile }, /queue.tls.ca_file: ENOENT/],
			[
				"nats:\n  tls:\n    mode: development\n",
				secure,
				/nats.tls.mode development \(plaintext\) requires development.enabled: true/,
			],
			[
				"{}\n",
				{ ...secure, ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: "" },
				/nats.creds_file is required outside development \(ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE\)/,
			],
			[
				"{}\n",
				{ ...secure, ANVILKIT_BACKGROUND_WORKER_NATS_TLS_MODE: "mtls" },
				/nats.tls.ca_file, cert_file and key_file are required under nats.tls.mode mtls/,
			],
			[
				"{}\n",
				{ ...secure, ANVILKIT_BACKGROUND_WORKER_NATS_TLS_CERT_FILE: "/c" },
				/nats.tls.cert_file and key_file apply only under nats.tls.mode mtls/,
			],
			["nats:\n  tls:\n    mode: plaintext\n", secure, /nats.tls.mode must be development, tls or mtls/],
			[
				"{}\n",
				{
					...secure,
					ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL: "postgres://relay:db-secret@pg:5432/k?sslmode=require",
				},
				/relay.database_url: sslmode must be verify-full outside development \(got "require"\)/,
			],
			[
				"{}\n",
				{ ...secure, ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL: "postgres://relay:db-secret@pg:5432/k" },
				/relay.database_url: sslmode must be verify-full outside development \(got none\)/,
			],
			[
				"{}\n",
				{
					...secure,
					ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL:
						"postgres://relay:db-secret@10.0.0.9:5432/k?sslmode=verify-full",
				},
				/relay.database_url: the host must be a DNS name outside development/,
			],
			[`nats:\n  creds_file: ${seedFile}\n`, secure, /nats.creds_file is a secret or a placement/],
		];
		for (const [content, environ, want] of cases) {
			const message = refusal(content, environ);
			expect(message, String(want)).toMatch(want);
			for (const secret of ["queue-secret", "db-secret", seed]) expect(message).not.toContain(secret);
		}
	});

	it("admits the plaintext lane only under the guard; NATS is validated only while placed", () => {
		const lane = {
			...secure,
			ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "redis://127.0.0.1:6379",
			ANVILKIT_BACKGROUND_WORKER_NATS_URL: "nats://127.0.0.1:4222",
			ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: "",
			ANVILKIT_BACKGROUND_WORKER_RELAY_DATABASE_URL: "postgres://relay:pw@127.0.0.1:5432/k?sslmode=disable",
		};
		const cfg = load("development:\n  enabled: true\nnats:\n  tls:\n    mode: development\n", lane);
		expect(cfg.nats.tls.mode).toBe("development");
		expect(cfg.nats.credential).toBeUndefined();
		expect(natsOptions(cfg, "lane")).toEqual({ servers: "nats://127.0.0.1:4222", name: "lane" });
		expect(connectionOptions(cfg.queue).tls).toBeUndefined();
		// The guard alone downgrades nothing: NATS stays tls by default.
		expect(load("development:\n  enabled: true\n", lane).nats.tls.mode).toBe("tls");
		// A worker without NATS needs no NATS credential.
		const {
			ANVILKIT_BACKGROUND_WORKER_NATS_URL: _u,
			ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: _c,
			...worker
		} = secure;
		expect(load("{}\n", worker).nats.credential).toBeUndefined();
		expect(refusal("{}\n", { ...secure, ANVILKIT_BACKGROUND_WORKER_QUEUE_URL: "redis://127.0.0.1:6379" })).toMatch(
			/queue.url must be rediss/,
		);
		expect(
			refusal("development:\n  enabled: true\nqueue:\n  tls:\n    server_name: valkey\n", {
				...lane,
				ANVILKIT_BACKGROUND_WORKER_NATS_URL: "",
			}),
		).toMatch(/queue.tls.ca_file and queue.tls.server_name apply only to a rediss:\/\/ queue.url/);
	});

	it("reads a NATS user .creds file or a bare NKey user seed and refuses anything else without echoing it", () => {
		const creds = load("{}\n", { ...secure, ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: credsFile });
		expect(creds.nats.credential?.kind).toBe("creds");
		const nonce = "nonce-from-the-server";
		const signed = (cfg: Config) => {
			const auth = natsOptions(cfg, "relay").authenticator;
			if (typeof auth !== "function") throw new Error("no single authenticator");
			return auth(nonce) as { jwt?: string; nkey: string; sig: string };
		};
		const viaCreds = signed(creds);
		expect(viaCreds.jwt).toBe("eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJzdWIiOiJ1In0.c2ln");
		const viaSeed = signed(load("{}\n", secure));
		for (const a of [viaCreds, viaSeed]) {
			expect(a.nkey).toBe(user.getPublicKey());
			expect(nkeys.fromPublic(a.nkey).verify(new TextEncoder().encode(nonce), nkeys.decode(a.sig))).toBe(true);
		}
		const garbage = path.join(dir, "garbage");
		writeFileSync(garbage, "password=hunter2-not-a-credential\n");
		const shortSeed = path.join(dir, "short.nk");
		writeFileSync(shortSeed, "SUAAAAAAAAAA\n");
		const accountSeed = path.join(dir, "account.nk");
		writeFileSync(accountSeed, new TextDecoder().decode(nkeys.createAccount().getSeed()));
		for (const file of [garbage, shortSeed, accountSeed]) {
			const message = refusal("{}\n", { ...secure, ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: file });
			expect(message).toMatch(/nats.creds_file: neither a NATS user credentials file nor an NKey user seed/);
			expect(message).not.toContain("hunter2");
		}
		expect(
			refusal("{}\n", { ...secure, ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: path.join(dir, "missing") }),
		).toMatch(/nats.creds_file: cannot read the file/);
	});
});

describe("data-plane clients against TLS doubles (P0.6)", () => {
	const ca = newCA("anvilkit-dev-data-ca");
	const other = newCA("someone-else");
	const caFile = path.join(dir, "data-ca.crt");
	const otherFile = path.join(dir, "other-ca.crt");
	writeFileSync(caFile, ca.pem);
	writeFileSync(otherFile, other.pem);
	const serverLeaf = issue(ca, "data", [], ["valkey.anvilkit-data.svc", "nats.anvilkit-data.svc"]);
	const relayURI = spiffe("anvilkit.local", "anvilkit-apps", "anvilkit-agent-background-worker");
	const relayLeaf = issue(ca, "relay", [relayURI]);
	const relayFiles = { cert: path.join(dir, "relay.crt"), key: path.join(dir, "relay.key") };
	writeFileSync(relayFiles.cert, relayLeaf.certPem);
	writeFileSync(relayFiles.key, relayLeaf.keyPem);
	const closers: (() => void)[] = [];
	afterAll(() => {
		for (const c of closers) c();
	});

	/** A Valkey double over TLS: records the AUTH user and answers every command. */
	async function valkey(): Promise<{ port: number; handshakes: number; users: string[]; commands: string[] }> {
		const state = { port: 0, handshakes: 0, users: [] as string[], commands: [] as string[] };
		const server: TlsServer = createTlsServer({ cert: serverLeaf.certPem, key: serverLeaf.keyPem }, (socket) => {
			state.handshakes++;
			socket.on("error", () => undefined);
			socket.on("data", (chunk: Buffer) => {
				const text = chunk.toString("utf8");
				for (const m of text.matchAll(/\*\d+\r\n\$\d+\r\n(\w+)\r\n((?:\$\d+\r\n[^\r]*\r\n)*)/g)) {
					const command = (m[1] ?? "").toUpperCase();
					const args = [...(m[2] ?? "").matchAll(/\$\d+\r\n([^\r]*)\r\n/g)].map((a) => a[1] ?? "");
					// ioredis authenticates with AUTH <user> <password> or HELLO <protover> AUTH <user> <password>.
					if (command === "AUTH") state.users.push(args.length === 2 ? (args[0] ?? "") : "");
					const auth = args.findIndex((a) => a.toUpperCase() === "AUTH");
					if (command === "HELLO" && auth >= 0) state.users.push(args[auth + 1] ?? "");
					state.commands.push(command);
					socket.write(command === "PING" ? "+PONG\r\n" : "+OK\r\n");
				}
			});
		});
		await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
		closers.push(() => server.close());
		state.port = (server.address() as { port: number }).port;
		return state;
	}

	function queueOf(port: number, tls: { caFile: string; serverName: string }): Config["queue"] {
		return { url: `rediss://background-worker:queue-secret@127.0.0.1:${port}`, prefix: "anvilkit", tls };
	}

	async function ping(queue: Config["queue"]): Promise<string> {
		const conn = connection(queue);
		try {
			return await new Promise<string>((resolve) => {
				conn.once("error", (err: Error) => resolve(err.message));
				conn.ping().then(resolve, (err: Error) => resolve(err.message));
			});
		} finally {
			conn.disconnect();
		}
	}

	it("the queue connection verifies the server against queue.tls and authenticates its ACL user", async () => {
		const v = await valkey();
		expect(await ping(queueOf(v.port, { caFile, serverName: "valkey.anvilkit-data.svc" }))).toBe("PONG");
		expect(v.users, v.commands.join(",")).toEqual(["background-worker"]);
		expect(await ping(queueOf(v.port, { caFile: otherFile, serverName: "valkey.anvilkit-data.svc" }))).toMatch(
			/certificate|self.signed|unable to verify|issuer/i,
		);
		expect(await ping(queueOf(v.port, { caFile, serverName: "cache.anvilkit-data.svc" }))).toMatch(/altnames/);
		expect(v.handshakes).toBe(1);
	});

	/** A NATS double: INFO with tls_required and a nonce, then TLS (client certificates required when ca is given) and PONG. */
	async function nats(clientCa?: Buffer): Promise<{
		port: number;
		connects: Record<string, unknown>[];
		peers: string[];
		server: TcpServer;
	}> {
		const state = { port: 0, connects: [] as Record<string, unknown>[], peers: [] as string[] };
		const server = createTcpServer((raw) => {
			raw.on("error", () => undefined);
			raw.write(
				`INFO ${JSON.stringify({ server_id: "double", server_name: "double", version: "2.12.4", proto: 1, max_payload: 1048576, tls_required: true, nonce: "server-nonce" })}\r\n`,
			);
			const tls = new TLSSocket(raw, {
				isServer: true,
				cert: serverLeaf.certPem,
				key: serverLeaf.keyPem,
				...(clientCa ? { requestCert: true, rejectUnauthorized: true, ca: clientCa } : {}),
			});
			tls.on("error", () => undefined);
			tls.once("secure", () => state.peers.push(tls.getPeerCertificate()?.subjectaltname ?? ""));
			let buffered = "";
			tls.on("data", (chunk: Buffer) => {
				buffered += chunk.toString("utf8");
				let i = buffered.indexOf("\r\n");
				while (i >= 0) {
					const line = buffered.slice(0, i);
					buffered = buffered.slice(i + 2);
					if (line.startsWith("CONNECT ")) state.connects.push(JSON.parse(line.slice(8)));
					if (line === "PING") tls.write("PONG\r\n");
					i = buffered.indexOf("\r\n");
				}
			});
		});
		await new Promise<void>((r) => server.listen(0, "127.0.0.1", () => r()));
		closers.push(() => server.close());
		return { ...state, port: (server.address() as { port: number }).port, server };
	}

	async function relayConnect(cfg: Config): Promise<string> {
		try {
			const nc = await connect({ ...natsOptions(cfg, "relay"), reconnect: false, timeout: 5000 });
			await nc.close();
			return "connected";
		} catch (err) {
			return `${(err as Error).message} ${String((err as { cause?: Error }).cause?.message ?? "")}`;
		}
	}

	function relayConfig(port: number, env: Record<string, string>): Config {
		return load("{}\n", { ...secure, ANVILKIT_BACKGROUND_WORKER_NATS_URL: `nats://127.0.0.1:${port}`, ...env });
	}

	it("the relay's NATS connection requires verified TLS and signs the server nonce with the credential", async () => {
		const n = await nats();
		const tls = {
			ANVILKIT_BACKGROUND_WORKER_NATS_TLS_CA_FILE: caFile,
			ANVILKIT_BACKGROUND_WORKER_NATS_TLS_SERVER_NAME: "nats.anvilkit-data.svc",
		};
		expect(
			await relayConnect(relayConfig(n.port, { ...tls, ANVILKIT_BACKGROUND_WORKER_NATS_CREDS_FILE: credsFile })),
		).toBe("connected");
		const sent = n.connects[0] as { jwt?: string; nkey?: string; sig?: string };
		expect(sent.jwt).toMatch(/^eyJ/);
		expect(sent.nkey).toBe(user.getPublicKey());
		expect(
			nkeys
				.fromPublic(sent.nkey as string)
				.verify(new TextEncoder().encode("server-nonce"), nkeys.decode(sent.sig as string)),
		).toBe(true);
		expect(
			await relayConnect(relayConfig(n.port, { ...tls, ANVILKIT_BACKGROUND_WORKER_NATS_TLS_CA_FILE: otherFile })),
		).toMatch(/certificate|self.signed|unable to verify|issuer/i);
		expect(
			await relayConnect(relayConfig(n.port, { ...tls, ANVILKIT_BACKGROUND_WORKER_NATS_TLS_SERVER_NAME: "other.svc" })),
		).toMatch(/altnames/);
		expect(n.connects).toHaveLength(1);
	});

	it("presents the relay certificate under mtls", async () => {
		const n = await nats(ca.pem);
		const cfg = relayConfig(n.port, {
			ANVILKIT_BACKGROUND_WORKER_NATS_TLS_MODE: "mtls",
			ANVILKIT_BACKGROUND_WORKER_NATS_TLS_CA_FILE: caFile,
			ANVILKIT_BACKGROUND_WORKER_NATS_TLS_CERT_FILE: relayFiles.cert,
			ANVILKIT_BACKGROUND_WORKER_NATS_TLS_KEY_FILE: relayFiles.key,
			ANVILKIT_BACKGROUND_WORKER_NATS_TLS_SERVER_NAME: "nats.anvilkit-data.svc",
		});
		expect(await relayConnect(cfg)).toBe("connected");
		expect(n.peers[0]).toContain(`URI:${relayURI}`);
		expect(n.connects[0]).toMatchObject({ nkey: user.getPublicKey() });
	});
});
