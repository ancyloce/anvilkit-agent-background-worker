// Bootstrap of anvilkit-agent-background-worker: `worker` runs the BullMQ
// Workers over the owners' queues, `relay` runs one owner's queue relay
// (DD-09 §3 lifecycle for both: the probe listener first, the clients
// probed before anything runs, readiness last; SIGTERM withdraws readiness,
// stops taking work, drains within bounds with a forced stop afterwards,
// records the drain and closes the clients, the probe listener last).

import { createServer, type Server } from "node:http";
import { collectDefaultMetrics, Registry } from "prom-client";
import { type Config, load, type Owner, owners, requireRelay, requireWorker } from "./config.js";
import { jsonLogger, type Logger } from "./log.js";
import { Metrics } from "./metrics.js";
import { connectIndex, connectIngest, connectOwner, connectProjection, type OwnerClient } from "./owner.js";
import { connection } from "./queue.js";
import { Relay } from "./relay/relay.js";
import { Telemetry } from "./telemetry.js";
import { createBoard } from "./worker/board.js";
import { handlersFor } from "./worker/handlers.js";
import { BackgroundWorker } from "./worker/worker.js";

export interface Started {
	stop(): Promise<void>;
	done: Promise<void>;
}

function healthServer(registry: Registry, isReady: () => boolean): Server {
	return createServer((req, res) => {
		if (req.url === "/healthz") return void res.writeHead(200).end();
		if (req.url === "/readyz") return void res.writeHead(isReady() ? 200 : 503).end();
		if (req.url === "/metrics")
			return void registry
				.metrics()
				.then((body) => res.writeHead(200, { "content-type": registry.contentType }).end(body));
		res.writeHead(404).end();
	});
}

function listen(server: Server, address: string): Promise<void> {
	const i = address.lastIndexOf(":");
	return new Promise<void>((resolve, reject) => {
		server.once("error", reject);
		server.listen(Number(address.slice(i + 1)), address.slice(0, i), () => resolve());
	});
}

function closeServer(server: Server): Promise<void> {
	return new Promise((r) => server.close(() => r()));
}

export async function startWorker(cfg: Config, log: Logger = jsonLogger()): Promise<Started> {
	requireWorker(cfg);
	const registry = new Registry();
	collectDefaultMetrics({ register: registry });
	const metrics = new Metrics(registry);
	metrics.configGeneration.set(1);
	let ready = false;
	const health = healthServer(registry, () => ready);
	const clients: Partial<Record<Owner, OwnerClient>> = {};
	for (const o of owners)
		if (cfg.owners[o].address) clients[o] = connectOwner(o, cfg.owners[o].address, cfg.worker.ownerTimeoutMs);
	const ingest = cfg.owners.knowledge.address
		? connectIngest(cfg.owners.knowledge.address, cfg.worker.ownerTimeoutMs)
		: undefined;
	const index = cfg.owners.knowledge.address
		? connectIndex(cfg.owners.knowledge.address, cfg.worker.ownerTimeoutMs)
		: undefined;
	const projection = cfg.owners.knowledge.address
		? connectProjection(cfg.owners.knowledge.address, cfg.worker.ownerTimeoutMs)
		: undefined;
	const conn = connection(cfg.queue.url);
	const telemetry = new Telemetry(cfg.telemetry, "anvilkit-agent-background-worker");
	const worker = new BackgroundWorker(
		cfg,
		clients,
		conn,
		metrics,
		log,
		handlersFor(ingest, index, projection),
		telemetry,
	);
	let board: Server | undefined;
	try {
		await listen(health, cfg.health.listen);
		await conn.ping();
		worker.start();
		if (cfg.bullBoard.enabled) {
			board = createBoard(worker.queueHandles, cfg.bullBoard.readOnly);
			await listen(board, cfg.bullBoard.listen);
			log.warn(
				cfg.bullBoard.readOnly
					? "Bull Board serving read-only"
					: "Bull Board serving with retries enabled; a retried delivery still needs the owner's admission",
				{ listen: cfg.bullBoard.listen },
			);
		}
		ready = true;
	} catch (err) {
		await worker.stop().catch(() => undefined);
		if (board) await closeServer(board);
		await closeServer(health);
		conn.disconnect();
		for (const c of Object.values(clients)) c.close();
		ingest?.close();
		index?.close();
		projection?.close();
		throw err;
	}
	let resolveDone!: () => void;
	const done = new Promise<void>((r) => {
		resolveDone = r;
	});
	let stopping: Promise<void> | undefined;
	const stop = () => {
		if (stopping) return stopping;
		stopping = (async () => {
			const begin = Date.now();
			ready = false;
			const forced = await worker.stop();
			if (board) await closeServer(board);
			conn.disconnect();
			for (const c of Object.values(clients)) c.close();
			ingest?.close();
			index?.close();
			projection?.close();
			metrics.drainSeconds.set((Date.now() - begin) / 1000);
			if (forced) metrics.forcedStop.set(1);
			log.info("background worker stopped", { drainSeconds: (Date.now() - begin) / 1000, forced });
			await telemetry.shutdown().catch((err) => log.error("span flush failed", { error: String(err) }));
			await closeServer(health);
			resolveDone();
		})();
		return stopping;
	};
	return { stop, done };
}

export async function startRelay(cfg: Config, log: Logger = jsonLogger()): Promise<Started> {
	requireRelay(cfg);
	const owner = cfg.relay.owner as Owner;
	const registry = new Registry();
	collectDefaultMetrics({ register: registry });
	const metrics = new Metrics(registry);
	metrics.configGeneration.set(1);
	let ready = false;
	const health = healthServer(registry, () => ready);
	const client = connectOwner(owner, cfg.owners[owner].address, cfg.worker.ownerTimeoutMs);
	const conn = connection(cfg.queue.url);
	const relay = new Relay(cfg, owner, client, conn, metrics, log);
	try {
		await listen(health, cfg.health.listen);
		await relay.start();
		ready = true;
	} catch (err) {
		await relay.stop().catch(() => undefined);
		await closeServer(health);
		conn.disconnect();
		client.close();
		throw err;
	}
	let resolveDone!: () => void;
	const done = new Promise<void>((r) => {
		resolveDone = r;
	});
	let stopping: Promise<void> | undefined;
	const stop = () => {
		if (stopping) return stopping;
		stopping = (async () => {
			const begin = Date.now();
			ready = false;
			const forced = await relay.stop();
			conn.disconnect();
			client.close();
			metrics.drainSeconds.set((Date.now() - begin) / 1000);
			if (forced) metrics.forcedStop.set(1);
			log.info("relay stopped", { owner, drainSeconds: (Date.now() - begin) / 1000, forced });
			await closeServer(health);
			resolveDone();
		})();
		return stopping;
	};
	return { stop, done };
}

const entry = process.argv[1] ?? "";
if (entry.endsWith("main.js") || entry.endsWith("main.ts")) {
	const mode = process.argv[2] ?? "worker";
	const log = jsonLogger();
	const run = mode === "relay" ? startRelay : mode === "worker" ? startWorker : null;
	if (!run) {
		process.stderr.write("usage: main.js worker|relay\n");
		process.exit(2);
	}
	run(load(), log)
		.then((s) => {
			process.once("SIGTERM", () => void s.stop());
			process.once("SIGINT", () => void s.stop());
			return s.done;
		})
		.then(() => process.exit(0))
		.catch((err) => {
			process.stderr.write(`${err instanceof Error ? err.message : String(err)}\n`);
			process.exit(1);
		});
}
