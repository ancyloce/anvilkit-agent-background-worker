// Bull Board (C05): read-only by default; every queue of the Worker is
// shown; a retry from a non-read-only board only re-enqueues a delivery,
// whose claim still has to pass the owner's admission.
import { createServer, type Server } from "node:http";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import type { Queue } from "bullmq";
import express from "express";

export function createBoard(queues: Queue[], readOnly: boolean): Server {
	const serverAdapter = new ExpressAdapter();
	serverAdapter.setBasePath("/");
	createBullBoard({ queues: queues.map((q) => new BullMQAdapter(q, { readOnlyMode: readOnly })), serverAdapter });
	const app = express();
	app.use("/", serverAdapter.getRouter());
	return createServer(app);
}
