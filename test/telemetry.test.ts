import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace";
import { describe, expect, it } from "vitest";
import { Telemetry } from "../src/telemetry.js";

describe("telemetry", () => {
	it("records one consumer span per delivery with the queue and outcome only", async () => {
		const exporter = new InMemorySpanExporter();
		const t = new Telemetry({ otlpEndpoint: "", sampleRatio: 1 }, "test", new SimpleSpanProcessor({ exporter }));
		const end = t.delivery("anvilkit-knowledge-index");
		end("accepted");
		end("error"); // a second end is ignored
		await new Promise((resolve) => setImmediate(resolve));
		const spans = exporter.getFinishedSpans();
		expect(spans).toHaveLength(1);
		expect(spans[0]?.name).toBe("process anvilkit-knowledge-index");
		expect(spans[0]?.attributes).toEqual({
			"messaging.system": "bullmq",
			"messaging.destination.name": "anvilkit-knowledge-index",
			"anvilkit.delivery.outcome": "accepted",
		});
		await t.shutdown();
	});
});
