// The Background Worker's spans (security.md "data classification, logging
// and deletion"): one consumer span per delivery, named after its queue,
// with the queue and the delivery's controlled outcome only. A task
// identifier, an input, a result reference or an owner response never
// becomes a span attribute. Spans go over OTLP/HTTP to the collector when an
// endpoint is placed and nowhere otherwise.
import { SpanKind, SpanStatusCode, type Tracer, trace } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
	BatchSpanProcessor,
	ParentBasedSampler,
	type SpanProcessor,
	TraceIdRatioBasedSampler,
	TracerProvider,
} from "@opentelemetry/sdk-trace";

/** Observes one delivery: returns the function that records its outcome. */
export interface DeliveryObserver {
	delivery(queue: string): (outcome: string) => void;
}

export class Telemetry implements DeliveryObserver {
	private readonly tracer: Tracer;
	private readonly provider: TracerProvider | undefined;

	constructor(cfg: { otlpEndpoint: string; sampleRatio: number }, service: string, processor?: SpanProcessor) {
		const spans =
			processor ??
			(cfg.otlpEndpoint
				? new BatchSpanProcessor({
						exporter: new OTLPTraceExporter({ url: `${cfg.otlpEndpoint.replace(/\/$/, "")}/v1/traces` }),
					})
				: undefined);
		if (spans) {
			this.provider = new TracerProvider({
				resource: resourceFromAttributes({ "service.name": service }),
				sampler: new ParentBasedSampler({ root: new TraceIdRatioBasedSampler(cfg.sampleRatio) }),
				spanProcessors: [spans],
			});
			this.tracer = this.provider.getTracer(service);
		} else {
			this.tracer = trace.getTracer(service);
		}
	}

	delivery(queue: string): (outcome: string) => void {
		const span = this.tracer.startSpan(`process ${queue}`, {
			kind: SpanKind.CONSUMER,
			attributes: { "messaging.system": "bullmq", "messaging.destination.name": queue },
		});
		let ended = false;
		return (outcome: string) => {
			if (ended) return;
			ended = true;
			span.setAttribute("anvilkit.delivery.outcome", outcome);
			if (outcome === "error") span.setStatus({ code: SpanStatusCode.ERROR });
			span.end();
		};
	}

	async shutdown(): Promise<void> {
		await this.provider?.shutdown();
	}
}
