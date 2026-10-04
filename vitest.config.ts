import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["test/**/*.test.ts"],
		testTimeout: 180_000,
		hookTimeout: 180_000,
		// Disposable PostgreSQL, Valkey and NATS containers per file; the
		// scenarios bind loopback listeners.
		fileParallelism: false,
		server: {
			deps: {
				inline: [/@anvilkit\/generated-clients/],
			},
		},
	},
});
