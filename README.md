# anvilkit-agent-background-worker

The Background Worker of the AnvilKit Agent platform (architecture V4.0, [DD-09](https://github.com/ancyloce/anvilkit-services/blob/main/docs/architecture/platform.md#async-config), delivery.md P14). TypeScript (Node.js 24), BullMQ 6 on the queue Valkey, grpc-js clients of the owners' `BackgroundTaskService`, `@nats-io` JetStream for the relay, Bull Board.

One package, two entries (`node dist/main.js worker|relay`):

- **worker** — BullMQ Workers on each configured owner's queues (`knowledge-ingest`, `knowledge-project`, `memory-project`, `mcp-catalog-refresh`, and the DEVELOPMENT_ONLY `local-check` lane) under the `anvilkit:<owner>` prefix. A queue entry carries identity, generation, digest and trace references only; every step goes through the owner: `ClaimTask` (one claimant per generation, one claim per worker identity — the identity is `<instance>/<job>/<delivery>`), `HeartbeatTask` while the bounded handler runs (a refused heartbeat aborts the handler and nothing is submitted), `SubmitTaskResult` (the owner's CAS decides; a lost receipt is answered by resubmitting the identical result within a bound, then by `GetTask`). Unknown handlers, oversized inputs, timeouts and handler failures are submitted failures. Duplicate and stalled deliveries reach the owner as claims it refuses. No business database, no NATS.
- **relay** — the owner queue relay, run beside each owner (the owner's chart) with the owner's relay role: the fixed durable pull consumer of `anvilkit.<owner>.background.requested` (explicit ACK, bounded ack wait and deliveries), every message validated against `contracts/events/events.schema.json`, checked against the durable request (tenant, generation, kind/digest, state) and recorded in the owner's `inbox` with its outcome before the ACK (a redelivery returns the recorded outcome; a revision gap is repaired through the owner's `GetTask`; invalid or foreign messages are terminated without touching facts), then the queue entry added under the task generation's job id. The reconcile loop rebuilds entries for due durable requests that lost theirs (Sentinel write loss, a crash between commit and add) and never re-admits a terminal or superseded generation.

Bull Board is off by default and read-only when on; a retry from a non-read-only board only re-enqueues a delivery whose claim still passes the owner's admission.

## Configuration

`config.yaml` holds the reviewed bounds; placements and secrets come only from `ANVILKIT_BACKGROUND_WORKER_{HEALTH_LISTEN,QUEUE_URL[_FILE],NATS_URL,KNOWLEDGE_ADDRESS,MCP_ADDRESS,CONTRACTS_DIR,BULL_BOARD_LISTEN,RELAY_OWNER,RELAY_DATABASE_URL[_FILE]}`; any other variable or unknown key rejects the candidate. `/healthz`, `/readyz` and `/metrics` (queue waiting/failed/delayed/lag, stalls, lease losses, handler timeouts, submit retries, inbox outcomes/duplicates/gaps, reconstructed entries, drain time) answer on the health listener.

## Checks

`pnpm install --frozen-lockfile --ignore-scripts && pnpm run check-types && pnpm run lint && pnpm run build && pnpm test` (Vitest; Docker-backed PostgreSQL 17, Valkey 9 and NATS 2.12 through Testcontainers; the `anvilkit_knowledge` migrations are found beside this package in the parent checkout or through `ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR`; the owner of the worker tests is a scripted gRPC double — the real owners are exercised by the parent's integration scenario), `docker build --build-context contracts=<contracts checkout> .`, `helm lint deploy/chart`.

## Repository

This package is the repository `anvilkit-agent-background-worker`, mounted in the parent `anvilkit-services` as the submodule `services/agent/background-worker`. Checked out on its own (as in CI), its checks take two inputs from outside it: the `anvilkit_knowledge` migrations through `ANVILKIT_KNOWLEDGE_MIGRATIONS_DIR` (they live in the parent's `jobs/migration/internal/migrate/sql/knowledge` until Knowledge owns them), and a checkout of the contracts repository at the commit `pnpm-lock.yaml` pins (`063f91f`, tag `go/v0.1.4`) through `ANVILKIT_BACKGROUND_WORKER_CONTRACTS_DIR` and as the image's `contracts` build context. Inside the parent checkout both are found without the variables.
