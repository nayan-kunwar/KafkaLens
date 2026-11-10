# Changelog

## 0.2.2 - 2026-11-10

- Node.js 22.22 or newer is required. The Docker images, CI, and `.nvmrc` are pinned to 22.22.0, matching the Testcontainers engine requirement.
- Added a production deployment guide covering SASL/TLS, secrets, migration and retention jobs, metrics exposure, readiness, scaling, and upgrades.
- Fixed the Compose `kafka-lens` service to build the runtime Docker stage; the default last stage is the demo image and has no command.
- Added captured diagnosis examples for the slow-consumer, stalled-consumer, partition-hotspot, and producer-spike scenarios.
- The producer-spike demo now sends warm-up records and waits for a commit before the burst, so the rate gap is visible to `diagnose_consumer_group`.
- Refreshed the temporary `minimatch`/`glob` overrides and documented that upstream ranges still resolve vulnerable versions.
- Security and reliability audit remediation: `npm audit` is clean for production and development dependencies, Compose ports publish on loopback with configurable PostgreSQL/Redis passwords, and metrics bind to `127.0.0.1` by default.
- Correlation hardening: streamed body cap, redirects blocked, HTTPS-only sources, credential-free URLs, sanitized titles, and `untrusted: true` event markers.
- Read-only capability manifest, typed MCP tool registration, prompt escaping, and schema validation for resources.
- Cluster-scoped history and incidents, complete partition history beyond the response page, background history writes with `flushHistory()`, bounded topic concurrency, advisory-locked migrations and incident dedupe, and batched retention deletes.
- PostgreSQL idle-client error handling and connection/statement/query timeouts, tool-level deadlines, original-cause logging, and idempotent shutdown that flushes history and closes the MCP transport. Shutdown and the migrate/retain CLIs are unit-tested.
- Build and CI hardening: clean `dist`/`dist-demo`, demo build separated from the production image, source maps disabled in production, integration tests on pull requests, and a production dependency audit gate.

## 0.2.1

- One long-lived Kafka admin client, shared by topic, group, and lag calls.
- `get_readiness` reports Kafka, PostgreSQL, and Redis without failing the call. `health_check` stays process liveness.
- SQL migrations run with `npm run migrate`, not during server startup. `npm run retain` deletes old lag and incident rows.
- Each tool call adds `toolCallId` to the log lines emitted while it runs.
- Documented the read-only Kafka ACLs.
- Testcontainers coverage for `diagnose_consumer_group`: a stalled group, a missing offset, and PostgreSQL down.
- GitHub Actions runs `npm run check` on pull requests and the integration tests on pushes to `main`.

## 0.2.0

Diagnosis and the rest of the read-only investigation surface.

- Diagnosis engine with ranked findings, evidence categories, and configurable imbalance and stall windows.
- Partition imbalance, consumer progress, and throughput estimates.
- `diagnose_consumer_group` plus the focused analysis tools.
- Incident history and `find_similar_incidents`.
- Optional Redis metadata cache, Prometheus metrics, and GitHub or deployment correlation.
- Read-only tool allowlist. Destructive Kafka operations are rejected.
- MCP resources and investigation prompts.
- Docker Compose stack and demo scenarios: slow consumer, stalled consumer, partition hotspot, and producer spike.

## 0.1.0

Foundation through lag history: Kafka connectivity, topic inspection, consumer groups, per-partition lag, and PostgreSQL lag history.
