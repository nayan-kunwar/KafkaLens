# Changelog

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
