# Production deployment

`kafka-lens` is a stdio MCP server. The MCP client starts the process and exchanges JSON-RPC over stdin/stdout. There is no inbound HTTP API; the only optional listener is the Prometheus metrics endpoint. The server is read-only.

For the Kafka permissions it needs, see [operations.md](operations.md#kafka-acls). This guide covers the deployment-specific pieces: image, configuration, secrets, migrations, retention, metrics, and shutdown.

## Deployment shape

```text
MCP client ──stdio──> kafka-lens process ──> Kafka (read-only)
                                      ├──> PostgreSQL (history, incidents)
                                      ├──> Redis (metadata cache, optional)
                                      └──> metrics listener (optional)
```

Run one process per MCP client. Processes share PostgreSQL and Redis safely; there is no leader election and no local durable state.

The image runs as the unprivileged `node` user. It contains the compiled server and production dependencies only.

## Image

Build the production target:

```bash
docker build --target runtime -t kafka-lens:0.2.2 .
```

Pin the image by digest in production. The runtime stage is `node:22.22.0-alpine`; Node.js 22.22 or newer is required.

Without Docker:

```bash
npm ci --omit=dev
npm run build
node dist/index.js
```

`dist/persistence/migrate-cli.js` and `dist/persistence/retain-cli.js` are available in the image for jobs. The entrypoint applies migrations before starting the server and continues if they fail. For production, prefer a dedicated migration job that must succeed before the new server version is rolled out. The entrypoint remains a safe fallback because migrations are idempotent and advisory-locked.

## Required configuration

Set `NODE_ENV=production`. Configuration is validated at startup; invalid values fail with `CONFIG_INVALID`, and messages contain field names only, never values.

| Variable                                                       | Production guidance                                                                      |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `KAFKA_BROKERS`                                                | Required. Explicit broker list; the localhost default is rejected in production.         |
| `POSTGRES_URL`                                                 | Required. Explicit connection string; the localhost default is rejected in production.   |
| `REDIS_URL`                                                    | Optional. Use `rediss://` for TLS. Cache failures degrade, they do not stop Kafka reads. |
| `KAFKA_SECURITY_PROTOCOL`                                      | `SSL` or `SASL_SSL` for secured clusters.                                                |
| `KAFKA_SASL_MECHANISM`                                         | `plain`, `scram-sha-256`, or `scram-sha-512`.                                            |
| `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD`                  | Required when the protocol uses SASL. Never log or commit them.                          |
| `KAFKA_SSL_REJECT_UNAUTHORIZED`                                | Keep `true`. Mount a private CA with `NODE_EXTRA_CA_CERTS` if the broker uses one.       |
| `POSTGRES_*_TIMEOUT_MS`                                        | Keep bounded. Defaults: 5s connect, 10s statement, 15s query.                            |
| `STALL_WINDOW_SECONDS`                                         | Progress window. Keep it longer than a normal rebalance.                                 |
| `PARTITION_IMBALANCE_RATIO`                                    | Max/min lag ratio that counts as skew. Tune to your workload.                            |
| `TOOL_TIMEOUT_MS`                                              | Per-call deadline. Default 60s. The underlying Kafka request cannot be cancelled.        |
| `METRICS_ENABLED` / `METRICS_HOST` / `METRICS_PORT`            | See [Metrics](#metrics).                                                                 |
| `LAG_RETENTION_DAYS` / `INCIDENT_RETENTION_DAYS`               | Retention used by `npm run retain`. `0` disables that table.                             |
| `GITHUB_REPOSITORY` / `GITHUB_TOKEN` / `DEPLOYMENT_SOURCE_URL` | Optional change correlation. Enable only when needed.                                    |

See [`.env.example`](../.env.example) for the full list and defaults.

### SASL/TLS example

```bash
NODE_ENV=production
KAFKA_BROKERS=broker-1.example.com:9093,broker-2.example.com:9093
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=scram-sha-512
KAFKA_SASL_USERNAME=kafka-lens
KAFKA_SASL_PASSWORD=<from-secret-store>
KAFKA_SSL_REJECT_UNAUTHORIZED=true
NODE_EXTRA_CA_CERTS=/etc/kafka-lens/ca.pem
POSTGRES_URL=postgresql://kafka_lens:<from-secret-store>@postgres.example.com:5432/kafka_inspector
REDIS_URL=rediss://:<from-secret-store>@redis.example.com:6379
```

The client does not accept a CA file through its own variables. Either trust the CA system-wide in a derived image or mount it and set `NODE_EXTRA_CA_CERTS`.

## Secrets

- Inject `KAFKA_SASL_PASSWORD`, `GITHUB_TOKEN`, and database credentials from a secret manager or orchestrator secret, not from the image or a committed `.env`.
- URL-encode credentials that contain reserved characters when building `POSTGRES_URL` or `REDIS_URL`.
- A `.env` file is loaded automatically if present. Do not ship one in the image or a shared host directory in production.
- Environment variables are visible to the process and to anything that can inspect it. Restrict access accordingly.
- Logs redact configuration values at validation time; connection strings and tokens are never printed by the server.

## Migrations

Run migrations as an explicit job before rolling out a new server version:

```bash
node dist/persistence/migrate-cli.js
```

The command exits non-zero on failure and writes only to stderr. It takes a PostgreSQL session advisory lock, so concurrent jobs are safe, and it applies each SQL file in one transaction. If the job fails, do not start the new version; history and incident features will report the store as unavailable.

The server never creates or alters tables during normal operation.

## Retention

Schedule the retention CLI outside the server process, for example daily:

```cron
15 2 * * * cd /app && node dist/persistence/retain-cli.js
```

It deletes `lag_observations` older than `LAG_RETENTION_DAYS` and `incidents` older than `INCIDENT_RETENTION_DAYS` in bounded batches of 5,000 rows. `0` skips that table. Run one retention job per database; overlapping runs are safe but unnecessary.

## Metrics

Set `METRICS_ENABLED=true` to expose `GET /metrics` on `METRICS_HOST:METRICS_PORT`.

- The endpoint has no authentication.
- The default bind address is `127.0.0.1`. Only bind a wider interface behind a network policy, service mesh, or authenticated reverse proxy.
- Scrape each process separately; series include process-local counters. Add an `instance` label at scrape time.
- Recommended alerts: `mcp_tool_errors_total` rate, `diagnosis_duration_seconds` p95, and Kafka request latency.

## Health and lifecycle

- `health_check` (MCP tool) reports process liveness only. It does not contact dependencies.
- `get_readiness` reports Kafka, PostgreSQL, and Redis. A down optional dependency is reported as `unavailable`; the call still succeeds and the process keeps running.
- If the orchestrator needs a non-MCP liveness probe, enable metrics and probe the metrics port. It confirms the HTTP listener, not Kafka readiness.
- Send `SIGTERM` and allow more than the 10-second shutdown deadline. The server flushes pending history writes, closes the MCP transport, cache, Kafka admin session, metrics server, and PostgreSQL pool, then exits. A second signal is ignored; a hung shutdown force-exits with code 1.

## Scaling and availability

- Kafka reads are stateless. Add processes for more MCP clients; they do not coordinate.
- PostgreSQL is the durable source of truth. Use a managed, backed-up instance; Redis is only a cache.
- Incident deduplication uses an advisory lock per cluster/group/topic, so concurrent diagnoses across processes remain safe.
- Each process keeps one Kafka admin client and a bounded metadata cache. Size CPU and memory for the number of concurrent MCP calls, not for topic count.
- History volume is proportional to `partitions × observations`. Size PostgreSQL and retention from your topic count and tool-call rate.

## Upgrade checklist

1. Back up PostgreSQL or confirm point-in-time recovery.
2. Run the migration job against the target database.
3. Roll out the new image, pinned by digest.
4. Call `get_readiness` and `diagnose_consumer_group` for a known group.
5. Confirm logs show no `partialFailures` for required sources.
6. Roll back the image if the readiness or diagnosis check fails. Do not roll back the schema blindly; migrations are forward-only.

## Verification commands

```bash
npm run check
npm run test:integration
npm run build
npm audit --audit-level=moderate
```
