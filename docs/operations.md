# Operations

## Kafka ACLs

This client is read-only. Grant only:

| Resource | Operation | Used for                              |
| -------- | --------- | ------------------------------------- |
| Cluster  | Describe  | Cluster id and broker list            |
| Topic    | Describe  | Topic metadata and log end offsets    |
| Group    | Describe  | Group state, members, and assignments |
| Group    | Read      | Committed offsets                     |

Do not grant Alter, AlterConfigs, Delete, Create, Write, or ClusterAction. The server does not create topics, produce, delete, or change offsets. Demo scripts under `demo/` are separate processes and do write to the local broker.

## Schema

`npm run migrate` applies `src/persistence/migrations` in file name order and records them in `schema_migrations`. The server process does not create tables. Run migrate before `npm start` or `npm run dev`. The Compose image runs migrate from its entrypoint, then starts the server even if migrate fails. History and incident calls then report the store as unavailable. Inside the production image `tsx` is absent, so use the entrypoint (or `node dist/persistence/migrate-cli.js`) rather than `npm run migrate` there.

Migrations take a session advisory lock and run each SQL file in one transaction, so concurrent migrate processes and partially failing files are safe.

`npm run retain` deletes `lag_observations` older than `LAG_RETENTION_DAYS` (default 14) and `incidents` older than `INCIDENT_RETENTION_DAYS` (default 90). `0` skips that table. Schedule it outside the server process. Deletes use a parameter for the cutoff and run in bounded batches of 5,000 rows. Table names come from a fixed list.

## Liveness and readiness

`health_check` reports that the process is up. It does not contact Kafka, PostgreSQL, or Redis.

`get_readiness` checks all three. A down dependency is `unavailable` in the report and the tool still returns. The process keeps running.

## Tracing

Each tool call generates a `toolCallId`. Log lines emitted while that call is running include it, including Kafka request timings and diagnosis timings. Prometheus series stay labeled by tool and operation. A unique id is not a metric label, because that would create one series per call.

## Metrics

The metrics server binds to `METRICS_HOST` (default `127.0.0.1`). It has no authentication, so only bind a wider interface behind a trusted network boundary. The Compose service sets `METRICS_HOST=0.0.0.0` for the container healthcheck and publishes the port on loopback only.

## Timeouts and shutdown

Each MCP tool call has a `TOOL_TIMEOUT_MS` deadline (default 60s). PostgreSQL connections use `POSTGRES_CONNECTION_TIMEOUT_MS`, server-side statements use `POSTGRES_STATEMENT_TIMEOUT_MS`, and client-side queries use `POSTGRES_QUERY_TIMEOUT_MS`. An idle client error is logged instead of terminating the process.

On `SIGINT`/`SIGTERM` the server flushes in-flight history writes, closes the MCP transport, cache, Kafka admin session, metrics server, and PostgreSQL pool, then exits. A second signal is ignored; shutdown force-exits after 10 seconds.

## Secrets

Compose passwords and the local `POSTGRES_URL` are for the demo stack only. Set `POSTGRES_PASSWORD` and `REDIS_PASSWORD` to override the demo defaults. `GITHUB_TOKEN` is optional. Correlation stays off unless `GITHUB_REPOSITORY` or `DEPLOYMENT_SOURCE_URL` is set. The token is sent to GitHub and must not be logged. One process uses one `KAFKA_BROKERS` list.
