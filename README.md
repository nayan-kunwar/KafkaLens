# kafka-lens

**Kafka Inspector MCP** — a Model Context Protocol server that gives AI agents structured, evidence-based access to Kafka operational information for investigating consumer lag, partition imbalance, stalled consumers, and related problems.

> MCP is the interface. The diagnostic engine is the product.

## Status

Version **0.2.0**. Milestones M0–M20 are implemented. The server is read-only. PLAINTEXT and SASL/SSL are supported.

The investigation entry point is `diagnose_consumer_group`. How to read its output is in [docs/diagnosis.md](docs/diagnosis.md).

New to MCP? Hands-on notes from building this server live in [docs/mcp/](docs/mcp/).

## Requirements

- Node.js >= 20
- Docker (integration tests and the demo stack)

## Getting started

```bash
npm install
cp .env.example .env
```

`.env` is loaded automatically at startup from the project root (then the working directory). Real environment variables always take precedence over the file.

## Commands

| Command                          | Purpose                                             |
| -------------------------------- | --------------------------------------------------- |
| `npm run dev`                    | Run server with hot reload (tsx)                    |
| `npm run build`                  | Compile TypeScript to `dist/`                       |
| `npm run build:demo`             | Compile the demo scenarios                          |
| `npm start`                      | Run compiled server                                 |
| `npm run typecheck`              | Type-check `src/`, `tests/`, `demo/`                |
| `npm run lint`                   | ESLint (typed rules, import ordering)               |
| `npm run format`                 | Prettier write                                      |
| `npm run format:check`           | Prettier check                                      |
| `npm test`                       | Vitest run (unit)                                   |
| `npm run test:integration`       | Vitest run against real Kafka (Docker)              |
| `npm run check`                  | typecheck + lint + format:check + test              |
| `npm run demo:up`                | Start Kafka, PostgreSQL, Redis, and the MCP service |
| `npm run demo:down`              | Stop the Compose stack                              |
| `npm run demo:slow-consumer`     | Produce faster than a slow consumer                 |
| `npm run demo:stalled-consumer`  | Consumer joins, then pauses                         |
| `npm run demo:partition-hotspot` | One key lands on one partition                      |
| `npm run demo:producer-spike`    | Burst of records, then a trickle                    |

## Configuration

All configuration is environment-driven (see `.env.example`). A `.env` file in the project root (or working directory) is loaded automatically if present; values already set in the environment are never overridden.

| Variable                        | Default                                           | Description                                      |
| ------------------------------- | ------------------------------------------------- | ------------------------------------------------ |
| `NODE_ENV`                      | `development`                                     | Runtime environment                              |
| `KAFKA_BROKERS`                 | `localhost:9092`                                  | Comma-separated broker list                      |
| `KAFKA_CLIENT_ID`               | `kafka-inspector`                                 | Kafka client id                                  |
| `KAFKA_SECURITY_PROTOCOL`       | `PLAINTEXT`                                       | `PLAINTEXT`/`SSL`/`SASL_PLAINTEXT`/`SASL_SSL`    |
| `KAFKA_SASL_MECHANISM`          | `plain`                                           | `plain`/`scram-sha-256`/`scram-sha-512`          |
| `KAFKA_SASL_USERNAME`           | —                                                 | Required when protocol uses SASL                 |
| `KAFKA_SASL_PASSWORD`           | —                                                 | Required when protocol uses SASL (never logged)  |
| `KAFKA_SSL_REJECT_UNAUTHORIZED` | `true`                                            | Verify broker TLS certificates                   |
| `KAFKA_REQUEST_TIMEOUT_MS`      | `30000`                                           | Kafka request timeout                            |
| `KAFKA_CONNECTION_TIMEOUT_MS`   | `10000`                                           | Kafka connection timeout                         |
| `KAFKA_RETRY_ATTEMPTS`          | `5`                                               | Client retry attempts on retryable errors        |
| `POSTGRES_URL`                  | `postgresql://postgres:postgres@localhost:5432/…` | Durable lag and incident history                 |
| `REDIS_URL`                     | `redis://localhost:6379`                          | Short-lived metadata cache                       |
| `LOG_LEVEL`                     | `info`                                            | Pino log level (stderr)                          |
| `PARTITION_IMBALANCE_RATIO`     | `5`                                               | Max/min (or max/mean) lag outlier threshold      |
| `STALL_WINDOW_SECONDS`          | `300`                                             | Progress and rate observation window             |
| `CACHE_TTL_SECONDS`             | `15`                                              | Redis TTL for cluster and topic metadata         |
| `METRICS_ENABLED`               | `false`                                           | Expose Prometheus metrics                        |
| `METRICS_PORT`                  | `9464`                                            | Metrics HTTP port (`GET /metrics`)               |
| `GITHUB_REPOSITORY`             | —                                                 | Optional `owner/name` for change correlation     |
| `GITHUB_TOKEN`                  | —                                                 | Optional token for the GitHub API (never logged) |
| `DEPLOYMENT_SOURCE_URL`         | —                                                 | Optional JSON feed of deployments                |

Invalid configuration fails fast with a structured `CONFIG_INVALID` error that reports field names only — never values or credentials.

PostgreSQL, Redis, GitHub, and the deployment feed are optional. The process starts without them. History, incidents, cache, and correlation then report that they are unavailable. Kafka reads still have to succeed.

## Architecture

```
MCP Layer            src/server, src/tools
Application Services src/services
Domain / Diagnosis   src/domain
Infrastructure       src/persistence, src/cache, src/observability
Cross-cutting        src/errors, src/safety, src/utils
```

- Structured logs go to **stderr** (stdout is reserved for the MCP stdio protocol).
- Read-only by design. `src/safety/readonly.ts` rejects destructive Kafka operations at startup and at registration. A future write would still need an explicit user confirmation. Model output is not confirmation.
- PostgreSQL is the durable store for lag observations and incidents. Redis only caches cluster and topic metadata.

## MCP usage

Register with an MCP client (stdio):

```json
{
  "mcpServers": {
    "kafka-lens": {
      "command": "node",
      "args": ["path/to/kafka-lens/dist/index.js"],
      "env": { "KAFKA_BROKERS": "localhost:9092" }
    }
  }
}
```

The `env` block is optional — a `.env` file next to the project works too.

### Tools

- `health_check` — server liveness
- `get_cluster_info` — cluster id, controller, and brokers
- `list_topics`, `get_topic_metadata`, `get_partition_info`
- `list_consumer_groups`, `get_consumer_group`, `get_consumer_assignments`
- `get_consumer_lag` — per-partition lag. Null means unknown. Totals sum known partitions only
- `get_lag_history` — stored observations, newest first, including partition id
- `analyze_consumer_lag`, `analyze_partition_imbalance`, `analyze_throughput`, `detect_stalled_consumers`
- `diagnose_consumer_group` — metadata, membership, assignments, lag, history, imbalance, progress, throughput, findings, and optional incidents and change correlation
- `find_similar_incidents` — deterministic overlap with stored incidents

Every tool is annotated read-only. Responses are bounded.

### Resources

- `kafka://topics`
- `kafka://topic/{topic}`
- `kafka://consumer-groups`
- `kafka://consumer-group/{group}`
- `kafka://consumer-group/{group}/lag`
- `kafka://incidents/{id}`

### Prompts

`diagnose_kafka_lag`, `investigate_consumer_group`, `analyze_partition_imbalance`, and `explain_consumer_group` tell the agent which tool to call and how to separate facts, estimates, inferences, and next steps. They do not name a cause.

## Demo

```bash
npm run demo:up
npm run demo:slow-consumer
```

`demo:up` starts Kafka (localhost:9092), PostgreSQL, Redis, and the MCP service. The service in Compose speaks MCP on stdin and exposes metrics on port 9464. For an editor or other MCP client, run `npm run dev` on the host against the same brokers.

Each demo script prints the `groupId` and `topic` to pass to `diagnose_consumer_group`. Shorten the stall window in `.env` (`STALL_WINDOW_SECONDS=60`) if you want a stall finding before the default five minutes of history exist.

Compose also has profile services `demo-slow-consumer`, `demo-stalled-consumer`, `demo-partition-hotspot`, and `demo-producer-spike` for the same scenarios inside the Docker network.

## Metrics

Set `METRICS_ENABLED=true`. `GET /metrics` exposes:

- `mcp_tool_calls_total`
- `mcp_tool_errors_total`
- `kafka_request_duration_seconds`
- `diagnosis_duration_seconds`
- `lag_observations_total`
