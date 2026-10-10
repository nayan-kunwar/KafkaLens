# kafka-lens

**Kafka Inspector MCP** — a Model Context Protocol server that gives AI agents structured, evidence-based access to Kafka operational information for investigating consumer lag, partition imbalance, stalled consumers, and related problems.

> MCP is the interface. The diagnostic engine is the product.

## Status

Milestone **M3 — consumer groups** (read-only; PLAINTEXT and SASL/SSL supported).

New to MCP? Hands-on, spec-grounded learning notes from building this server live in [docs/mcp/](docs/mcp/) — error handling, the stdio transport, and the MCP Inspector workflow.

## Requirements

- Node.js >= 20
- Docker (integration tests only)

## Getting started

```bash
npm install
cp .env.example .env
```

`.env` is loaded automatically at startup from the project root (then the working directory). Real environment variables always take precedence over the file.

## Commands

| Command                    | Purpose                                |
| -------------------------- | -------------------------------------- |
| `npm run dev`              | Run server with hot reload (tsx)       |
| `npm run build`            | Compile TypeScript to `dist/`          |
| `npm start`                | Run compiled server                    |
| `npm run typecheck`        | Type-check `src/`, `tests/`, config    |
| `npm run lint`             | ESLint (typed rules, import ordering)  |
| `npm run format`           | Prettier write                         |
| `npm run format:check`     | Prettier check                         |
| `npm test`                 | Vitest run (unit)                      |
| `npm run test:integration` | Vitest run against real Kafka (Docker) |
| `npm run check`            | typecheck + lint + format:check + test |

## Configuration

All configuration is environment-driven (see `.env.example`). A `.env` file in the project root (or working directory) is loaded automatically if present; values already set in the environment are never overridden.

| Variable                        | Default                                           | Description                                     |
| ------------------------------- | ------------------------------------------------- | ----------------------------------------------- |
| `NODE_ENV`                      | `development`                                     | Runtime environment                             |
| `KAFKA_BROKERS`                 | `localhost:9092`                                  | Comma-separated broker list                     |
| `KAFKA_CLIENT_ID`               | `kafka-inspector`                                 | Kafka client id                                 |
| `KAFKA_SECURITY_PROTOCOL`       | `PLAINTEXT`                                       | `PLAINTEXT`/`SSL`/`SASL_PLAINTEXT`/`SASL_SSL`   |
| `KAFKA_SASL_MECHANISM`          | `plain`                                           | `plain`/`scram-sha-256`/`scram-sha-512`         |
| `KAFKA_SASL_USERNAME`           | —                                                 | Required when protocol uses SASL                |
| `KAFKA_SASL_PASSWORD`           | —                                                 | Required when protocol uses SASL (never logged) |
| `KAFKA_SSL_REJECT_UNAUTHORIZED` | `true`                                            | Verify broker TLS certificates                  |
| `KAFKA_REQUEST_TIMEOUT_MS`      | `30000`                                           | Kafka request timeout                           |
| `KAFKA_CONNECTION_TIMEOUT_MS`   | `10000`                                           | Kafka connection timeout                        |
| `KAFKA_RETRY_ATTEMPTS`          | `5`                                               | Client retry attempts on retryable errors       |
| `POSTGRES_URL`                  | `postgresql://postgres:postgres@localhost:5432/…` | Durable lag-history store                       |
| `REDIS_URL`                     | `redis://localhost:6379`                          | Short-lived cache                               |
| `LOG_LEVEL`                     | `info`                                            | Pino log level (stderr)                         |
| `PARTITION_IMBALANCE_RATIO`     | `5`                                               | Imbalance detection threshold                   |
| `STALL_WINDOW_SECONDS`          | `300`                                             | Stall detection observation window              |

Invalid configuration fails fast with a structured `CONFIG_INVALID` error that reports field names only — never values or credentials.

## Architecture

```
MCP Layer            src/server, src/tools
Application Services src/services
Domain / Diagnosis    src/domain
Infrastructure        src/schemas, src/config
Cross-cutting         src/errors, src/utils
```

- Structured logs go to **stderr** (stdout is reserved for the MCP stdio protocol).
- Read-only by design: no destructive Kafka operations.

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

Available tools:

- `health_check` — server liveness
- `get_cluster_info` — live cluster id, controller, and broker list from Kafka
- `list_topics` — topic names with optional substring filter and bounded limit (total/truncated flags)
- `get_topic_metadata` — partition count and per-partition leader, replicas, and ISR for one topic
- `get_partition_info` — per-partition leader, replicas, ISR, and high/low watermarks (decimal strings; null means unknown)
- `list_consumer_groups` — group ids with state and memberCount, optional substring/protocol filters and bounded limit (total/truncated flags)
- `get_consumer_group` — state, protocol, and members for one group; NOT_FOUND when unknown
- `get_consumer_assignments` — decoded topic-partition assignments per member (null means unknown, not empty)
