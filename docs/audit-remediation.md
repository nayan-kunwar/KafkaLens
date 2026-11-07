# Audit Remediation Plan

This document tracks the fixes for the full application audit performed against revision
`1deb4ec` (v0.2.1). It is the shared reference for the implementation work: each finding
has an ID, a location, the intended fix, and a status.

## Status legend

| Status        | Meaning                                           |
| ------------- | ------------------------------------------------- |
| `TODO`        | Confirmed finding, no change made yet.            |
| `IN PROGRESS` | Being implemented in the current batch.           |
| `DONE`        | Implemented, tested, and verified.                |
| `DEFERRED`    | Deliberately postponed; rationale recorded below. |
| `WON'T FIX`   | Accepted risk; rationale recorded below.          |

## Baseline

| Item                          | Value                                 |
| ----------------------------- | ------------------------------------- |
| Revision                      | `1deb4ec`                             |
| Version                       | 0.2.1                                 |
| Audit date                    | 2026-11-07                            |
| Unit tests at baseline        | 224 passing                           |
| Integration tests at baseline | 25 passing                            |
| Build at baseline             | Passing                               |
| Production audit at baseline  | 5 vulnerabilities: 2 high, 3 moderate |

## Post-remediation

| Item                   | Value                                              |
| ---------------------- | -------------------------------------------------- |
| Unit tests             | 263 passing                                        |
| Production audit       | 0 vulnerabilities                                  |
| Full dependency audit  | 0 vulnerabilities (including dev dependencies)     |
| Build artifacts        | `dist` (server) and `dist-demo` (demo) built clean |
| Read-only boundary     | Capability manifest enforced at registration       |
| Structured tool errors | Cause and stack logged; client gets `AppError`     |

## Verification commands

Run after each batch:

```bash
npm run typecheck
npm run lint
npm run test
npm run build
npm run test:integration
npm audit --omit=dev --audit-level=high
npm audit --audit-level=moderate
```

`npm run check` runs typecheck, lint, format check, and unit tests.

## High-severity findings

| ID  | Area                     | Finding                                                                                              | Location                                                    | Status |
| --- | ------------------------ | ---------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | ------ |
| H-1 | Security / supply chain  | `zod@4.6.5` and `body-parser@2.3.0` advisories; transitive `fast-uri`, `ip-address`, `qs` advisories | `package.json`, `package-lock.json`                         | DONE   |
| H-2 | Bug / correctness        | `diagnose()` can return `no_issue_detected` when all watermarks are unknown and `memberCount === 0`  | `src/domain/diagnosis.ts`                                   | DONE   |
| H-3 | Reliability              | No `pool.on('error')`; an idle PostgreSQL client error terminates the process                        | `src/index.ts`                                              | DONE   |
| H-4 | Security / configuration | Kafka, PostgreSQL, Redis, and metrics published on all host interfaces with default/no credentials   | `docker-compose.yml`, `src/observability/metrics-server.ts` | DONE   |

## Medium-severity findings

| ID   | Area                      | Finding                                                                                                   | Location                                                                       | Status |
| ---- | ------------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | ------ |
| M-1  | Security / reliability    | Correlation HTTP buffers the whole body before enforcing the 1 MB cap; redirects/SSRF hardening missing   | `src/services/correlation-service.ts`                                          | DONE   |
| M-2  | Security / AI safety      | Prompt injection via raw group/topic interpolation and untrusted correlation events                       | `src/server/prompts.ts`, `src/services/correlation-service.ts`                 | DONE   |
| M-3  | Bug / data integrity      | `cluster_id` is written but never filtered in history, dedupe, or incident lookup                         | `src/persistence/*`, `src/services/*`                                          | DONE   |
| M-4  | Bug / data loss           | History persists only the first 500 partitions of a topic                                                 | `src/services/offsets-service.ts`                                              | DONE   |
| M-5  | Performance               | Group-wide lag does up to 100 sequential watermark calls and awaits history writes                        | `src/services/offsets-service.ts`                                              | DONE   |
| M-6  | Performance / correctness | Diagnosis performs three `describeGroups` calls and a redundant topic-metadata call                       | `src/services/diagnosis-service.ts`, `src/services/consumer-groups-service.ts` | DONE   |
| M-7  | Reliability               | No PostgreSQL statement/query timeout and no tool-level deadline; Kafka retries can block the admin queue | `src/index.ts`, `src/config/env.ts`, `src/tools/run-tool.ts`                   | DONE   |
| M-8  | Performance / database    | Index/query mismatches; retention delete is a full scan and one large transaction                         | `src/persistence/migrations/*`, `src/persistence/retention.ts`                 | DONE   |
| M-9  | Bug / observability       | Tool errors discard the original cause and stack                                                          | `src/tools/run-tool.ts`                                                        | DONE   |
| M-10 | Bug / configuration       | Numeric env validation accepts `Infinity` and extreme values                                              | `src/config/env.ts`                                                            | DONE   |
| M-11 | Bug / reliability         | Naive SQL statement splitter; migrations are not concurrency-safe                                         | `src/persistence/migrate.ts`                                                   | DONE   |
| M-12 | Bug / data integrity      | Incident dedupe is select-then-insert, race-prone, and ignores `cluster_id`                               | `src/persistence/incident-store.ts`                                            | DONE   |
| M-13 | Code quality              | `as never` casts defeat type checking at every MCP tool registration                                      | `src/server/mcp-server.ts`                                                     | DONE   |
| M-14 | Best practice / testing   | Integration tests run only on `main`; no dependency scan in CI                                            | `.github/workflows/check.yml`                                                  | DONE   |

## Low-severity findings

| ID   | Area             | Finding                                                                                            | Location                                                                                         | Status |
| ---- | ---------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ | ------ |
| L-1  | Input validation | Resources bypass shared schemas; prompts use a weaker topic schema                                 | `src/server/resources.ts`, `src/server/prompts.ts`                                               | DONE   |
| L-2  | Data quality     | Incident similarity takes the newest 200 candidates before scoring                                 | `src/persistence/incident-store.ts`                                                              | DONE   |
| L-3  | Performance      | History writes are awaited on every lag read                                                       | `src/services/offsets-service.ts`                                                                | DONE   |
| L-4  | Code quality     | Duplicate `UNKNOWN` branch; test-only production APIs; unused `CorrelationService.logger`          | `src/domain/diagnosis.ts`, `src/persistence/lag-store.ts`, `src/services/correlation-service.ts` | DONE   |
| L-5  | Image hygiene    | Source maps and demo build ship in the production image; images are tag-pinned; no app healthcheck | `tsconfig.build.json`, `Dockerfile`, `docker-compose.yml`                                        | DONE   |
| L-6  | Reliability      | Shutdown is not idempotent and does not close the MCP transport                                    | `src/index.ts`                                                                                   | DONE   |
| L-7  | Security         | Metrics server binds all interfaces by default and has no auth                                     | `src/observability/metrics-server.ts`                                                            | DONE   |
| L-8  | Configuration    | Development defaults for `POSTGRES_URL`/`KAFKA_BROKERS` are accepted in production                 | `src/config/env.ts`                                                                              | DONE   |
| L-9  | Performance      | Per-partition history filtering is O(partitions × history); responses are pretty-printed           | `src/services/diagnosis-service.ts`, `src/tools/run-tool.ts`                                     | DONE   |
| L-10 | Testing          | No tests for resources, metrics server, startup/shutdown, CLIs, or pool errors                     | `tests/`                                                                                         | DONE   |
| L-11 | Security         | Read-only enforcement is name-based, not capability-based                                          | `src/safety/readonly.ts`                                                                         | DONE   |

## Implementation notes

### 2026-11-07 — Remediation started

- Created this document before making code changes.
- Work proceeded in batches: dependencies/security, correctness/domain, persistence/data, then
  code quality/infrastructure.
- Each batch kept `npm run check` green before moving on.

### Dependencies and security

- H-1: `npm audit fix` updated transitive packages and `zod` was raised to `^4.6.7`.
  `npm audit --omit=dev --audit-level=high` now reports 0 vulnerabilities.
- H-4/L-7: Compose publishes Kafka, PostgreSQL, Redis, and metrics on `127.0.0.1` only.
  PostgreSQL and Redis use configurable passwords; metrics defaults to `METRICS_HOST=127.0.0.1`.
- M-1: correlation bodies are streamed with a hard byte cap, redirects are blocked, URLs must be
  HTTPS (loopback HTTP is allowed), and credentials in URLs are rejected.
- M-2: prompt values are JSON-escaped, prompts tell the model to treat external text as data, and
  every correlation event carries `untrusted: true` after control/bidi sanitization.
- L-11: `TOOL_CAPABILITIES` declares per-tool capabilities; registration rejects any tool that
  declares a non-read Kafka capability.

### Correctness and domain

- H-2: any `UNKNOWN_WATERMARK` or `INVALID_OFFSET` now prevents a `no_issue_detected` verdict.
- M-6: `getConsumerGroupWithAssignments()` describes once; diagnosis reuses that state for the
  lag read and no longer fetches topic metadata separately.
- L-9: history points are grouped once per partition instead of filtering the full history for
  every partition; tool and resource responses use compact JSON.

### Persistence and data

- M-3: history, incident lookup, dedupe, and `get_lag_history` filter by `cluster_id`. If the
  cluster id cannot be read, diagnosis degrades to unscoped history and records a partial failure.
- M-4/L-3: every partition is persisted, while response pages stay capped at 500. Writes are
  backgrounded; `OffsetsService.flushHistory()` lets diagnosis and shutdown wait when needed.
- M-5: group-wide topic watermarks run with concurrency 8.
- M-8: migration `003-cluster-indexes.sql` adds cluster and retention indexes; retention deletes in
  bounded CTE batches.
- M-11: migrations run under a session advisory lock and each SQL file executes in one transaction.
- M-12: incident dedupe uses a transaction plus an advisory lock; candidate selection orders by
  group/topic/type relevance before deterministic scoring.
- H-3/M-7: `createPgPool()` attaches an idle-client error handler and applies connection,
  statement, query, and idle-in-transaction timeouts.
- L-6: shutdown is idempotent, flushes history, closes the MCP transport, and force-exits after 10s.

### Infrastructure and quality

- L-5: the build cleans `dist`/`dist-demo`; the production image has no demo output or source maps;
  the demo is a separate Docker stage; Node is pinned to `22.20.0-alpine`.
- M-9/M-13: tool failures log the original cause and stack while returning only `AppError`;
  MCP registration is typed with `ToolCallback`/`ZodRawShapeCompat` instead of `as never`.
- M-14: CI runs integration tests on pull requests and enforces both the production and full dependency
  audits.
- L-4: removed `LagStore.fromUrl` and the unused correlation logger option.
- L-10: shutdown moved to `createShutdownHandler()` with tests for idempotence, flush ordering,
  failure tolerance, and forced exit. `runMigrateCli()` and `runRetainCli()` return exit codes and
  are tested with injected pools, including structured failures and invalid configuration.
- Dev advisories: `overrides` pin `minimatch` to `^11.0.1` and `glob` to `^14.0.2`. Both provide
  CommonJS exports, and the ESLint toolchain plus an archiver glob/directory smoke test pass.
  Remove the overrides once ESLint, TypeScript-ESLint, and Testcontainers widen their ranges.

### Accepted risks

- Local Compose Kafka remains PLAINTEXT because it is loopback-only. Production deployments must
  configure SASL/TLS and explicit endpoints; `NODE_ENV=production` rejects localhost defaults.
- `testcontainers@12.2.1` warns that Node `>=22.22` is required while local Node is `22.20.0`.
  The Docker image is pinned to `22.20.0-alpine`; the warning does not affect the suite.

## Verification log

| Date       | Batch        | Command                                   | Result                                    |
| ---------- | ------------ | ----------------------------------------- | ----------------------------------------- |
| 2026-11-07 | Baseline     | `npm run check`                           | 224/224 unit tests passing                |
| 2026-11-07 | Baseline     | `npm run build`                           | Passing                                   |
| 2026-11-07 | Baseline     | `npm run test:integration`                | 25/25 passing                             |
| 2026-11-07 | Baseline     | `npm audit --omit=dev`                    | 5 vulnerabilities (2 high, 3 moderate)    |
| 2026-11-07 | Remediation  | `npm run check`                           | 253/253 unit tests passing                |
| 2026-11-07 | Remediation  | `npm run build && npm run build:demo`     | Passing; clean `dist` and `dist-demo`     |
| 2026-11-07 | Remediation  | `npm run test:integration`                | 25/25 passing                             |
| 2026-11-07 | Remediation  | `npm audit --omit=dev --audit-level=high` | 0 vulnerabilities                         |
| 2026-11-07 | Remediation  | `docker build --target demo`              | Demo and runtime stages built             |
| 2026-11-07 | Remediation  | runtime image inspection                  | No `dist-demo` output and no `.map` files |
| 2026-11-07 | Finalization | `npm run check`                           | 263/263 unit tests passing                |
| 2026-11-07 | Finalization | `npm run build && npm run build:demo`     | Passing                                   |
| 2026-11-07 | Finalization | `npm run test:integration`                | 25/25 passing                             |
| 2026-11-07 | Finalization | `npm audit --audit-level=moderate`        | 0 vulnerabilities (including dev)         |
| 2026-11-07 | Finalization | `docker build --target demo`              | Demo and runtime stages built clean       |
| 2026-11-07 | Finalization | runtime image inspection                  | No `dist-demo` output and no `.map` files |
