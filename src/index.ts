import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { Pool } from 'pg';

import { MetadataCache } from './cache/metadata-cache.js';
import { loadEnv } from './config/env.js';
import { loadDotenv } from './config/load-dotenv.js';
import { AppError } from './errors/app-error.js';
import { startMetricsServer } from './observability/metrics-server.js';
import { createMetrics } from './observability/metrics.js';
import { IncidentStore } from './persistence/incident-store.js';
import { LagStore } from './persistence/lag-store.js';
import { createMcpServer } from './server/mcp-server.js';
import { AdminService } from './services/admin-service.js';
import { ConsumerGroupsService } from './services/consumer-groups-service.js';
import { CorrelationService } from './services/correlation-service.js';
import { DiagnosisService } from './services/diagnosis-service.js';
import { KafkaAdminSession } from './services/kafka-admin-session.js';
import { createKafka } from './services/kafka-client.js';
import { OffsetsService } from './services/offsets-service.js';
import { ReadinessService } from './services/readiness-service.js';
import { createLogger } from './utils/logger.js';

async function main(): Promise<void> {
  const dotenvFiles = loadDotenv();
  const env = loadEnv();
  const logger = createLogger(env.LOG_LEVEL);
  logger.debug({ files: dotenvFiles }, 'dotenv files loaded');

  process.on('uncaughtException', (err) => {
    logger.fatal({ err }, 'uncaught exception');
    process.exit(1);
  });
  process.on('unhandledRejection', (err) => {
    logger.fatal({ err }, 'unhandled rejection');
    process.exit(1);
  });

  const metrics = createMetrics(env.METRICS_ENABLED);
  let metricsServer: { close: (callback?: () => void) => void } | null = null;
  if (env.METRICS_ENABLED) {
    try {
      metricsServer = await startMetricsServer(env.METRICS_PORT, metrics, logger);
    } catch (err) {
      logger.warn(
        { err: err instanceof Error ? err.message : String(err), port: env.METRICS_PORT },
        'metrics disabled after bind failure',
      );
    }
  }

  const kafka = createKafka(env, logger);
  const session = new KafkaAdminSession(kafka);
  const cache = await MetadataCache.connect(env.REDIS_URL, logger);
  const adminService = new AdminService(kafka, {
    metrics,
    cache,
    cacheTtlSeconds: env.CACHE_TTL_SECONDS,
    logger,
    session,
  });
  const consumerGroupsService = new ConsumerGroupsService(kafka, { metrics, logger, session });
  const pool = new Pool({
    connectionString: env.POSTGRES_URL,
    connectionTimeoutMillis: 5_000,
  });
  const lagStore = new LagStore(pool);
  const incidentStore = new IncidentStore(pool);
  logger.info('schema is applied by npm run migrate, not during server startup');

  const offsetsService = new OffsetsService(
    kafka,
    consumerGroupsService,
    lagStore,
    logger,
    metrics,
    session,
  );
  const correlation = new CorrelationService({
    githubRepository: env.GITHUB_REPOSITORY,
    githubToken: env.GITHUB_TOKEN,
    deploymentSourceUrl: env.DEPLOYMENT_SOURCE_URL,
    logger,
  });
  const diagnosisService = new DiagnosisService(
    adminService,
    consumerGroupsService,
    offsetsService,
    lagStore,
    incidentStore,
    correlation,
    {
      imbalanceRatio: env.PARTITION_IMBALANCE_RATIO,
      stallWindowSeconds: env.STALL_WINDOW_SECONDS,
      metrics,
      logger,
    },
  );
  const readinessService = new ReadinessService(session, pool, cache);
  const server = createMcpServer({
    adminService,
    consumerGroupsService,
    offsetsService,
    lagStore,
    diagnosisService,
    incidentStore,
    readinessService,
    logger,
    metrics,
  });

  const shutdown = (): void => {
    const closeMetrics = metricsServer
      ? new Promise<void>((resolve) => {
          metricsServer?.close(() => resolve());
        })
      : Promise.resolve();
    Promise.all([lagStore.close(), cache.close(), session.close(), closeMetrics])
      .catch(() => undefined)
      .finally(() => process.exit(0));
  };
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.on(signal, shutdown);
  }

  await server.connect(new StdioServerTransport());
  logger.info(
    { clientId: env.KAFKA_CLIENT_ID, nodeEnv: env.NODE_ENV, metrics: env.METRICS_ENABLED },
    'kafka-lens MCP server started on stdio',
  );
}

main().catch((err: unknown) => {
  if (err instanceof AppError) {
    process.stderr.write(`${JSON.stringify(err.toJSON())}\n`);
  } else {
    process.stderr.write(`${JSON.stringify({ code: 'INTERNAL', message: String(err) })}\n`);
  }
  process.exit(1);
});
