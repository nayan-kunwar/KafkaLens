import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

import { loadEnv } from './config/env.js';
import { loadDotenv } from './config/load-dotenv.js';
import { AppError } from './errors/app-error.js';
import { createMcpServer } from './server/mcp-server.js';
import { AdminService } from './services/admin-service.js';
import { ConsumerGroupsService } from './services/consumer-groups-service.js';
import { createKafka } from './services/kafka-client.js';
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

  const kafka = createKafka(env, logger);
  const adminService = new AdminService(kafka);
  const consumerGroupsService = new ConsumerGroupsService(kafka);
  const server = createMcpServer({ adminService, consumerGroupsService, logger });

  await server.connect(new StdioServerTransport());
  logger.info(
    { clientId: env.KAFKA_CLIENT_ID, nodeEnv: env.NODE_ENV },
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
