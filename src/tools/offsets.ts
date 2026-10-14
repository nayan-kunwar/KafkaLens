import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type { GetConsumerLagInput } from '../schemas/lag.js';
import type { OffsetsService } from '../services/offsets-service.js';

import { executeTool } from './run-tool.js';

export function createGetConsumerLagHandler(
  offsetsService: OffsetsService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: GetConsumerLagInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'get_consumer_lag',
      () => offsetsService.getConsumerLag(args.groupId, args.topic),
      metrics,
    );
}
