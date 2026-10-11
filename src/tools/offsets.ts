import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import type { GetConsumerLagInput } from '../schemas/lag.js';
import type { OffsetsService } from '../services/offsets-service.js';

import { executeTool } from './run-tool.js';

export function createGetConsumerLagHandler(
  offsetsService: OffsetsService,
  logger: Logger,
): (args: GetConsumerLagInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(logger, 'get_consumer_lag', () =>
      offsetsService.getConsumerLag(args.groupId, args.topic),
    );
}
