import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type { LagStore } from '../persistence/lag-store.js';
import type { GetLagHistoryInput } from '../schemas/history.js';

import { executeTool } from './run-tool.js';

export function createGetLagHistoryHandler(
  lagStore: LagStore,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: GetLagHistoryInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'get_lag_history',
      async () => {
        const { observations, truncated } = await lagStore.queryHistory({
          groupId: args.groupId,
          topic: args.topic,
          partition: args.partition,
          since: args.since,
          limit: args.limit,
        });
        return {
          groupId: args.groupId,
          topic: args.topic,
          observations,
          truncated,
          observedAt: new Date().toISOString(),
        };
      },
      metrics,
    );
}
