import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type { ReadinessService } from '../services/readiness-service.js';

import { executeTool } from './run-tool.js';

export function createGetReadinessHandler(
  readiness: ReadinessService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): () => Promise<CallToolResult> {
  return () => executeTool(logger, 'get_readiness', () => readiness.report(), metrics);
}
