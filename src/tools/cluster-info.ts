import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type { AdminService } from '../services/admin-service.js';

import { executeTool } from './run-tool.js';

export function createGetClusterInfoHandler(
  adminService: AdminService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): () => Promise<CallToolResult> {
  return () =>
    executeTool(logger, 'get_cluster_info', () => adminService.getClusterInfo(), metrics);
}
