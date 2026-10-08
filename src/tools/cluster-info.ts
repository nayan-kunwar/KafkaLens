import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import type { AdminService } from '../services/admin-service.js';

import { executeTool } from './run-tool.js';

export function createGetClusterInfoHandler(
  adminService: AdminService,
  logger: Logger,
): () => Promise<CallToolResult> {
  return () => executeTool(logger, 'get_cluster_info', () => adminService.getClusterInfo());
}
