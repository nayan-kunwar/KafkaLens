import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { AppError } from '../errors/app-error.js';
import type { AdminService } from '../services/admin-service.js';

export function createGetClusterInfoHandler(
  adminService: AdminService,
  logger: Logger,
): () => Promise<CallToolResult> {
  return async (): Promise<CallToolResult> => {
    const startedAt = Date.now();
    try {
      const info = await adminService.getClusterInfo();
      const durationMs = Date.now() - startedAt;
      logger.debug({ tool: 'get_cluster_info', durationMs }, 'tool completed');
      return {
        content: [{ type: 'text', text: JSON.stringify(info, null, 2) }],
      };
    } catch (err) {
      const durationMs = Date.now() - startedAt;
      const appError = err instanceof AppError ? err : new AppError('INTERNAL', 'Tool failed');
      logger.warn(
        { tool: 'get_cluster_info', durationMs, error: appError.toJSON() },
        'tool failed',
      );
      return {
        isError: true,
        content: [{ type: 'text', text: JSON.stringify(appError.toJSON()) }],
      };
    }
  };
}
