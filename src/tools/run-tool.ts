import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { AppError } from '../errors/app-error.js';

export function executeTool(
  logger: Logger,
  tool: string,
  run: () => Promise<unknown>,
): Promise<CallToolResult> {
  const startedAt = Date.now();
  return run().then(
    (data) => {
      logger.debug({ tool, durationMs: Date.now() - startedAt }, 'tool completed');
      return {
        content: [{ type: 'text' as const, text: JSON.stringify(data, null, 2) }],
      };
    },
    (err: unknown) => {
      const durationMs = Date.now() - startedAt;
      const appError = err instanceof AppError ? err : new AppError('INTERNAL', 'Tool failed');
      logger.warn({ tool, durationMs, error: appError.toJSON() }, 'tool failed');
      return {
        isError: true,
        content: [{ type: 'text' as const, text: JSON.stringify(appError.toJSON()) }],
      };
    },
  );
}
