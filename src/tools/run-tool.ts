import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { AppError } from '../errors/app-error.js';
import { noopMetrics, type Metrics } from '../observability/metrics.js';
import { runToolCall } from '../observability/tool-call.js';

export function executeTool(
  logger: Logger,
  tool: string,
  run: () => Promise<unknown>,
  metrics: Metrics = noopMetrics,
): Promise<CallToolResult> {
  return runToolCall((toolCallId) => {
    const startedAt = Date.now();
    return run().then(
      (data) => {
        const durationMs = Date.now() - startedAt;
        metrics.toolCall(tool, durationMs);
        logger.debug({ tool, toolCallId, durationMs }, 'tool completed');
        return {
          content: [{ type: 'text' as const, text: JSON.stringify(data, null, 2) }],
        };
      },
      (err: unknown) => {
        const durationMs = Date.now() - startedAt;
        const appError = err instanceof AppError ? err : new AppError('INTERNAL', 'Tool failed');
        metrics.toolCall(tool, durationMs, appError.code);
        logger.warn({ tool, toolCallId, durationMs, error: appError.toJSON() }, 'tool failed');
        return {
          isError: true,
          content: [{ type: 'text' as const, text: JSON.stringify(appError.toJSON()) }],
        };
      },
    );
  });
}
