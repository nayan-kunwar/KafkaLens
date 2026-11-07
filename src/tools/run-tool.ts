import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { AppError } from '../errors/app-error.js';
import { noopMetrics, type Metrics } from '../observability/metrics.js';
import { runToolCall } from '../observability/tool-call.js';

const DEFAULT_TOOL_TIMEOUT_MS = 60_000;
let toolTimeoutMs = DEFAULT_TOOL_TIMEOUT_MS;

/**
 * Sets the maximum wall-clock time a tool handler may spend before the MCP
 * caller receives an INTERNAL timeout result. The underlying operation cannot
 * be cancelled, but it no longer blocks the response forever.
 */
export function setToolTimeoutMs(value: number): void {
  if (Number.isFinite(value) && value > 0) toolTimeoutMs = Math.floor(value);
}

export function executeTool(
  logger: Logger,
  tool: string,
  run: () => Promise<unknown>,
  metrics: Metrics = noopMetrics,
): Promise<CallToolResult> {
  return runToolCall(() => {
    const startedAt = Date.now();
    const timeout = toolTimeoutMs;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        reject(new AppError('INTERNAL', `Tool timed out after ${timeout} ms`));
      }, timeout);
      timer.unref();
    });

    return Promise.race([run(), deadline])
      .then(
        (data) => {
          const durationMs = Date.now() - startedAt;
          metrics.toolCall(tool, durationMs);
          logger.debug({ tool, durationMs }, 'tool completed');
          return {
            content: [{ type: 'text' as const, text: JSON.stringify(data) }],
          };
        },
        (err: unknown) => {
          if (timer !== undefined) clearTimeout(timer);
          const durationMs = Date.now() - startedAt;
          const appError = err instanceof AppError ? err : new AppError('INTERNAL', 'Tool failed');
          metrics.toolCall(tool, durationMs, appError.code);
          // Keep the underlying cause and stack in server logs. The client gets
          // only the structured AppError, which never contains connection
          // strings or credentials for unexpected failures.
          logger.warn({ tool, durationMs, error: appError.toJSON(), err }, 'tool failed');
          return {
            isError: true,
            content: [{ type: 'text' as const, text: JSON.stringify(appError.toJSON()) }],
          };
        },
      )
      .finally(() => {
        if (timer !== undefined) clearTimeout(timer);
      });
  });
}
