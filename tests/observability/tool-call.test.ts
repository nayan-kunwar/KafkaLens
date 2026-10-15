import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { runToolCall, toolCallBindings } from '../../src/observability/tool-call.js';

describe('tool call id', () => {
  it('adds the same id to log lines inside a tool call', async () => {
    const lines: string[] = [];
    const logger = pino(
      { mixin: toolCallBindings },
      {
        write(line: string) {
          lines.push(line);
        },
      },
    );

    await runToolCall((toolCallId) => {
      logger.info('inside');
      logger.info({ toolCallId }, 'explicit');
      return Promise.resolve();
    });
    logger.info('outside');

    const inside = JSON.parse(lines[0] ?? '{}') as { toolCallId?: string; msg?: string };
    const explicit = JSON.parse(lines[1] ?? '{}') as { toolCallId?: string };
    const outside = JSON.parse(lines[2] ?? '{}') as { toolCallId?: string };
    expect(inside.msg).toBe('inside');
    expect(inside.toolCallId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
    expect(explicit.toolCallId).toBe(inside.toolCallId);
    expect(outside.toolCallId).toBeUndefined();
  });
});
