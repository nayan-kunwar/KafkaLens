import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { logTiming, runToolCall, toolCallBindings } from '../../src/observability/tool-call.js';

function captureLogger(): { logger: pino.Logger; lines: string[] } {
  const lines: string[] = [];
  const logger = pino(
    { level: 'debug', mixin: toolCallBindings },
    {
      write(line: string) {
        lines.push(line);
      },
    },
  );
  return { logger, lines };
}

function parse(line: string | undefined): Record<string, unknown> {
  return JSON.parse(line ?? '{}') as Record<string, unknown>;
}

describe('tool call id', () => {
  it('adds one id to every log line inside a tool call', async () => {
    const { logger, lines } = captureLogger();

    await runToolCall(() => {
      logger.info('first');
      logger.info('second');
      return Promise.resolve();
    });
    logger.info('outside');

    const first = parse(lines[0]);
    const second = parse(lines[1]);
    const outside = parse(lines[2]);
    expect(first.msg).toBe('first');
    expect(second.msg).toBe('second');
    expect(first.toolCallId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
    expect(second.toolCallId).toBe(first.toolCallId);
    expect(outside.toolCallId).toBeUndefined();
  });

  it('gives concurrent tool calls distinct ids', async () => {
    const { logger, lines } = captureLogger();

    await Promise.all([
      runToolCall(async () => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        logger.info('slow');
      }),
      runToolCall(() => {
        logger.info('fast');
        return Promise.resolve();
      }),
    ]);

    const ids = lines.map((line) => parse(line).toolCallId);
    expect(new Set(ids).size).toBe(2);
  });
});

describe('logTiming', () => {
  it('logs fields inside a tool call and stays quiet outside one', async () => {
    const { logger, lines } = captureLogger();

    logTiming(logger, 'kafka request', { operation: 'consumer_lag', durationMs: 5 });
    expect(lines).toHaveLength(0);

    await runToolCall(() => {
      logTiming(logger, 'kafka request', { operation: 'consumer_lag', durationMs: 5 });
      return Promise.resolve();
    });

    expect(lines).toHaveLength(1);
    expect(parse(lines[0])).toMatchObject({
      msg: 'kafka request',
      operation: 'consumer_lag',
      durationMs: 5,
    });
  });
});
