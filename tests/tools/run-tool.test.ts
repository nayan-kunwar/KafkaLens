import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';
import { afterEach, describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { executeTool, setToolTimeoutMs } from '../../src/tools/run-tool.js';

interface LogEntry {
  level: 'debug' | 'warn';
  fields: Record<string, unknown>;
  message: string;
}

function captureLogger(entries: LogEntry[]): Logger {
  return {
    debug: (fields: object, message: string) => {
      entries.push({ level: 'debug', fields: fields as Record<string, unknown>, message });
    },
    warn: (fields: object, message: string) => {
      entries.push({ level: 'warn', fields: fields as Record<string, unknown>, message });
    },
  } as unknown as Logger;
}

function textOf(result: CallToolResult): string {
  const block = result.content[0];
  return block !== undefined && block.type === 'text' ? block.text : '';
}

describe('executeTool', () => {
  afterEach(() => {
    setToolTimeoutMs(60_000);
  });

  it('returns an INTERNAL timeout result when the handler exceeds the deadline', async () => {
    setToolTimeoutMs(20);
    const result = await executeTool(
      captureLogger([]),
      'slow_tool',
      () => new Promise(() => undefined),
    );
    expect(result.isError).toBe(true);
    const payload = JSON.parse(textOf(result)) as { code?: unknown; message?: string };
    expect(payload.code).toBe('INTERNAL');
    expect(payload.message).toContain('timed out');
  });

  it('returns compact single-line JSON', async () => {
    const result = await executeTool(captureLogger([]), 'ok_tool', () =>
      Promise.resolve({ groupId: 'payments', lag: '0' }),
    );
    expect(textOf(result)).toBe('{"groupId":"payments","lag":"0"}');
  });

  it('logs the underlying error but returns only the structured AppError', async () => {
    const entries: LogEntry[] = [];
    const result = await executeTool(captureLogger(entries), 'broken_tool', () =>
      Promise.reject(new Error('connection reset with credentials')),
    );

    expect(result.isError).toBe(true);
    expect(textOf(result)).not.toContain('credentials');
    expect(JSON.parse(textOf(result))).toEqual({ code: 'INTERNAL', message: 'Tool failed' });

    const warn = entries.find((entry) => entry.level === 'warn');
    expect(warn?.fields.error).toMatchObject({ code: 'INTERNAL', message: 'Tool failed' });
    expect(warn?.fields.err).toBeInstanceOf(Error);
    expect((warn?.fields.err as Error).message).toBe('connection reset with credentials');
  });

  it('passes a structured AppError through unchanged', async () => {
    const error = new AppError('NOT_FOUND', 'Consumer group "missing" not found', {
      groupId: 'missing',
    });
    const result = await executeTool(captureLogger([]), 'missing_tool', () =>
      Promise.reject(error),
    );
    expect(JSON.parse(textOf(result))).toEqual(error.toJSON());
  });
});
