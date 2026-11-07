import type { Logger } from 'pino';
import { describe, expect, it } from 'vitest';

import { createShutdownHandler } from '../../src/lifecycle/shutdown.js';

type LoggedLogger = Pick<Logger, 'info' | 'error'>;

function loggerInto(entries: string[]): LoggedLogger {
  return {
    info: (fields: object, message: string) => {
      const signal = (fields as { signal?: unknown }).signal;
      entries.push(`${message}:${String(signal)}`);
    },
    error: (first: unknown, second?: string) => {
      entries.push(second ?? String(first));
    },
  } as unknown as LoggedLogger;
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('timed out waiting for shutdown');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

describe('createShutdownHandler', () => {
  it('flushes, closes, and exits once even when signaled twice', async () => {
    const entries: string[] = [];
    const shutdown = createShutdownHandler({
      logger: loggerInto(entries),
      flushHistory: () => {
        entries.push('flush');
        return Promise.resolve();
      },
      close: () => [
        Promise.resolve().then(() => {
          entries.push('close');
        }),
      ],
      exit: (code) => entries.push(`exit:${code}`),
    });

    shutdown('SIGTERM');
    shutdown('SIGINT');
    await waitFor(() => entries.includes('exit:0'));
    expect(entries).toEqual(['shutting down:SIGTERM', 'flush', 'close', 'exit:0']);
  });

  it('still closes and exits 0 when flushing and closing fail', async () => {
    const entries: string[] = [];
    const shutdown = createShutdownHandler({
      logger: loggerInto(entries),
      flushHistory: () => Promise.reject(new Error('store down')),
      close: () => [
        Promise.reject(new Error('kafka close failed')),
        Promise.resolve().then(() => {
          entries.push('closed');
        }),
      ],
      exit: (code) => entries.push(`exit:${code}`),
    });

    shutdown('SIGTERM');
    await waitFor(() => entries.includes('exit:0'));
    expect(entries).toContain('closed');
    expect(entries.filter((entry) => entry.startsWith('exit:'))).toEqual(['exit:0']);
  });

  it('forces exit 1 when shutdown exceeds the deadline', async () => {
    const entries: string[] = [];
    const shutdown = createShutdownHandler({
      logger: loggerInto(entries),
      flushHistory: () => new Promise(() => undefined),
      close: () => [],
      exit: (code) => entries.push(`exit:${code}`),
      timeoutMs: 20,
    });

    shutdown('SIGTERM');
    await waitFor(() => entries.includes('exit:1'));
    expect(entries).toContain('shutdown timed out; forcing exit');
    expect(entries.filter((entry) => entry.startsWith('exit:'))).toEqual(['exit:1']);
  });
});
