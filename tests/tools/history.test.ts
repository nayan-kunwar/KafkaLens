import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { LagStore } from '../../src/persistence/lag-store.js';
import { createGetLagHistoryHandler } from '../../src/tools/history.js';

const logger = pino({ level: 'silent' });

function fakeStore(overrides: Partial<LagStore>): LagStore {
  return overrides as unknown as LagStore;
}

function parseResult(result: CallToolResult): { isError: boolean; data: unknown } {
  const block = result.content[0];
  const text = block !== undefined && block.type === 'text' ? block.text : 'null';
  return { isError: result.isError === true, data: JSON.parse(text) };
}

describe('get_lag_history handler', () => {
  it('passes the query through and wraps observations with metadata', async () => {
    let received: unknown;
    const store = fakeStore({
      queryHistory: (query) => {
        received = query;
        return Promise.resolve({
          observations: [
            {
              partition: 0,
              observedAt: '2026-01-02T00:00:00.000Z',
              highWatermark: '10',
              committedOffset: '4',
              lag: '6',
            },
          ],
          truncated: false,
        });
      },
    });

    const handler = createGetLagHistoryHandler(store, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'g', topic: 't', limit: 100 }));
    expect(received).toMatchObject({ groupId: 'g', topic: 't', limit: 100 });
    expect(isError).toBe(false);
    expect(data).toMatchObject({
      groupId: 'g',
      topic: 't',
      truncated: false,
    });
    expect((data as { observations: unknown[] }).observations).toHaveLength(1);
  });

  it('returns STORE_UNAVAILABLE as isError content', async () => {
    const store = fakeStore({
      queryHistory: () => Promise.reject(new AppError('STORE_UNAVAILABLE', 'down')),
    });

    const handler = createGetLagHistoryHandler(store, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'g', topic: 't', limit: 100 }));
    expect(isError).toBe(true);
    expect(data).toMatchObject({ code: 'STORE_UNAVAILABLE' });
  });

  it('wraps unexpected errors as INTERNAL', async () => {
    const store = fakeStore({
      queryHistory: () => Promise.reject(new Error('boom')),
    });

    const handler = createGetLagHistoryHandler(store, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'g', topic: 't', limit: 100 }));
    expect(isError).toBe(true);
    expect(data).toEqual({ code: 'INTERNAL', message: 'Tool failed' });
  });
});
