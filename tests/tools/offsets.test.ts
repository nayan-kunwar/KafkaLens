import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { OffsetsService } from '../../src/services/offsets-service.js';
import { createGetConsumerLagHandler } from '../../src/tools/offsets.js';

const logger = pino({ level: 'silent' });

function fakeService(overrides: Partial<OffsetsService>): OffsetsService {
  return overrides as unknown as OffsetsService;
}

function parseResult(result: CallToolResult): { isError: boolean; data: unknown } {
  const block = result.content[0];
  const text = block !== undefined && block.type === 'text' ? block.text : 'null';
  return { isError: result.isError === true, data: JSON.parse(text) };
}

describe('get_consumer_lag handler', () => {
  it('passes groupId and topic through and returns JSON content', async () => {
    let received: { groupId: string; topic?: string | undefined } | undefined;
    const service = fakeService({
      getConsumerLag: (groupId: string, topic?: string) => {
        received = topic === undefined ? { groupId } : { groupId, topic };
        return Promise.resolve({
          groupId,
          topic: topic ?? 'orders',
          groupState: 'Stable',
          partitionCount: 1,
          partitions: [],
          totalLag: '0',
          unknownPartitionCount: 0,
          orphanedCommits: [],
          truncated: false,
          observedAt: '2026-01-01T00:00:00.000Z',
        });
      },
    });

    const handler = createGetConsumerLagHandler(service, logger);
    const { isError, data } = parseResult(
      await handler({ groupId: 'orders-worker', topic: 'orders' }),
    );
    expect(received).toEqual({ groupId: 'orders-worker', topic: 'orders' });
    expect(isError).toBe(false);
    expect(data).toMatchObject({ groupId: 'orders-worker', totalLag: '0' });
  });

  it('returns NOT_FOUND errors as isError content', async () => {
    const service = fakeService({
      getConsumerLag: () =>
        Promise.reject(new AppError('NOT_FOUND', 'Consumer group "missing" not found')),
    });

    const handler = createGetConsumerLagHandler(service, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'missing' }));
    expect(isError).toBe(true);
    expect(data).toMatchObject({ code: 'NOT_FOUND' });
  });

  it('wraps unexpected errors as INTERNAL', async () => {
    const service = fakeService({
      getConsumerLag: () => Promise.reject(new Error('boom')),
    });

    const handler = createGetConsumerLagHandler(service, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'g' }));
    expect(isError).toBe(true);
    expect(data).toEqual({ code: 'INTERNAL', message: 'Tool failed' });
  });
});
