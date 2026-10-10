import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { ConsumerGroupsService } from '../../src/services/consumer-groups-service.js';
import {
  createGetConsumerAssignmentsHandler,
  createGetConsumerGroupHandler,
  createListConsumerGroupsHandler,
} from '../../src/tools/consumer-groups.js';

const logger = pino({ level: 'silent' });

function fakeService(overrides: Partial<ConsumerGroupsService>): ConsumerGroupsService {
  return overrides as unknown as ConsumerGroupsService;
}

function parseResult(result: CallToolResult): { isError: boolean; data: unknown } {
  const block = result.content[0];
  const text = block !== undefined && block.type === 'text' ? block.text : 'null';
  return { isError: result.isError === true, data: JSON.parse(text) };
}

describe('list_consumer_groups handler', () => {
  it('returns JSON content on success', async () => {
    const service = fakeService({
      listConsumerGroups: () =>
        Promise.resolve({
          groups: [
            { groupId: 'orders-worker', protocolType: 'consumer', state: 'Stable', memberCount: 1 },
          ],
          total: 1,
          truncated: false,
          observedAt: '2026-01-01T00:00:00.000Z',
        }),
    });

    const handler = createListConsumerGroupsHandler(service, logger);
    const { isError, data } = parseResult(await handler({ limit: 100 }));
    expect(isError).toBe(false);
    expect(data).toMatchObject({ total: 1, truncated: false });
  });

  it('returns structured error content on failure', async () => {
    const service = fakeService({
      listConsumerGroups: () => Promise.reject(new AppError('KAFKA_UNAVAILABLE', 'down')),
    });

    const handler = createListConsumerGroupsHandler(service, logger);
    const { isError, data } = parseResult(await handler({ limit: 100 }));
    expect(isError).toBe(true);
    expect(data).toEqual({ code: 'KAFKA_UNAVAILABLE', message: 'down' });
  });
});

describe('get_consumer_group handler', () => {
  it('passes the groupId argument through', async () => {
    let received: string | undefined;
    const service = fakeService({
      getConsumerGroup: (groupId: string) => {
        received = groupId;
        return Promise.resolve({
          groupId,
          state: 'Stable',
          protocolType: 'consumer',
          protocol: 'range',
          memberCount: 1,
          members: [{ memberId: 'm1', clientId: 'c1', clientHost: 'h1' }],
          truncated: false,
          observedAt: '2026-01-01T00:00:00.000Z',
        });
      },
    });

    const handler = createGetConsumerGroupHandler(service, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'orders-worker' }));
    expect(received).toBe('orders-worker');
    expect(isError).toBe(false);
    expect(data).toMatchObject({ groupId: 'orders-worker', memberCount: 1 });
  });

  it('wraps unexpected errors as INTERNAL', async () => {
    const service = fakeService({
      getConsumerGroup: () => Promise.reject(new Error('boom')),
    });

    const handler = createGetConsumerGroupHandler(service, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'orders-worker' }));
    expect(isError).toBe(true);
    expect(data).toEqual({ code: 'INTERNAL', message: 'Tool failed' });
  });
});

describe('get_consumer_assignments handler', () => {
  it('passes the groupId argument through', async () => {
    let received: string | undefined;
    const service = fakeService({
      getConsumerAssignments: (groupId: string) => {
        received = groupId;
        return Promise.resolve({
          groupId,
          memberCount: 1,
          assignments: [{ memberId: 'm1', assignment: [{ topic: 'orders', partitions: [0] }] }],
          truncated: false,
          observedAt: '2026-01-01T00:00:00.000Z',
        });
      },
    });

    const handler = createGetConsumerAssignmentsHandler(service, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'orders-worker' }));
    expect(received).toBe('orders-worker');
    expect(isError).toBe(false);
    expect(data).toMatchObject({ groupId: 'orders-worker' });
  });

  it('returns NOT_FOUND errors as isError content', async () => {
    const service = fakeService({
      getConsumerAssignments: () =>
        Promise.reject(new AppError('NOT_FOUND', 'Consumer group "missing" not found')),
    });

    const handler = createGetConsumerAssignmentsHandler(service, logger);
    const { isError, data } = parseResult(await handler({ groupId: 'missing' }));
    expect(isError).toBe(true);
    expect(data).toMatchObject({ code: 'NOT_FOUND' });
  });
});
