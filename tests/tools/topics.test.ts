import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { AdminService } from '../../src/services/admin-service.js';
import {
  createGetPartitionInfoHandler,
  createGetTopicMetadataHandler,
  createListTopicsHandler,
} from '../../src/tools/topics.js';

const logger = pino({ level: 'silent' });

function fakeService(overrides: Partial<AdminService>): AdminService {
  return overrides as unknown as AdminService;
}

function parseResult(result: CallToolResult): { isError: boolean; data: unknown } {
  const block = result.content[0];
  const text = block !== undefined && block.type === 'text' ? block.text : 'null';
  return { isError: result.isError === true, data: JSON.parse(text) };
}

describe('list_topics handler', () => {
  it('returns JSON content on success', async () => {
    const service = fakeService({
      listTopics: () =>
        Promise.resolve({
          topics: [{ name: 'orders', internal: false }],
          total: 1,
          truncated: false,
          observedAt: '2026-01-01T00:00:00.000Z',
        }),
    });

    const handler = createListTopicsHandler(service, logger);
    const { isError, data } = parseResult(await handler({ limit: 100 }));
    expect(isError).toBe(false);
    expect(data).toMatchObject({ total: 1, truncated: false });
  });

  it('returns structured error content on failure', async () => {
    const service = fakeService({
      listTopics: () => Promise.reject(new AppError('KAFKA_UNAVAILABLE', 'down')),
    });

    const handler = createListTopicsHandler(service, logger);
    const { isError, data } = parseResult(await handler({ limit: 100 }));
    expect(isError).toBe(true);
    expect(data).toEqual({ code: 'KAFKA_UNAVAILABLE', message: 'down' });
  });
});

describe('get_topic_metadata handler', () => {
  it('passes the topic argument through', async () => {
    let received: string | undefined;
    const service = fakeService({
      getTopicMetadata: (topic: string) => {
        received = topic;
        return Promise.resolve({
          name: topic,
          partitionCount: 1,
          partitions: [{ partitionId: 0, leader: 0, replicas: [0], isr: [0] }],
          observedAt: '2026-01-01T00:00:00.000Z',
        });
      },
    });

    const handler = createGetTopicMetadataHandler(service, logger);
    const { isError, data } = parseResult(await handler({ topic: 'orders' }));
    expect(received).toBe('orders');
    expect(isError).toBe(false);
    expect(data).toMatchObject({ name: 'orders', partitionCount: 1 });
  });

  it('wraps unexpected errors as INTERNAL', async () => {
    const service = fakeService({
      getTopicMetadata: () => Promise.reject(new Error('boom')),
    });

    const handler = createGetTopicMetadataHandler(service, logger);
    const { isError, data } = parseResult(await handler({ topic: 'orders' }));
    expect(isError).toBe(true);
    expect(data).toEqual({ code: 'INTERNAL', message: 'Tool failed' });
  });
});

describe('get_partition_info handler', () => {
  it('passes topic and partition through', async () => {
    let receivedTopic: string | undefined;
    let receivedPartition: number | undefined;
    const service = fakeService({
      getPartitionInfo: (topic: string, partition?: number) => {
        receivedTopic = topic;
        receivedPartition = partition;
        return Promise.resolve({
          topic,
          partitionCount: 2,
          partitions: [],
          truncated: false,
          observedAt: '2026-01-01T00:00:00.000Z',
        });
      },
    });

    const handler = createGetPartitionInfoHandler(service, logger);
    await handler({ topic: 'orders', partition: 1 });
    expect(receivedTopic).toBe('orders');
    expect(receivedPartition).toBe(1);
  });

  it('returns NOT_FOUND errors as isError content', async () => {
    const service = fakeService({
      getPartitionInfo: () =>
        Promise.reject(new AppError('NOT_FOUND', 'Topic "missing" not found')),
    });

    const handler = createGetPartitionInfoHandler(service, logger);
    const { isError, data } = parseResult(await handler({ topic: 'missing' }));
    expect(isError).toBe(true);
    expect(data).toMatchObject({ code: 'NOT_FOUND' });
  });
});
