import type { Kafka } from 'kafkajs';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { AdminService } from '../../src/services/admin-service.js';

interface StubAdmin {
  listTopics?: () => Promise<string[]>;
  fetchTopicMetadata?: (options?: { topics: string[] }) => Promise<{ topics: unknown[] }>;
  fetchTopicOffsets?: (
    topic: string,
  ) => Promise<{ partition: number; high: string; low: string }[]>;
}

function serviceWith(stub: StubAdmin): AdminService {
  const admin = {
    connect: () => Promise.resolve(),
    disconnect: () => Promise.resolve(),
    ...stub,
  };
  const kafka = { admin: () => admin };
  return new AdminService(kafka as unknown as Kafka);
}

function partition(partitionId: number, leader = 0) {
  return { partitionId, leader, replicas: [0], isr: [0], errorCode: 0 };
}

async function expectAppError(promise: Promise<unknown>, code: string): Promise<AppError> {
  let thrown: unknown;
  try {
    await promise;
  } catch (err) {
    thrown = err;
  }
  expect(thrown).toBeInstanceOf(AppError);
  const error = thrown as AppError;
  expect(error.code).toBe(code);
  return error;
}

describe('listTopics', () => {
  const stub: StubAdmin = {
    listTopics: () =>
      Promise.resolve(['orders', 'payments', '__consumer_offsets', 'orders-archive']),
  };

  it('sorts results and flags internal topics', async () => {
    const result = await serviceWith(stub).listTopics({ limit: 100 });
    expect(result.topics).toEqual([
      { name: '__consumer_offsets', internal: true },
      { name: 'orders', internal: false },
      { name: 'orders-archive', internal: false },
      { name: 'payments', internal: false },
    ]);
    expect(result.total).toBe(4);
    expect(result.truncated).toBe(false);
    expect(() => new Date(result.observedAt).toISOString()).not.toThrow();
  });

  it('filters by literal substring', async () => {
    const result = await serviceWith(stub).listTopics({ nameContains: 'orders', limit: 100 });
    expect(result.topics.map((t) => t.name)).toEqual(['orders', 'orders-archive']);
    expect(result.total).toBe(2);
  });

  it('caps at limit and reports truncation', async () => {
    const result = await serviceWith(stub).listTopics({ limit: 1 });
    expect(result.topics).toHaveLength(1);
    expect(result.total).toBe(4);
    expect(result.truncated).toBe(true);
  });

  it('maps unexpected failures to KAFKA_ERROR', async () => {
    await expectAppError(
      serviceWith({ listTopics: () => Promise.reject(new Error('boom')) }).listTopics({
        limit: 100,
      }),
      'KAFKA_ERROR',
    );
  });
});

describe('getTopicMetadata', () => {
  it('maps partition metadata sorted by partitionId', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () =>
        Promise.resolve({
          topics: [{ name: 'orders', partitions: [partition(1, 0), partition(0, 0)] }],
        }),
    });
    const result = await service.getTopicMetadata('orders');
    expect(result.name).toBe('orders');
    expect(result.partitionCount).toBe(2);
    expect(result.partitions).toEqual([
      { partitionId: 0, leader: 0, replicas: [0], isr: [0] },
      { partitionId: 1, leader: 0, replicas: [0], isr: [0] },
    ]);
  });

  it('throws NOT_FOUND for an unknown topic', async () => {
    const service = serviceWith({ fetchTopicMetadata: () => Promise.resolve({ topics: [] }) });
    const error = await expectAppError(service.getTopicMetadata('missing'), 'NOT_FOUND');
    expect(error.message).toContain('missing');
  });

  it('throws NOT_FOUND when the topic has no partitions', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve({ topics: [{ name: 'empty', partitions: [] }] }),
    });
    await expectAppError(service.getTopicMetadata('empty'), 'NOT_FOUND');
  });
});

describe('getPartitionInfo', () => {
  it('merges metadata with watermarks and sorts by partitionId', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () =>
        Promise.resolve({
          topics: [
            { name: 'orders', partitions: [partition(2, 0), partition(0, 0), partition(1, 0)] },
          ],
        }),
      fetchTopicOffsets: () =>
        Promise.resolve([
          { partition: 1, high: '500', low: '0' },
          { partition: 0, high: '42', low: '7' },
        ]),
    });

    const result = await service.getPartitionInfo('orders');
    expect(result.partitionCount).toBe(3);
    expect(result.truncated).toBe(false);
    expect(result.partitions).toEqual([
      {
        partitionId: 0,
        leader: 0,
        replicas: [0],
        isr: [0],
        highWatermark: '42',
        lowWatermark: '7',
      },
      {
        partitionId: 1,
        leader: 0,
        replicas: [0],
        isr: [0],
        highWatermark: '500',
        lowWatermark: '0',
      },
      {
        partitionId: 2,
        leader: 0,
        replicas: [0],
        isr: [0],
        highWatermark: null,
        lowWatermark: null,
      },
    ]);
  });

  it('returns a single partition when requested', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () =>
        Promise.resolve({
          topics: [{ name: 'orders', partitions: [partition(0), partition(1)] }],
        }),
      fetchTopicOffsets: () =>
        Promise.resolve([
          { partition: 0, high: '1', low: '0' },
          { partition: 1, high: '2', low: '0' },
        ]),
    });

    const result = await service.getPartitionInfo('orders', 1);
    expect(result.partitions).toHaveLength(1);
    expect(result.partitions[0]?.partitionId).toBe(1);
    expect(result.partitions[0]?.highWatermark).toBe('2');
  });

  it('throws NOT_FOUND for an out-of-range partition', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () =>
        Promise.resolve({
          topics: [{ name: 'orders', partitions: [partition(0), partition(1)] }],
        }),
      fetchTopicOffsets: () => Promise.resolve([]),
    });

    const error = await expectAppError(service.getPartitionInfo('orders', 5), 'NOT_FOUND');
    expect(error.context).toMatchObject({ topic: 'orders', partition: 5, partitionCount: 2 });
  });

  it('caps rows at 500 and reports truncation', async () => {
    const partitions = Array.from({ length: 501 }, (_, i) => partition(i));
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve({ topics: [{ name: 'big', partitions }] }),
      fetchTopicOffsets: () => Promise.resolve([]),
    });

    const result = await service.getPartitionInfo('big');
    expect(result.partitionCount).toBe(501);
    expect(result.partitions).toHaveLength(500);
    expect(result.truncated).toBe(true);
    expect(result.partitions[499]?.partitionId).toBe(499);
  });

  it('throws NOT_FOUND for an unknown topic', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve({ topics: [] }),
      fetchTopicOffsets: () => Promise.resolve([]),
    });
    await expectAppError(service.getPartitionInfo('missing'), 'NOT_FOUND');
  });
});
