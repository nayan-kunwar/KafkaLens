import type { Kafka } from 'kafkajs';
import kafkajs from 'kafkajs';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type {
  ConsumerGroupResult,
  ConsumerGroupsService,
} from '../../src/services/consumer-groups-service.js';
import {
  OffsetsService,
  type GroupLagResult,
  type TopicLagResult,
} from '../../src/services/offsets-service.js';

interface StubAdmin {
  fetchTopicMetadata?: (options?: { topics: string[] }) => Promise<{ topics: unknown[] }>;
  fetchTopicOffsets?: (
    topic: string,
  ) => Promise<{ partition: number; high: string; low: string }[]>;
  fetchOffsets?: (options: {
    groupId: string;
    topics?: string[];
  }) => Promise<{ topic: string; partitions: { partition: number; offset: string }[] }[]>;
}

function serviceWith(stub: StubAdmin, groupState = 'Stable'): OffsetsService {
  const admin = {
    connect: () => Promise.resolve(),
    disconnect: () => Promise.resolve(),
    ...stub,
  };
  const kafka = { admin: () => admin };
  const consumerGroups = {
    getConsumerGroup: (groupId: string) =>
      Promise.resolve({
        groupId,
        state: groupState,
        protocolType: 'consumer',
        protocol: 'range',
        memberCount: 1,
        members: [],
        truncated: false,
        observedAt: '2026-01-01T00:00:00.000Z',
      } satisfies ConsumerGroupResult),
  };
  return new OffsetsService(
    kafka as unknown as Kafka,
    consumerGroups as unknown as ConsumerGroupsService,
  );
}

function meta(topic: string, partitions: number[]): { topics: unknown[] } {
  return {
    topics: [{ name: topic, partitions: partitions.map((partitionId) => ({ partitionId })) }],
  };
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

const GROUP = 'lag-worker';
const TOPIC = 'lag-orders';

function topicStubs(): StubAdmin {
  return {
    fetchTopicMetadata: () => Promise.resolve(meta(TOPIC, [0, 1, 2])),
    fetchTopicOffsets: () =>
      Promise.resolve([
        { partition: 0, high: '100', low: '0' },
        { partition: 1, high: '50', low: '0' },
        { partition: 2, high: '0', low: '0' },
      ]),
    fetchOffsets: () =>
      Promise.resolve([
        {
          topic: TOPIC,
          partitions: [
            { partition: 0, offset: '40' },
            { partition: 1, offset: '-1' },
          ],
        },
      ]),
  };
}

describe('getConsumerLag for one topic', () => {
  it('joins watermarks with commits and sums known lags only', async () => {
    const result = (await serviceWith(topicStubs()).getConsumerLag(GROUP, TOPIC)) as TopicLagResult;
    expect(result.groupId).toBe(GROUP);
    expect(result.topic).toBe(TOPIC);
    expect(result.groupState).toBe('Stable');
    expect(result.partitionCount).toBe(3);
    expect(result.partitions).toEqual([
      {
        partitionId: 0,
        highWatermark: '100',
        committedOffset: '40',
        lag: '60',
      },
      {
        partitionId: 1,
        highWatermark: '50',
        committedOffset: '-1',
        lag: null,
        unknownReason: 'NO_COMMITTED_OFFSET',
      },
      {
        partitionId: 2,
        highWatermark: '0',
        committedOffset: null,
        lag: null,
        unknownReason: 'NO_COMMITTED_OFFSET',
      },
    ]);
    expect(result.totalLag).toBe('60');
    expect(result.unknownPartitionCount).toBe(2);
    expect(result.orphanedCommits).toEqual([]);
    expect(result.truncated).toBe(false);
  });

  it('flags clamped negative lag instead of reporting it', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve(meta(TOPIC, [0])),
      fetchTopicOffsets: () => Promise.resolve([{ partition: 0, high: '50', low: '0' }]),
      fetchOffsets: () =>
        Promise.resolve([{ topic: TOPIC, partitions: [{ partition: 0, offset: '80' }] }]),
    });
    const result = (await service.getConsumerLag(GROUP, TOPIC)) as TopicLagResult;
    expect(result.partitions[0]).toMatchObject({ lag: '0', adjusted: true });
    expect(result.totalLag).toBe('0');
  });

  it('reports commits for deleted partitions as orphans, not lag rows', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve(meta(TOPIC, [0])),
      fetchTopicOffsets: () => Promise.resolve([{ partition: 0, high: '10', low: '0' }]),
      fetchOffsets: () =>
        Promise.resolve([
          {
            topic: TOPIC,
            partitions: [
              { partition: 0, offset: '4' },
              { partition: 7, offset: '9' },
            ],
          },
        ]),
    });
    const result = (await service.getConsumerLag(GROUP, TOPIC)) as TopicLagResult;
    expect(result.partitions).toHaveLength(1);
    expect(result.totalLag).toBe('6');
    expect(result.orphanedCommits).toEqual([{ partition: 7, committedOffset: '9' }]);
  });

  it('reports every partition unknown for a group with no commits', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve(meta(TOPIC, [0, 1])),
      fetchTopicOffsets: () =>
        Promise.resolve([
          { partition: 0, high: '0', low: '0' },
          { partition: 1, high: '0', low: '0' },
        ]),
      fetchOffsets: () => Promise.resolve([]),
    });
    const result = (await service.getConsumerLag(GROUP, TOPIC)) as TopicLagResult;
    expect(result.totalLag).toBeNull();
    expect(result.unknownPartitionCount).toBe(2);
  });

  it('throws NOT_FOUND for an unknown topic', async () => {
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve({ topics: [] }),
    });
    const error = await expectAppError(service.getConsumerLag(GROUP, 'missing'), 'NOT_FOUND');
    expect(error.message).toContain('missing');
  });

  it('propagates NOT_FOUND for an unknown group without touching Kafka offsets', async () => {
    let calls = 0;
    const admin = {
      connect: () => Promise.resolve(),
      disconnect: () => Promise.resolve(),
      fetchTopicMetadata: () => {
        calls += 1;
        return Promise.resolve(meta(TOPIC, [0]));
      },
    };
    const consumerGroups = {
      getConsumerGroup: () => Promise.reject(new AppError('NOT_FOUND', 'no such group')),
    };
    const service = new OffsetsService(
      { admin: () => admin } as unknown as Kafka,
      consumerGroups as unknown as ConsumerGroupsService,
    );
    await expectAppError(service.getConsumerLag('missing', TOPIC), 'NOT_FOUND');
    expect(calls).toBe(0);
  });

  it('caps partitions at 500 and reports truncation', async () => {
    const ids = Array.from({ length: 501 }, (_, i) => i);
    const service = serviceWith({
      fetchTopicMetadata: () => Promise.resolve(meta(TOPIC, ids)),
      fetchTopicOffsets: () =>
        Promise.resolve(ids.map((partition) => ({ partition, high: '1', low: '0' }))),
      fetchOffsets: () => Promise.resolve([]),
    });
    const result = (await service.getConsumerLag(GROUP, TOPIC)) as TopicLagResult;
    expect(result.partitionCount).toBe(501);
    expect(result.partitions).toHaveLength(500);
    expect(result.truncated).toBe(true);
  });
});

describe('getConsumerLag for a whole group', () => {
  function groupStubs(): StubAdmin {
    return {
      fetchTopicOffsets: (topic: string) => {
        if (topic === 'a') {
          return Promise.resolve([
            { partition: 0, high: '10', low: '0' },
            { partition: 1, high: '20', low: '0' },
          ]);
        }
        return Promise.resolve([{ partition: 0, high: '5', low: '0' }]);
      },
      fetchOffsets: () =>
        Promise.resolve([
          {
            topic: 'a',
            partitions: [
              { partition: 0, offset: '7' },
              { partition: 1, offset: '-1' },
            ],
          },
          { topic: 'b', partitions: [{ partition: 0, offset: '5' }] },
        ]),
    };
  }

  it('returns one bounded summary row per committed topic', async () => {
    const result = (await serviceWith(groupStubs()).getConsumerLag(GROUP)) as GroupLagResult;
    expect(result.groupId).toBe(GROUP);
    expect(result.groupState).toBe('Stable');
    expect(result.topics).toEqual([
      { topic: 'a', partitionCount: 2, totalLag: '3', unknownPartitions: 1 },
      { topic: 'b', partitionCount: 1, totalLag: '0', unknownPartitions: 0 },
    ]);
    expect(result.topicCount).toBe(2);
    expect(result.truncated).toBe(false);
  });

  it('keeps deleted topics visible as unknown instead of failing', async () => {
    const { KafkaJSProtocolError } = kafkajs;
    const unknownTopic = new KafkaJSProtocolError(
      Object.assign(new Error('unknown topic'), { type: 'UNKNOWN_TOPIC_OR_PARTITION', code: 3 }),
    );
    const service = serviceWith({
      fetchTopicOffsets: () => Promise.reject(unknownTopic),
      fetchOffsets: () =>
        Promise.resolve([{ topic: 'gone', partitions: [{ partition: 0, offset: '3' }] }]),
    });
    const result = (await service.getConsumerLag(GROUP)) as GroupLagResult;
    expect(result.topics).toEqual([
      {
        topic: 'gone',
        partitionCount: 1,
        totalLag: null,
        unknownPartitions: 1,
        topicMissing: true,
      },
    ]);
  });

  it('returns an empty summary for a group with no commits', async () => {
    const service = serviceWith({ fetchOffsets: () => Promise.resolve([]) });
    const result = (await service.getConsumerLag(GROUP)) as GroupLagResult;
    expect(result).toMatchObject({ topics: [], topicCount: 0, truncated: false });
  });
});
