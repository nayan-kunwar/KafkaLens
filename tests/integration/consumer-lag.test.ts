import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { pino } from 'pino';
import { Wait } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { loadEnv } from '../../src/config/env.js';
import { AppError } from '../../src/errors/app-error.js';
import { LagStore } from '../../src/persistence/lag-store.js';
import { ConsumerGroupsService } from '../../src/services/consumer-groups-service.js';
import { createKafka } from '../../src/services/kafka-client.js';
import {
  OffsetsService,
  type GroupLagResult,
  type TopicLagResult,
} from '../../src/services/offsets-service.js';

const PLAINTEXT_IMAGE = 'confluentinc/cp-kafka:7.7.1';
const TOPIC = 'm4-lag';
const GROUP = 'm4-lag-worker';
const FRESH_GROUP = 'm4-fresh-worker';

const silentLogger = () => pino({ level: 'silent' });

function envFor(brokers: string): NodeJS.ProcessEnv {
  return { NODE_ENV: 'test', KAFKA_BROKERS: brokers, KAFKA_RETRY_ATTEMPTS: '5' };
}

function serviceFor(brokers: string, lagStore?: LagStore): OffsetsService {
  const kafka = createKafka(loadEnv(envFor(brokers)), silentLogger());
  return new OffsetsService(kafka, new ConsumerGroupsService(kafka), lagStore);
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

describe('consumer lag', () => {
  let container: StartedKafkaContainer;
  let brokers: string;
  let pgContainer: StartedPostgreSqlContainer;
  let lagStore: LagStore;

  beforeAll(async () => {
    container = await new KafkaContainer(PLAINTEXT_IMAGE)
      .withWaitStrategy(Wait.forLogMessage(/\[KafkaServer id=1\] started/, 1))
      .start();
    brokers = `${container.getHost()}:${container.getMappedPort(9093)}`;
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    lagStore = new LagStore(new Pool({ connectionString: pgContainer.getConnectionUri() }));
    await lagStore.ensureSchema();

    const kafka = createKafka(loadEnv(envFor(brokers)), silentLogger());
    const admin = kafka.admin();
    await admin.connect();
    try {
      const existing = await admin.listTopics();
      if (!existing.includes(TOPIC)) {
        await admin.createTopics({
          topics: [{ topic: TOPIC, numPartitions: 2 }],
          waitForLeaders: true,
        });
      }
    } finally {
      await admin.disconnect().catch(() => undefined);
    }

    const producer = kafka.producer();
    await producer.connect();
    try {
      await producer.send({
        topic: TOPIC,
        messages: Array.from({ length: 10 }, (_, i) => ({ value: `m4-${i}`, partition: 0 })),
      });
    } finally {
      await producer.disconnect().catch(() => undefined);
    }

    const seeder = kafka.admin();
    await seeder.connect();
    try {
      await seeder.setOffsets({
        groupId: GROUP,
        topic: TOPIC,
        partitions: [{ partition: 0, offset: '4' }],
      });
    } finally {
      await seeder.disconnect().catch(() => undefined);
    }
  }, 300_000);

  afterAll(async () => {
    await lagStore?.close();
    await pgContainer?.stop();
    await container?.stop();
  });

  it('computes per-partition lag and totals known partitions only', async () => {
    const service = serviceFor(brokers);
    const result = (await service.getConsumerLag(GROUP, TOPIC)) as TopicLagResult;

    expect(result.groupId).toBe(GROUP);
    expect(result.topic).toBe(TOPIC);
    expect(result.partitionCount).toBe(2);
    expect(result.partitions).toEqual([
      {
        partitionId: 0,
        highWatermark: '10',
        committedOffset: '4',
        lag: '6',
      },
      {
        partitionId: 1,
        highWatermark: '0',
        committedOffset: '-1',
        lag: null,
        unknownReason: 'NO_COMMITTED_OFFSET',
      },
    ]);
    expect(result.totalLag).toBe('6');
    expect(result.unknownPartitionCount).toBe(1);
    expect(result.orphanedCommits).toEqual([]);
    expect(result.truncated).toBe(false);
  });

  it('returns a group-wide summary across committed topics', async () => {
    const service = serviceFor(brokers);
    const result = (await service.getConsumerLag(GROUP)) as GroupLagResult;

    expect(result.groupId).toBe(GROUP);
    const row = result.topics.find((entry) => entry.topic === TOPIC);
    expect(row).toMatchObject({ partitionCount: 2, totalLag: '6', unknownPartitions: 1 });
    expect(result.topicCount).toBeGreaterThanOrEqual(1);
  });

  it('reports unknown lag for a joined group with no commits', async () => {
    const kafka = createKafka(loadEnv(envFor(brokers)), silentLogger());
    const consumer = kafka.consumer({ groupId: FRESH_GROUP });
    await consumer.connect();
    try {
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      await consumer.run({ autoCommit: false, eachMessage: () => Promise.resolve() });

      const service = serviceFor(brokers);
      let joined = false;
      for (let attempt = 0; attempt < 40; attempt++) {
        try {
          const described = (await service.getConsumerLag(FRESH_GROUP, TOPIC)) as TopicLagResult;
          if (described.groupState === 'Stable' && described.partitions.length === 2) {
            joined = true;
            break;
          }
        } catch {
          // Group is still forming (Dead/unknown) — keep polling.
        }
        await new Promise((resolve) => setTimeout(resolve, 1500));
      }
      expect(joined).toBe(true);

      const result = (await service.getConsumerLag(FRESH_GROUP, TOPIC)) as TopicLagResult;
      expect(result.totalLag).toBeNull();
      expect(result.unknownPartitionCount).toBe(2);
      expect(result.partitions.map((row) => row.lag)).toEqual([null, null]);
      expect(result.partitions[0]?.highWatermark).toBe('10');
    } finally {
      await consumer.disconnect().catch(() => undefined);
    }
  });

  it('persists observations to lag history on every lag call', async () => {
    const service = serviceFor(brokers, lagStore);
    await service.getConsumerLag(GROUP, TOPIC);

    const partitionZero = await lagStore.queryHistory({
      groupId: GROUP,
      topic: TOPIC,
      partition: 0,
      limit: 10,
    });
    expect(partitionZero.observations[0]).toMatchObject({
      highWatermark: '10',
      committedOffset: '4',
      lag: '6',
    });

    const partitionOne = await lagStore.queryHistory({
      groupId: GROUP,
      topic: TOPIC,
      partition: 1,
      limit: 10,
    });
    expect(partitionOne.observations[0]).toMatchObject({
      highWatermark: '0',
      lag: null,
    });
  });

  it('throws NOT_FOUND for unknown group and unknown topic', async () => {
    const service = serviceFor(brokers);
    const error = await expectAppError(service.getConsumerLag('m4-missing', TOPIC), 'NOT_FOUND');
    expect(error.message).toContain('m4-missing');
    await expectAppError(service.getConsumerLag(GROUP, 'm4-missing-topic'), 'NOT_FOUND');
  });
});
