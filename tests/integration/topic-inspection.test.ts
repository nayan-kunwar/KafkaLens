import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { pino } from 'pino';
import { Wait } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { loadEnv } from '../../src/config/env.js';
import { AppError } from '../../src/errors/app-error.js';
import { AdminService } from '../../src/services/admin-service.js';
import { createKafka } from '../../src/services/kafka-client.js';

const PLAINTEXT_IMAGE = 'confluentinc/cp-kafka:7.7.1';
const ORDERS_TOPIC = 'm2-orders';
const PAYMENTS_TOPIC = 'm2-payments';

const silentLogger = () => pino({ level: 'silent' });

function envFor(brokers: string): NodeJS.ProcessEnv {
  return { NODE_ENV: 'test', KAFKA_BROKERS: brokers, KAFKA_RETRY_ATTEMPTS: '5' };
}

function serviceFor(brokers: string): AdminService {
  return new AdminService(createKafka(loadEnv(envFor(brokers)), silentLogger()));
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

async function createFixtureTopics(kafka: ReturnType<typeof createKafka>): Promise<void> {
  const wanted = [
    { topic: ORDERS_TOPIC, numPartitions: 3 },
    { topic: PAYMENTS_TOPIC, numPartitions: 1 },
  ];
  let lastError: unknown;
  for (let attempt = 1; attempt <= 10; attempt++) {
    const admin = kafka.admin();
    try {
      await admin.connect();
      const existing = await admin.listTopics();
      const missing = wanted.filter((t) => !existing.includes(t.topic));
      if (missing.length > 0) {
        await admin.createTopics({ topics: missing, waitForLeaders: true });
      }
      const created = await admin.listTopics();
      if (wanted.every((t) => created.includes(t.topic))) {
        await admin.disconnect();
        return;
      }
      lastError = new Error('fixture topics missing after createTopics');
    } catch (err) {
      lastError = err;
    }
    await admin.disconnect().catch(() => undefined);
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
  throw lastError;
}

describe('topic inspection', () => {
  let container: StartedKafkaContainer;
  let brokers: string;

  beforeAll(async () => {
    container = await new KafkaContainer(PLAINTEXT_IMAGE)
      .withWaitStrategy(Wait.forLogMessage(/\[KafkaServer id=1\] started/, 1))
      .start();
    brokers = `${container.getHost()}:${container.getMappedPort(9093)}`;

    const kafka = createKafka(loadEnv(envFor(brokers)), silentLogger());
    await createFixtureTopics(kafka);

    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
      topic: ORDERS_TOPIC,
      messages: [{ value: 'm2-msg-1' }, { value: 'm2-msg-2' }],
    });
    await producer.disconnect();
  }, 300_000);

  afterAll(async () => {
    await container?.stop();
  });

  it('lists topics with filter and truncation', async () => {
    const service = serviceFor(brokers);

    const all = await service.listTopics({ limit: 100 });
    const names = all.topics.map((t) => t.name);
    expect(names).toContain(ORDERS_TOPIC);
    expect(names).toContain(PAYMENTS_TOPIC);
    expect(all.total).toBeGreaterThanOrEqual(2);
    expect(all.truncated).toBe(false);
    expect(all.topics.find((t) => t.name === ORDERS_TOPIC)?.internal).toBe(false);

    const filtered = await service.listTopics({ nameContains: 'm2-', limit: 100 });
    expect(filtered.topics.map((t) => t.name)).toEqual([ORDERS_TOPIC, PAYMENTS_TOPIC]);
    expect(filtered.total).toBe(2);

    const capped = await service.listTopics({ limit: 1 });
    expect(capped.topics).toHaveLength(1);
    expect(capped.total).toBeGreaterThanOrEqual(2);
    expect(capped.truncated).toBe(true);
  });

  it('returns metadata for a topic', async () => {
    const service = serviceFor(brokers);
    const meta = await service.getTopicMetadata(ORDERS_TOPIC);

    expect(meta.name).toBe(ORDERS_TOPIC);
    expect(meta.partitionCount).toBe(3);
    expect(meta.partitions).toHaveLength(3);
    for (const part of meta.partitions) {
      expect(part.leader).toBeGreaterThanOrEqual(0);
      expect(part.replicas).toContain(part.leader);
      expect(part.isr).toContain(part.leader);
    }
  });

  it('throws NOT_FOUND for an unknown topic', async () => {
    const service = serviceFor(brokers);
    const error = await expectAppError(service.getTopicMetadata('m2-missing'), 'NOT_FOUND');
    expect(error.message).toContain('m2-missing');
  });

  it('returns partition info with watermarks', async () => {
    const service = serviceFor(brokers);
    const info = await service.getPartitionInfo(ORDERS_TOPIC);

    expect(info.topic).toBe(ORDERS_TOPIC);
    expect(info.partitionCount).toBe(3);
    expect(info.truncated).toBe(false);
    expect(info.partitions).toHaveLength(3);
    const highTotal = info.partitions.reduce(
      (sum, part) => sum + Number(part.highWatermark ?? '0'),
      0,
    );
    expect(highTotal).toBe(2);
    for (const part of info.partitions) {
      expect(part.highWatermark).not.toBeNull();
      expect(part.lowWatermark).toBe('0');
      expect(part.leader).toBeGreaterThanOrEqual(0);
    }
  });

  it('returns a single partition on request', async () => {
    const service = serviceFor(brokers);
    const single = await service.getPartitionInfo(ORDERS_TOPIC, 1);
    expect(single.partitions).toHaveLength(1);
    expect(single.partitions[0]?.partitionId).toBe(1);
  });

  it('reports zero watermarks for an empty topic', async () => {
    const service = serviceFor(brokers);
    const info = await service.getPartitionInfo(PAYMENTS_TOPIC);
    expect(info.partitionCount).toBe(1);
    expect(info.partitions[0]?.highWatermark).toBe('0');
    expect(info.partitions[0]?.lowWatermark).toBe('0');
  });

  it('throws NOT_FOUND for out-of-range partition and unknown topic', async () => {
    const service = serviceFor(brokers);
    await expectAppError(service.getPartitionInfo(ORDERS_TOPIC, 99), 'NOT_FOUND');
    await expectAppError(service.getPartitionInfo('m2-missing'), 'NOT_FOUND');
  });
});
