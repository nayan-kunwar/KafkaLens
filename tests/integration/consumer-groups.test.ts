import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { pino } from 'pino';
import { Wait } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { loadEnv } from '../../src/config/env.js';
import { AppError } from '../../src/errors/app-error.js';
import { ConsumerGroupsService } from '../../src/services/consumer-groups-service.js';
import { createKafka } from '../../src/services/kafka-client.js';

const PLAINTEXT_IMAGE = 'confluentinc/cp-kafka:7.7.1';
const TOPIC = 'm3-orders';
const GROUP = 'm3-orders-worker';

const silentLogger = () => pino({ level: 'silent' });

function envFor(brokers: string): NodeJS.ProcessEnv {
  return { NODE_ENV: 'test', KAFKA_BROKERS: brokers, KAFKA_RETRY_ATTEMPTS: '5' };
}

function serviceFor(brokers: string): ConsumerGroupsService {
  return new ConsumerGroupsService(createKafka(loadEnv(envFor(brokers)), silentLogger()));
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

describe('consumer groups', () => {
  let container: StartedKafkaContainer;
  let brokers: string;

  beforeAll(async () => {
    container = await new KafkaContainer(PLAINTEXT_IMAGE)
      .withWaitStrategy(Wait.forLogMessage(/\[KafkaServer id=1\] started/, 1))
      .start();
    brokers = `${container.getHost()}:${container.getMappedPort(9093)}`;

    const kafka = createKafka(loadEnv(envFor(brokers)), silentLogger());
    const admin = kafka.admin();
    await admin.connect();
    try {
      const existing = await admin.listTopics();
      if (!existing.includes(TOPIC)) {
        await admin.createTopics({
          topics: [{ topic: TOPIC, numPartitions: 3 }],
          waitForLeaders: true,
        });
      }
    } finally {
      await admin.disconnect().catch(() => undefined);
    }
  }, 300_000);

  afterAll(async () => {
    await container?.stop();
  });

  it('lists, describes, and reports assignments for a joined group', async () => {
    const kafka = createKafka(loadEnv(envFor(brokers)), silentLogger());
    const consumer = kafka.consumer({ groupId: GROUP });
    await consumer.connect();
    try {
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      await consumer.run({ eachMessage: () => Promise.resolve() });

      const service = serviceFor(brokers);
      let assigned = false;
      let lastError: unknown;
      for (let attempt = 0; attempt < 40; attempt++) {
        try {
          const current = await service.getConsumerAssignments(GROUP);
          if (current.assignments.some((row) => (row.assignment?.length ?? 0) > 0)) {
            assigned = true;
            break;
          }
        } catch (err) {
          lastError = err;
        }
        await new Promise((resolve) => setTimeout(resolve, 1500));
      }
      expect(lastError).toBeUndefined();
      expect(assigned).toBe(true);

      const listed = await service.listConsumerGroups({ limit: 100 });
      const row = listed.groups.find((entry) => entry.groupId === GROUP);
      expect(row).toMatchObject({ protocolType: 'consumer', memberCount: 1 });
      expect(typeof row?.state).toBe('string');

      const filtered = await service.listConsumerGroups({ groupIdContains: 'm3-', limit: 100 });
      expect(filtered.groups.map((entry) => entry.groupId)).toContain(GROUP);

      const described = await service.getConsumerGroup(GROUP);
      expect(described.groupId).toBe(GROUP);
      expect(described.memberCount).toBe(1);
      expect(described.members).toHaveLength(1);
      expect(described.truncated).toBe(false);

      const assignments = await service.getConsumerAssignments(GROUP);
      expect(assignments.groupId).toBe(GROUP);
      expect(assignments.memberCount).toBe(1);
      const topics = assignments.assignments.flatMap((entry) => entry.assignment ?? []);
      expect(topics.map((entry) => entry.topic)).toContain(TOPIC);
    } finally {
      await consumer.disconnect().catch(() => undefined);
    }
  });

  it('throws NOT_FOUND for an unknown group', async () => {
    const service = serviceFor(brokers);
    const error = await expectAppError(service.getConsumerGroup('m3-missing'), 'NOT_FOUND');
    expect(error.message).toContain('m3-missing');
    await expectAppError(service.getConsumerAssignments('m3-missing'), 'NOT_FOUND');
  });
});
