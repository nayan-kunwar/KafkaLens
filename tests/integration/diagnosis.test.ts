import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Consumer, Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { pino } from 'pino';
import { Wait } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { loadEnv } from '../../src/config/env.js';
import { IncidentStore } from '../../src/persistence/incident-store.js';
import { LagStore } from '../../src/persistence/lag-store.js';
import { AdminService } from '../../src/services/admin-service.js';
import { ConsumerGroupsService } from '../../src/services/consumer-groups-service.js';
import { CorrelationService } from '../../src/services/correlation-service.js';
import { DiagnosisService } from '../../src/services/diagnosis-service.js';
import { KafkaAdminSession } from '../../src/services/kafka-admin-session.js';
import { createKafka } from '../../src/services/kafka-client.js';
import { OffsetsService } from '../../src/services/offsets-service.js';

const PLAINTEXT_IMAGE = 'confluentinc/cp-kafka:7.7.1';
const TOPIC = 'diag-events';
const STALL_GROUP = 'diag-stalled';
const UNCOMMITTED_GROUP = 'diag-uncommitted';

const silentLogger = () => pino({ level: 'silent' });

function envFor(brokers: string): NodeJS.ProcessEnv {
  return { NODE_ENV: 'test', KAFKA_BROKERS: brokers, KAFKA_RETRY_ATTEMPTS: '5' };
}

async function waitForAssignment(groups: ConsumerGroupsService, groupId: string): Promise<void> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 40; attempt += 1) {
    try {
      const current = await groups.getConsumerAssignments(groupId);
      if (current.assignments.some((row) => (row.assignment?.length ?? 0) > 0)) return;
    } catch (err) {
      lastError = err;
    }
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }
  throw lastError instanceof Error ? lastError : new Error(`group ${groupId} did not join`);
}

describe('diagnose_consumer_group', () => {
  let kafkaContainer: StartedKafkaContainer;
  let pgContainer: StartedPostgreSqlContainer;
  let pool: Pool;
  let lagStore: LagStore;
  let incidentStore: IncidentStore;
  let kafkaClient: Kafka;
  let session: KafkaAdminSession;
  let admin: AdminService;
  let diagnosis: DiagnosisService;
  let groups: ConsumerGroupsService;
  let stalled: Consumer;
  let uncommitted: Consumer;

  beforeAll(async () => {
    kafkaContainer = await new KafkaContainer(PLAINTEXT_IMAGE)
      .withWaitStrategy(Wait.forLogMessage(/\[KafkaServer id=1\] started/, 1))
      .start();
    pgContainer = await new PostgreSqlContainer('postgres:16-alpine').start();
    const brokers = `${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`;
    const logger = silentLogger();
    const kafka = createKafka(loadEnv(envFor(brokers)), logger);
    kafkaClient = kafka;
    pool = new Pool({ connectionString: pgContainer.getConnectionUri() });
    lagStore = new LagStore(pool);
    incidentStore = new IncidentStore(pool);
    await lagStore.ensureSchema();

    const setup = kafka.admin();
    await setup.connect();
    try {
      await setup.createTopics({
        waitForLeaders: true,
        topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }],
      });
      const producer = kafka.producer();
      await producer.connect();
      try {
        await producer.send({
          topic: TOPIC,
          messages: Array.from({ length: 20 }, (_, index) => ({
            value: `diag-${index}`,
            partition: 0,
          })),
        });
      } finally {
        await producer.disconnect().catch(() => undefined);
      }
      await setup.setOffsets({
        groupId: STALL_GROUP,
        topic: TOPIC,
        partitions: [{ partition: 0, offset: '4' }],
      });
    } finally {
      await setup.disconnect().catch(() => undefined);
    }

    stalled = kafka.consumer({ groupId: STALL_GROUP });
    await stalled.connect();
    await stalled.subscribe({ topic: TOPIC, fromBeginning: false });
    await stalled.run({ autoCommit: false, eachMessage: () => Promise.resolve() });

    uncommitted = kafka.consumer({ groupId: UNCOMMITTED_GROUP });
    await uncommitted.connect();
    await uncommitted.subscribe({ topic: TOPIC, fromBeginning: true });
    await uncommitted.run({ autoCommit: false, eachMessage: () => Promise.resolve() });

    session = new KafkaAdminSession(kafka);
    admin = new AdminService(kafka, { session, logger });
    groups = new ConsumerGroupsService(kafka, { session, logger });
    const offsets = new OffsetsService(kafka, groups, lagStore, logger, undefined, session);
    diagnosis = new DiagnosisService(
      admin,
      groups,
      offsets,
      lagStore,
      incidentStore,
      new CorrelationService({ logger }),
      { imbalanceRatio: 5, stallWindowSeconds: 60, logger },
    );
    await waitForAssignment(groups, STALL_GROUP);
    await waitForAssignment(groups, UNCOMMITTED_GROUP);

    await lagStore.recordBatch([
      {
        clusterId: 'integration',
        topic: TOPIC,
        consumerGroup: STALL_GROUP,
        partition: 0,
        highWatermark: '20',
        committedOffset: '4',
        lag: '16',
        observedAt: new Date(Date.now() - 50_000).toISOString(),
      },
    ]);
  }, 300_000);

  afterAll(async () => {
    await stalled?.disconnect().catch(() => undefined);
    await uncommitted?.disconnect().catch(() => undefined);
    await session?.close();
    await pool?.end();
    await pgContainer?.stop();
    await kafkaContainer?.stop();
  });

  it('reports a stalled assigned group when the committed offset does not move', async () => {
    const report = await diagnosis.diagnoseConsumerGroup(STALL_GROUP, TOPIC);
    expect(report.groupId).toBe(STALL_GROUP);
    expect(report.assignmentsKnown).toBe(true);
    expect(report.group.memberCount).toBe(1);
    expect(report.findings.map((finding) => finding.type)).toContain('CONSUMER_STALLED');
    expect(report.stall.groupProgress).toBe('ASSIGNED_NOT_PROGRESSING');
    const partition = report.partitions.find((row) => row.partitionId === 0);
    expect(partition?.committedOffset).toBe('4');
    expect(partition?.lag).not.toBe('0');
    expect(partition?.lag).not.toBeNull();
  });

  it('reports a missing committed offset without storing it as zero', async () => {
    const report = await diagnosis.diagnoseConsumerGroup(UNCOMMITTED_GROUP, TOPIC);
    const finding = report.findings.find((entry) => entry.type === 'NO_COMMITTED_OFFSET');
    expect(finding?.evidence.join(' ')).toMatch(/not treated as zero/);
    const partition = report.partitions.find((row) => row.partitionId === 0);
    expect(partition?.committedOffset === null || partition?.committedOffset === '-1').toBe(true);
    expect(partition?.lag).toBeNull();
    expect(partition?.unknownReason).toBe('NO_COMMITTED_OFFSET');
  });

  it('still returns the Kafka diagnosis when PostgreSQL is down', async () => {
    const broken = new Pool({
      connectionString: 'postgresql://postgres:postgres@127.0.0.1:1/kafka_inspector',
      connectionTimeoutMillis: 1_000,
    });
    const offline = new DiagnosisService(
      admin,
      groups,
      new OffsetsService(
        kafkaClient,
        groups,
        new LagStore(broken),
        silentLogger(),
        undefined,
        session,
      ),
      new LagStore(broken),
      new IncidentStore(broken),
      new CorrelationService({ logger: silentLogger() }),
      { imbalanceRatio: 5, stallWindowSeconds: 60, logger: silentLogger() },
    );
    try {
      const report = await offline.diagnoseConsumerGroup(STALL_GROUP, TOPIC);
      expect(report.groupId).toBe(STALL_GROUP);
      expect(report.findings.length).toBeGreaterThan(0);
      expect(report.partialFailures.map((failure) => failure.source)).toContain('lag_history');
      expect(JSON.stringify(report)).not.toContain('postgres:postgres');
    } finally {
      await broken.end();
    }
  });
});
