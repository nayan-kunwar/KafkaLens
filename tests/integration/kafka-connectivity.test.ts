import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import { pino } from 'pino';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { loadEnv } from '../../src/config/env.js';
import { AppError } from '../../src/errors/app-error.js';
import { AdminService } from '../../src/services/admin-service.js';
import { createKafka } from '../../src/services/kafka-client.js';

const PLAINTEXT_IMAGE = 'confluentinc/cp-kafka:7.7.1';
const SASL_IMAGE = 'apache/kafka:3.9.0';
const SASL_HOST_PORT = 19094;

const JAAS_CONFIG = `KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret"
  user_alice="alice-secret";
};
`;

const silentLogger = () => pino({ level: 'silent' });

function serviceFor(envSource: NodeJS.ProcessEnv): AdminService {
  const env = loadEnv(envSource);
  return new AdminService(createKafka(env, silentLogger()));
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

describe('PLAINTEXT connectivity', () => {
  let container: StartedKafkaContainer;

  beforeAll(async () => {
    container = await new KafkaContainer(PLAINTEXT_IMAGE)
      .withWaitStrategy(Wait.forLogMessage(/\[KafkaServer id=1\] started/, 1))
      .start();
  }, 300_000);

  afterAll(async () => {
    await container?.stop();
  });

  it('reports live cluster info', async () => {
    const service = serviceFor({
      NODE_ENV: 'test',
      KAFKA_BROKERS: `${container.getHost()}:${container.getMappedPort(9093)}`,
      KAFKA_RETRY_ATTEMPTS: '5',
    });

    const info = await service.getClusterInfo();
    expect(info.clusterId).toBeTruthy();
    expect(info.brokerCount).toBeGreaterThanOrEqual(1);
    expect(typeof info.controllerId).toBe('number');
    expect(() => new Date(info.observedAt).toISOString()).not.toThrow();
  });

  it('returns KAFKA_UNAVAILABLE for unreachable brokers', async () => {
    const service = serviceFor({
      NODE_ENV: 'test',
      KAFKA_BROKERS: '127.0.0.1:1',
      KAFKA_CONNECTION_TIMEOUT_MS: '1500',
      KAFKA_REQUEST_TIMEOUT_MS: '1500',
      KAFKA_RETRY_ATTEMPTS: '0',
    });

    await expectAppError(service.getClusterInfo(), 'KAFKA_UNAVAILABLE');
  });
});

describe('SASL_PLAINTEXT connectivity', () => {
  let container: StartedTestContainer;

  beforeAll(async () => {
    container = await new GenericContainer(SASL_IMAGE)
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/, 1))
      .withExposedPorts({ container: 9092, host: SASL_HOST_PORT })
      .withCopyContentToContainer([
        { content: JAAS_CONFIG, target: '/etc/kafka/jaas.conf', mode: 0o644 },
      ])
      .withEnvironment({
        KAFKA_NODE_ID: '1',
        KAFKA_PROCESS_ROLES: 'broker,controller',
        KAFKA_LISTENERS:
          'SASL_PLAINTEXT://0.0.0.0:9092,INTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093',
        KAFKA_ADVERTISED_LISTENERS: `SASL_PLAINTEXT://localhost:${SASL_HOST_PORT},INTERNAL://localhost:9094,CONTROLLER://localhost:9093`,
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
          'SASL_PLAINTEXT:SASL_PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
        KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
        KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
        KAFKA_INTER_BROKER_LISTENER_NAME: 'INTERNAL',
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
        KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
        KAFKA_SASL_ENABLED_MECHANISMS: 'PLAIN',
        KAFKA_CLUSTER_ID: '5L6g3nShT-eMCtK--X86sw',
        KAFKA_OPTS: '-Djava.security.auth.login.config=/etc/kafka/jaas.conf',
      })
      .start();
  }, 300_000);

  afterAll(async () => {
    await container?.stop();
  });

  it('authenticates with valid credentials', async () => {
    const service = serviceFor({
      NODE_ENV: 'test',
      KAFKA_BROKERS: `localhost:${SASL_HOST_PORT}`,
      KAFKA_SECURITY_PROTOCOL: 'SASL_PLAINTEXT',
      KAFKA_SASL_MECHANISM: 'plain',
      KAFKA_SASL_USERNAME: 'alice',
      KAFKA_SASL_PASSWORD: 'alice-secret',
      KAFKA_RETRY_ATTEMPTS: '5',
    });

    const info = await service.getClusterInfo();
    expect(info.clusterId).toBeTruthy();
    expect(info.brokerCount).toBe(1);
  });

  it('fails with AUTH_FAILED for wrong credentials', async () => {
    const service = serviceFor({
      NODE_ENV: 'test',
      KAFKA_BROKERS: `localhost:${SASL_HOST_PORT}`,
      KAFKA_SECURITY_PROTOCOL: 'SASL_PLAINTEXT',
      KAFKA_SASL_MECHANISM: 'plain',
      KAFKA_SASL_USERNAME: 'alice',
      KAFKA_SASL_PASSWORD: 'wrong-password',
      KAFKA_RETRY_ATTEMPTS: '1',
      KAFKA_CONNECTION_TIMEOUT_MS: '5000',
      KAFKA_REQUEST_TIMEOUT_MS: '5000',
    });

    const error = await expectAppError(service.getClusterInfo(), 'AUTH_FAILED');
    expect(error.message).not.toContain('wrong-password');
  });
});
