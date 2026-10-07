import kafkajs from 'kafkajs';
import { describe, expect, it } from 'vitest';

const { Kafka } = kafkajs;

import { loadEnv } from '../../src/config/env.js';
import { buildKafkaConfig, createKafka, parseBrokers } from '../../src/services/kafka-client.js';

function env(extra: NodeJS.ProcessEnv = {}) {
  return loadEnv({ KAFKA_BROKERS: 'broker-1:9092,broker-2:9092', ...extra });
}

describe('parseBrokers', () => {
  it('splits and trims broker lists', () => {
    expect(parseBrokers(' b1:9092 , b2:9092 ')).toEqual(['b1:9092', 'b2:9092']);
  });

  it('falls back to localhost when the list is empty', () => {
    expect(parseBrokers(' , ')).toEqual(['localhost:9092']);
  });
});

describe('buildKafkaConfig', () => {
  it('builds a PLAINTEXT config without sasl or ssl', () => {
    const config = buildKafkaConfig(env());
    expect(config.brokers).toEqual(['broker-1:9092', 'broker-2:9092']);
    expect(config.clientId).toBe('kafka-inspector');
    expect(config.sasl).toBeUndefined();
    expect(config.ssl).toBeUndefined();
  });

  it('applies timeouts and retry settings from env', () => {
    const config = buildKafkaConfig(
      env({
        KAFKA_REQUEST_TIMEOUT_MS: '5000',
        KAFKA_CONNECTION_TIMEOUT_MS: '2000',
        KAFKA_RETRY_ATTEMPTS: '2',
      }),
    );
    expect(config.requestTimeout).toBe(5000);
    expect(config.connectionTimeout).toBe(2000);
    expect(config.retry).toEqual({ retries: 2 });
  });

  it('builds an SSL config with rejectUnauthorized', () => {
    const config = buildKafkaConfig(
      env({ KAFKA_SECURITY_PROTOCOL: 'SSL', KAFKA_SSL_REJECT_UNAUTHORIZED: 'false' }),
    );
    expect(config.ssl).toEqual({ rejectUnauthorized: false });
    expect(config.sasl).toBeUndefined();
  });

  it('builds a SASL_PLAINTEXT config with plain mechanism', () => {
    const config = buildKafkaConfig(
      env({
        KAFKA_SECURITY_PROTOCOL: 'SASL_PLAINTEXT',
        KAFKA_SASL_USERNAME: 'alice',
        KAFKA_SASL_PASSWORD: 'alice-secret',
      }),
    );
    expect(config.sasl).toEqual({
      mechanism: 'plain',
      username: 'alice',
      password: 'alice-secret',
    });
    expect(config.ssl).toBeUndefined();
  });

  it('builds a SASL_SSL config with both sasl and ssl', () => {
    const config = buildKafkaConfig(
      env({
        KAFKA_SECURITY_PROTOCOL: 'SASL_SSL',
        KAFKA_SASL_MECHANISM: 'scram-sha-512',
        KAFKA_SASL_USERNAME: 'alice',
        KAFKA_SASL_PASSWORD: 'alice-secret',
      }),
    );
    expect(config.ssl).toEqual({ rejectUnauthorized: true });
    expect(config.sasl).toEqual({
      mechanism: 'scram-sha-512',
      username: 'alice',
      password: 'alice-secret',
    });
  });

  it('supports scram-sha-256 mechanism', () => {
    const config = buildKafkaConfig(
      env({
        KAFKA_SECURITY_PROTOCOL: 'SASL_PLAINTEXT',
        KAFKA_SASL_MECHANISM: 'scram-sha-256',
        KAFKA_SASL_USERNAME: 'alice',
        KAFKA_SASL_PASSWORD: 'alice-secret',
      }),
    );
    expect(config.sasl).toMatchObject({ mechanism: 'scram-sha-256' });
  });
});

describe('createKafka', () => {
  it('returns a KafkaJS client instance without connecting', () => {
    expect(createKafka(env())).toBeInstanceOf(Kafka);
  });
});
