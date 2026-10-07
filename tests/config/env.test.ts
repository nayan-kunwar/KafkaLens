import { describe, expect, it } from 'vitest';

import { loadEnv } from '../../src/config/env.js';
import { AppError } from '../../src/errors/app-error.js';

const validEnv: NodeJS.ProcessEnv = {
  NODE_ENV: 'test',
  KAFKA_BROKERS: 'broker-1:9092',
  KAFKA_CLIENT_ID: 'kafka-inspector',
  KAFKA_SECURITY_PROTOCOL: 'PLAINTEXT',
  POSTGRES_URL: 'postgresql://user:SECRETPASS@db:5432/kafka_inspector',
  REDIS_URL: 'redis://localhost:6379',
  LOG_LEVEL: 'info',
  PARTITION_IMBALANCE_RATIO: '5',
  STALL_WINDOW_SECONDS: '300',
};

describe('loadEnv', () => {
  it('parses a valid environment', () => {
    const env = loadEnv(validEnv);
    expect(env.KAFKA_BROKERS).toBe('broker-1:9092');
    expect(env.PARTITION_IMBALANCE_RATIO).toBe(5);
    expect(env.STALL_WINDOW_SECONDS).toBe(300);
  });

  it('applies defaults when optional vars are missing', () => {
    const env = loadEnv({});
    expect(env.NODE_ENV).toBe('development');
    expect(env.KAFKA_BROKERS).toBe('localhost:9092');
    expect(env.LOG_LEVEL).toBe('info');
    expect(env.PARTITION_IMBALANCE_RATIO).toBe(5);
    expect(env.STALL_WINDOW_SECONDS).toBe(300);
  });

  it('throws a structured AppError on invalid values without leaking secrets', () => {
    const env = { ...validEnv, LOG_LEVEL: 'chatty' };
    let thrown: unknown;
    try {
      loadEnv(env);
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(AppError);
    const error = thrown as AppError;
    expect(error.code).toBe('CONFIG_INVALID');
    expect(error.message).toContain('LOG_LEVEL');
    expect(error.message).not.toContain('SECRETPASS');
    expect(JSON.stringify(error.toJSON())).not.toContain('SECRETPASS');
  });

  it('rejects non-positive thresholds', () => {
    const env = { ...validEnv, STALL_WINDOW_SECONDS: '0' };
    expect(() => loadEnv(env)).toThrow(AppError);
  });

  it('rejects invalid enum values for security protocol', () => {
    const env = { ...validEnv, KAFKA_SECURITY_PROTOCOL: 'KERBEROS' };
    let thrown: unknown;
    try {
      loadEnv(env);
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(AppError);
    expect((thrown as AppError).context?.fields).toBeDefined();
  });
});
