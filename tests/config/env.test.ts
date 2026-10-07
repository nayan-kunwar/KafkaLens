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

function captureError(env: NodeJS.ProcessEnv): AppError {
  try {
    loadEnv(env);
  } catch (err) {
    if (err instanceof AppError) return err;
    throw err;
  }
  throw new Error('expected loadEnv to throw');
}

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
    expect(env.KAFKA_SASL_MECHANISM).toBe('plain');
    expect(env.KAFKA_SSL_REJECT_UNAUTHORIZED).toBe(true);
    expect(env.KAFKA_REQUEST_TIMEOUT_MS).toBe(30_000);
    expect(env.KAFKA_CONNECTION_TIMEOUT_MS).toBe(10_000);
    expect(env.KAFKA_RETRY_ATTEMPTS).toBe(5);
  });

  it('throws a structured AppError on invalid values without leaking secrets', () => {
    const env = { ...validEnv, LOG_LEVEL: 'chatty' };
    const error = captureError(env);
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
    const error = captureError(env);
    expect(error.code).toBe('CONFIG_INVALID');
    expect(error.context?.fields).toBeDefined();
  });

  describe('SASL cross-field validation', () => {
    it('requires credentials when security protocol uses SASL', () => {
      const env = { ...validEnv, KAFKA_SECURITY_PROTOCOL: 'SASL_PLAINTEXT' };
      const error = captureError(env);
      expect(error.code).toBe('CONFIG_INVALID');
      expect(error.message).toContain('KAFKA_SASL_USERNAME');
      expect(error.message).toContain('KAFKA_SASL_PASSWORD');
    });

    it('accepts SASL_PLAINTEXT with credentials', () => {
      const env = loadEnv({
        ...validEnv,
        KAFKA_SECURITY_PROTOCOL: 'SASL_PLAINTEXT',
        KAFKA_SASL_USERNAME: 'alice',
        KAFKA_SASL_PASSWORD: 'alice-secret',
      });
      expect(env.KAFKA_SECURITY_PROTOCOL).toBe('SASL_PLAINTEXT');
      expect(env.KAFKA_SASL_USERNAME).toBe('alice');
    });

    it('does not echo the password in validation errors', () => {
      const env = {
        ...validEnv,
        KAFKA_SECURITY_PROTOCOL: 'SASL_SSL',
        KAFKA_SASL_USERNAME: 'alice',
        KAFKA_SASL_PASSWORD: 'SECRETPASS',
        KAFKA_SASL_MECHANISM: 'not-a-mechanism',
      };
      const error = captureError(env);
      expect(error.message).not.toContain('SECRETPASS');
    });
  });

  describe('SSL and retry settings', () => {
    it('parses boolean false for KAFKA_SSL_REJECT_UNAUTHORIZED', () => {
      const env = loadEnv({ ...validEnv, KAFKA_SSL_REJECT_UNAUTHORIZED: 'false' });
      expect(env.KAFKA_SSL_REJECT_UNAUTHORIZED).toBe(false);
    });

    it('rejects malformed boolean values', () => {
      const error = captureError({ ...validEnv, KAFKA_SSL_REJECT_UNAUTHORIZED: 'yes' });
      expect(error.code).toBe('CONFIG_INVALID');
      expect(error.message).toContain('KAFKA_SSL_REJECT_UNAUTHORIZED');
    });

    it('parses KAFKA_RETRY_ATTEMPTS including zero', () => {
      const env = loadEnv({ ...validEnv, KAFKA_RETRY_ATTEMPTS: '0' });
      expect(env.KAFKA_RETRY_ATTEMPTS).toBe(0);
    });
  });
});
