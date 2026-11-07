import type { Logger } from 'pino';
import { describe, expect, it } from 'vitest';

import { loadEnv } from '../../src/config/env.js';
import { createPgPool, pgPoolConfig } from '../../src/persistence/pg-pool.js';

interface LogEntry {
  fields: object;
  message: string;
}

function captureLogger(entries: LogEntry[]): Logger {
  return {
    error: (fields: object, message: string) => {
      entries.push({ fields, message });
    },
  } as unknown as Logger;
}

describe('pgPoolConfig', () => {
  it('maps explicit timeouts and the connection string into the pool config', () => {
    const env = loadEnv({
      NODE_ENV: 'test',
      POSTGRES_URL: 'postgresql://user:pass@db:5432/kafka_inspector',
      POSTGRES_CONNECTION_TIMEOUT_MS: '1234',
      POSTGRES_STATEMENT_TIMEOUT_MS: '2345',
      POSTGRES_QUERY_TIMEOUT_MS: '3456',
    });
    expect(pgPoolConfig(env)).toMatchObject({
      connectionString: 'postgresql://user:pass@db:5432/kafka_inspector',
      application_name: 'kafka-lens',
      connectionTimeoutMillis: 1234,
      statement_timeout: 2345,
      query_timeout: 3456,
      idle_in_transaction_session_timeout: 2345,
    });
  });
});

describe('createPgPool', () => {
  it('logs an idle client error instead of crashing on the unhandled event', async () => {
    const entries: LogEntry[] = [];
    const pool = createPgPool(loadEnv({ NODE_ENV: 'test' }), captureLogger(entries));
    try {
      const error = new Error('terminating connection due to administrator command');
      expect(() => pool.emit('error', error)).not.toThrow();
      expect(entries).toHaveLength(1);
      expect(entries[0]?.message).toBe('idle PostgreSQL client error');
      const fields = entries[0]?.fields as {
        err?: { name?: unknown; message?: unknown; stack?: unknown };
      };
      expect(fields.err?.name).toBe('Error');
      expect(fields.err?.message).toBe('terminating connection due to administrator command');
      expect(typeof fields.err?.stack).toBe('string');
    } finally {
      await pool.end();
    }
  });
});
