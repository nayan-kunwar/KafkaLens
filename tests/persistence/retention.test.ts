import type { Pool } from 'pg';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { applyMigrations, sqlStatements } from '../../src/persistence/migrate.js';
import {
  deleteOlderThan,
  retentionCutoff as cutoff,
  type RetentionTable,
} from '../../src/persistence/retention.js';

describe('retention cutoff', () => {
  it('returns null when retention is disabled', () => {
    expect(cutoff(0, new Date('2026-06-01T00:00:00.000Z'))).toBeNull();
  });

  it('subtracts whole days from the clock', () => {
    expect(cutoff(14, new Date('2026-06-15T00:00:00.000Z'))?.toISOString()).toBe(
      '2026-06-01T00:00:00.000Z',
    );
  });

  it('returns null for negative or non-finite input, disabling deletion', () => {
    expect(cutoff(-1)).toBeNull();
    expect(cutoff(Number.NaN)).toBeNull();
  });
});

describe('migration statements', () => {
  it('drops comments and blank pieces', () => {
    expect(
      sqlStatements('-- note\nCREATE TABLE t (id INT);\n\nCREATE INDEX i ON t (id);\n'),
    ).toEqual(['CREATE TABLE t (id INT)', 'CREATE INDEX i ON t (id)']);
  });
});

describe('deleteOlderThan', () => {
  const deletedAt = new Date('2026-06-01T00:00:00.000Z');

  function stubPool(rowCount: number): { pool: Pool; seen: { sql: string; params: unknown[] }[] } {
    const seen: { sql: string; params: unknown[] }[] = [];
    const pool = {
      query: (sql: string, params: unknown[] = []) => {
        seen.push({ sql, params });
        return Promise.resolve({ rowCount, rows: [] });
      },
    };
    return { pool: pool as unknown as Pool, seen };
  }

  it.each([['lag_observations'], ['incidents']] as RetentionTable[][])(
    'deletes from %s with the cutoff as a bound parameter',
    async (table) => {
      const { pool, seen } = stubPool(3);
      const deleted = await deleteOlderThan(pool, table, deletedAt);
      expect(deleted).toBe(3);
      expect(seen).toHaveLength(1);
      expect(seen[0]?.sql).toBe(`DELETE FROM ${table} WHERE observed_at < $1`);
      expect(seen[0]?.params).toEqual([deletedAt.toISOString()]);
    },
  );

  it('maps a database failure to STORE_UNAVAILABLE with the table in context', async () => {
    const pool = {
      query: () => Promise.reject(new Error('connection reset')),
    } as unknown as Pool;
    const thrown = await deleteOlderThan(pool, 'incidents', deletedAt).catch((err: unknown) => err);
    expect(thrown).toBeInstanceOf(AppError);
    expect((thrown as AppError).code).toBe('STORE_UNAVAILABLE');
    expect((thrown as AppError).context).toMatchObject({ table: 'incidents' });
  });
});

describe('applyMigrations', () => {
  interface StubClient {
    statements: string[];
    query: (sql: string, params?: unknown[]) => Promise<{ rowCount: number; rows: unknown[] }>;
    release: () => void;
  }

  function stubPool(failOn?: RegExp): {
    pool: Pool;
    applied: Set<string>;
    statements: string[];
    inserts: string[];
  } {
    const applied = new Set<string>();
    const statements: string[] = [];
    const inserts: string[] = [];
    const client: StubClient = {
      statements,
      query: (sql: string, params: unknown[] = []) => {
        statements.push(sql);
        if (failOn?.test(sql)) return Promise.reject(new Error('syntax error'));
        if (sql.startsWith('INSERT INTO schema_migrations')) {
          const version = params[0] as string;
          applied.add(version);
          inserts.push(version);
        }
        return Promise.resolve({ rowCount: 0, rows: [] });
      },
      release: () => undefined,
    };
    const pool = {
      query: (sql: string, params: unknown[] = []) => {
        if (sql.startsWith('SELECT 1 FROM schema_migrations')) {
          const version = params[0] as string;
          return Promise.resolve({ rowCount: applied.has(version) ? 1 : 0, rows: [] });
        }
        return Promise.resolve({ rowCount: 0, rows: [] });
      },
      connect: () => Promise.resolve(client),
    };
    return { pool: pool as unknown as Pool, applied, statements, inserts };
  }

  it('applies pending files in order and records each one', async () => {
    const { pool, applied, statements, inserts } = stubPool();
    const result = await applyMigrations(pool);
    expect(result).toEqual(['001-lag-observations.sql', '002-incidents.sql']);
    expect(applied).toEqual(new Set(['001-lag-observations.sql', '002-incidents.sql']));
    expect(inserts).toEqual(['001-lag-observations.sql', '002-incidents.sql']);
    const lagTable = statements.findIndex((sql) => sql.includes('lag_observations'));
    const lagRecordAt = statements.findIndex((sql) =>
      sql.startsWith('INSERT INTO schema_migrations'),
    );
    expect(lagTable).toBeGreaterThanOrEqual(0);
    expect(lagRecordAt).toBeGreaterThan(lagTable);
  });

  it('is a no-op on the second run', async () => {
    const { pool } = stubPool();
    await applyMigrations(pool);
    await expect(applyMigrations(pool)).resolves.toEqual([]);
  });

  it('rolls back a failing file without recording it', async () => {
    const { pool, applied, statements } = stubPool(/CREATE INDEX/);
    const thrown = await applyMigrations(pool).catch((err: unknown) => err);
    expect(thrown).toBeInstanceOf(AppError);
    expect((thrown as AppError).code).toBe('STORE_UNAVAILABLE');
    expect((thrown as AppError).context).toMatchObject({ file: '001-lag-observations.sql' });
    expect(applied).toEqual(new Set());
    expect(statements).toContain('ROLLBACK');
    expect(statements.some((sql) => sql.startsWith('INSERT INTO schema_migrations'))).toBe(false);
  });
});
