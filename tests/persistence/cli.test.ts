import type { Pool } from 'pg';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { runMigrateCli } from '../../src/persistence/migrate-cli.js';
import { runRetainCli } from '../../src/persistence/retain-cli.js';

function poolStub(ended: string[]): Pool {
  return {
    end: () => {
      ended.push('end');
      return Promise.resolve();
    },
  } as unknown as Pool;
}

describe('runMigrateCli', () => {
  it('applies migrations and reports the files', async () => {
    const lines: string[] = [];
    const ended: string[] = [];
    const code = await runMigrateCli({
      env: { NODE_ENV: 'test' },
      createPool: () => poolStub(ended),
      apply: () => Promise.resolve(['001-lag-observations.sql', '002-incidents.sql']),
      write: (text) => lines.push(text),
    });

    expect(code).toBe(0);
    expect(lines).toEqual([
      'kafka-lens migrations applied: 001-lag-observations.sql, 002-incidents.sql\n',
    ]);
    expect(ended).toEqual(['end']);
  });

  it('reports "none" when the schema is already current', async () => {
    const lines: string[] = [];
    const code = await runMigrateCli({
      env: { NODE_ENV: 'test' },
      createPool: () => poolStub([]),
      apply: () => Promise.resolve([]),
      write: (text) => lines.push(text),
    });
    expect(code).toBe(0);
    expect(lines[0]).toContain('applied: none');
  });

  it('returns 1 for a structured migration failure and still closes the pool', async () => {
    const lines: string[] = [];
    const ended: string[] = [];
    const code = await runMigrateCli({
      env: { NODE_ENV: 'test' },
      createPool: () => poolStub(ended),
      apply: () =>
        Promise.reject(
          new AppError('STORE_UNAVAILABLE', 'PostgreSQL migration failed: 001.sql', {
            store: 'postgres',
          }),
        ),
      write: (text) => lines.push(text),
    });

    expect(code).toBe(1);
    expect(lines[0]).toContain('PostgreSQL migration failed: 001.sql');
    expect(lines[0]).toContain('"store":"postgres"');
    expect(ended).toEqual(['end']);
  });

  it('fails fast on invalid configuration without creating a pool', async () => {
    const lines: string[] = [];
    let created = 0;
    const code = await runMigrateCli({
      env: { NODE_ENV: 'production' },
      createPool: () => {
        created += 1;
        return poolStub([]);
      },
      write: (text) => lines.push(text),
    });

    expect(code).toBe(1);
    expect(created).toBe(0);
    expect(lines[0]).toContain('Invalid environment configuration');
    expect(lines[0]).toContain('KAFKA_BROKERS');
  });
});

describe('runRetainCli', () => {
  it('deletes with the configured cutoffs and prints both counts', async () => {
    const lines: string[] = [];
    const ended: string[] = [];
    const calls: { table: string; cutoff: string }[] = [];
    const now = new Date('2026-06-15T00:00:00.000Z');
    const code = await runRetainCli({
      env: {
        NODE_ENV: 'test',
        LAG_RETENTION_DAYS: '14',
        INCIDENT_RETENTION_DAYS: '90',
      },
      createPool: () => poolStub(ended),
      now: () => now,
      remove: (_pool, table, cutoff) => {
        calls.push({ table, cutoff: cutoff.toISOString() });
        return Promise.resolve(table === 'lag_observations' ? 5 : 2);
      },
      write: (text) => lines.push(text),
    });

    expect(code).toBe(0);
    expect(calls).toEqual([
      { table: 'lag_observations', cutoff: '2026-06-01T00:00:00.000Z' },
      { table: 'incidents', cutoff: '2026-03-17T00:00:00.000Z' },
    ]);
    expect(lines).toEqual(['kafka-lens retention deleted 5 lag row(s) and 2 incident row(s)\n']);
    expect(ended).toEqual(['end']);
  });

  it('skips both tables when retention is disabled', async () => {
    const lines: string[] = [];
    let calls = 0;
    const code = await runRetainCli({
      env: {
        NODE_ENV: 'test',
        LAG_RETENTION_DAYS: '0',
        INCIDENT_RETENTION_DAYS: '0',
      },
      createPool: () => poolStub([]),
      remove: () => {
        calls += 1;
        return Promise.resolve(0);
      },
      write: (text) => lines.push(text),
    });

    expect(code).toBe(0);
    expect(calls).toBe(0);
    expect(lines[0]).toContain('deleted 0 lag row(s) and 0 incident row(s)');
  });

  it('returns 1 for a structured delete failure and still closes the pool', async () => {
    const lines: string[] = [];
    const ended: string[] = [];
    const code = await runRetainCli({
      env: { NODE_ENV: 'test', LAG_RETENTION_DAYS: '14' },
      createPool: () => poolStub(ended),
      remove: () => Promise.reject(new AppError('STORE_UNAVAILABLE', 'retention delete failed')),
      write: (text) => lines.push(text),
    });

    expect(code).toBe(1);
    expect(lines[0]).toContain('retention delete failed');
    expect(ended).toEqual(['end']);
  });
});
