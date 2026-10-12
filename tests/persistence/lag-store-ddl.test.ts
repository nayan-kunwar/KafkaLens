import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import { LAG_OBSERVATIONS_DDL } from '../../src/persistence/lag-store.js';

const MIGRATION_URL = new URL(
  '../../src/persistence/migrations/001-lag-observations.sql',
  import.meta.url,
);

function normalize(sql: string): string {
  return sql
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith('--'))
    .join('\n');
}

describe('lag history DDL', () => {
  it('matches the reviewed migration file', async () => {
    const migration = await readFile(MIGRATION_URL, 'utf8');
    expect(normalize(LAG_OBSERVATIONS_DDL)).toBe(normalize(migration));
  });
});
