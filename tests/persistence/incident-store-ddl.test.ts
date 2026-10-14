import { readFile } from 'node:fs/promises';

import { describe, expect, it } from 'vitest';

import { INCIDENTS_DDL } from '../../src/persistence/incident-store.js';

const MIGRATION_URL = new URL(
  '../../src/persistence/migrations/002-incidents.sql',
  import.meta.url,
);

function normalize(sql: string): string {
  return sql
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith('--'))
    .join('\n');
}

describe('incident DDL', () => {
  it('matches the reviewed migration file', async () => {
    const migration = await readFile(MIGRATION_URL, 'utf8');
    expect(normalize(INCIDENTS_DDL)).toBe(normalize(migration));
  });
});
