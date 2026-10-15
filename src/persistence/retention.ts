import type { Pool } from 'pg';

import { AppError } from '../errors/app-error.js';

const TABLES = {
  lag_observations: 'lag_observations',
  incidents: 'incidents',
} as const;

export type RetentionTable = keyof typeof TABLES;

/** `days <= 0` disables retention. The cutoff is exclusive. */
export function retentionCutoff(days: number, now: Date = new Date()): Date | null {
  if (!Number.isFinite(days) || days <= 0) return null;
  return new Date(now.getTime() - days * 24 * 60 * 60 * 1000);
}

export async function deleteOlderThan(
  pool: Pool,
  table: RetentionTable,
  cutoff: Date,
): Promise<number> {
  const name = TABLES[table];
  try {
    const result = await pool.query(`DELETE FROM ${name} WHERE observed_at < $1`, [
      cutoff.toISOString(),
    ]);
    return result.rowCount ?? 0;
  } catch {
    throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL retention delete failed', {
      store: 'postgres',
      table: name,
    });
  }
}
