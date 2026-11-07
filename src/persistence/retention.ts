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

export const RETENTION_BATCH_ROWS = 5_000;

/**
 * Deletes in bounded batches so one retention run does not hold a long
 * transaction or lock a large share of the table. Returns the total deleted.
 */
export async function deleteOlderThan(
  pool: Pool,
  table: RetentionTable,
  cutoff: Date,
  batchSize: number = RETENTION_BATCH_ROWS,
): Promise<number> {
  const name = TABLES[table];
  const limit = Number.isInteger(batchSize) && batchSize > 0 ? batchSize : RETENTION_BATCH_ROWS;
  try {
    let total = 0;
    for (;;) {
      const result = await pool.query(
        `WITH doomed AS (
           SELECT id FROM ${name} WHERE observed_at < $1 ORDER BY observed_at LIMIT $2
         )
         DELETE FROM ${name} WHERE id IN (SELECT id FROM doomed)`,
        [cutoff.toISOString(), limit],
      );
      const deleted = result.rowCount ?? 0;
      total += deleted;
      if (deleted < limit) return total;
    }
  } catch {
    throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL retention delete failed', {
      store: 'postgres',
      table: name,
    });
  }
}
