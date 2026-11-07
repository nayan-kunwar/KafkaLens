import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Pool, PoolClient } from 'pg';

import { AppError } from '../errors/app-error.js';

const MIGRATIONS_TABLE = 'schema_migrations';
// Stable application-wide key for the migration advisory lock. Any process
// using this code serializes schema changes on the same key.
const MIGRATION_LOCK_KEY = 7_341_928_465;

export function migrationsDirectory(): string {
  return path.join(path.dirname(fileURLToPath(import.meta.url)), 'migrations');
}

/**
 * Applies pending SQL files in name order. A session advisory lock makes
 * concurrent `migrate` processes safe, and each file runs as one transaction
 * using the raw SQL (no naive `;` splitting).
 */
export async function applyMigrations(pool: Pool): Promise<string[]> {
  let client: PoolClient | undefined;
  let locked = false;
  try {
    client = await pool.connect();
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )`,
    );
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_KEY]);
    locked = true;

    // Byte order: migration files must stay zero-padded (001, 002, ...) so
    // plain code-unit sorting matches version order under every locale.
    const files = (await readdir(migrationsDirectory()))
      .filter((name) => name.endsWith('.sql'))
      .sort();
    const applied: string[] = [];
    for (const file of files) {
      const found = await client.query(`SELECT 1 FROM ${MIGRATIONS_TABLE} WHERE version = $1`, [
        file,
      ]);
      if ((found.rowCount ?? 0) > 0) continue;
      const sql = await readFile(path.join(migrationsDirectory(), file), 'utf8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query(`INSERT INTO ${MIGRATIONS_TABLE} (version) VALUES ($1)`, [file]);
        await client.query('COMMIT');
        applied.push(file);
      } catch (err) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw new AppError('STORE_UNAVAILABLE', `PostgreSQL migration failed: ${file}`, {
          store: 'postgres',
          file,
          cause: err instanceof Error ? err.message : String(err),
        });
      }
    }
    return applied;
  } catch (err) {
    if (err instanceof AppError) throw err;
    throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL schema setup failed', {
      store: 'postgres',
      cause: err instanceof Error ? err.message : String(err),
    });
  } finally {
    if (client !== undefined) {
      if (locked) {
        await client
          .query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_KEY])
          .catch(() => undefined);
      }
      client.release();
    }
  }
}
