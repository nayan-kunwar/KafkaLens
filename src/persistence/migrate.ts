import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Pool } from 'pg';

import { AppError } from '../errors/app-error.js';

const MIGRATIONS_TABLE = 'schema_migrations';

export function migrationsDirectory(): string {
  return path.join(path.dirname(fileURLToPath(import.meta.url)), 'migrations');
}

/** Applies pending SQL files in name order. Safe to run more than once. */
export async function applyMigrations(pool: Pool): Promise<string[]> {
  try {
    await pool.query(
      `CREATE TABLE IF NOT EXISTS ${MIGRATIONS_TABLE} (
        version TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )`,
    );
    // Byte order: migration files must stay zero-padded (001, 002, ...) so
    // plain code-unit sorting matches version order under every locale.
    const files = (await readdir(migrationsDirectory()))
      .filter((name) => name.endsWith('.sql'))
      .sort();
    const applied: string[] = [];
    for (const file of files) {
      const found = await pool.query(`SELECT 1 FROM ${MIGRATIONS_TABLE} WHERE version = $1`, [
        file,
      ]);
      if ((found.rowCount ?? 0) > 0) continue;
      const sql = await readFile(path.join(migrationsDirectory(), file), 'utf8');
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        for (const statement of sqlStatements(sql)) {
          await client.query(statement);
        }
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
      } finally {
        client.release();
      }
    }
    return applied;
  } catch (err) {
    if (err instanceof AppError) throw err;
    throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL schema setup failed', {
      store: 'postgres',
      cause: err instanceof Error ? err.message : String(err),
    });
  }
}

export function sqlStatements(sql: string): string[] {
  const withoutComments = sql
    .split('\n')
    .filter((line) => !line.trim().startsWith('--'))
    .join('\n');
  return withoutComments
    .split(';')
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
}
