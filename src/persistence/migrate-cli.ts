import { pathToFileURL } from 'node:url';

import { Pool } from 'pg';

import { loadEnv, type Env } from '../config/env.js';
import { loadDotenv } from '../config/load-dotenv.js';
import { AppError } from '../errors/app-error.js';

import { applyMigrations } from './migrate.js';
import { pgPoolConfig } from './pg-pool.js';

export interface MigrateCliOptions {
  env?: NodeJS.ProcessEnv;
  createPool?: (env: Env) => Pool;
  apply?: (pool: Pool) => Promise<string[]>;
  write?: (text: string) => void;
}

/** Returns a process exit code. Writes only to stderr, like the real CLI. */
export async function runMigrateCli(options: MigrateCliOptions = {}): Promise<number> {
  const write = options.write ?? ((text: string) => process.stderr.write(text));
  try {
    const env = loadEnv(options.env ?? process.env);
    const pool = options.createPool?.(env) ?? new Pool(pgPoolConfig(env));
    try {
      const applied = await (options.apply ?? applyMigrations)(pool);
      const detail = applied.length === 0 ? 'none' : applied.join(', ');
      write(`kafka-lens migrations applied: ${detail}\n`);
    } finally {
      await pool.end();
    }
    return 0;
  } catch (err) {
    const message = err instanceof AppError ? err.message : 'PostgreSQL schema setup failed';
    const context =
      err instanceof AppError && err.context !== undefined ? ` ${JSON.stringify(err.context)}` : '';
    write(`kafka-lens migration failed: ${message}${context}\n`);
    return 1;
  }
}

async function main(): Promise<void> {
  loadDotenv();
  process.exitCode = await runMigrateCli();
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
