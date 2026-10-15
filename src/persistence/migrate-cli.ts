import { Pool } from 'pg';

import { loadEnv } from '../config/env.js';
import { loadDotenv } from '../config/load-dotenv.js';
import { AppError } from '../errors/app-error.js';

import { applyMigrations } from './migrate.js';

loadDotenv();

try {
  const env = loadEnv();
  const pool = new Pool({
    connectionString: env.POSTGRES_URL,
    connectionTimeoutMillis: 5_000,
  });
  try {
    const applied = await applyMigrations(pool);
    const detail = applied.length === 0 ? 'none' : applied.join(', ');
    process.stderr.write(`kafka-lens migrations applied: ${detail}\n`);
  } finally {
    await pool.end();
  }
} catch (err) {
  const message = err instanceof AppError ? err.message : 'PostgreSQL schema setup failed';
  const context =
    err instanceof AppError && err.context !== undefined ? ` ${JSON.stringify(err.context)}` : '';
  process.stderr.write(`kafka-lens migration failed: ${message}${context}\n`);
  process.exitCode = 1;
}
