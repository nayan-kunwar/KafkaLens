import { pathToFileURL } from 'node:url';

import { Pool } from 'pg';

import { loadEnv, type Env } from '../config/env.js';
import { loadDotenv } from '../config/load-dotenv.js';
import { AppError } from '../errors/app-error.js';

import { pgPoolConfig } from './pg-pool.js';
import { deleteOlderThan, retentionCutoff } from './retention.js';

export interface RetainCliOptions {
  env?: NodeJS.ProcessEnv;
  createPool?: (env: Env) => Pool;
  remove?: typeof deleteOlderThan;
  now?: () => Date;
  write?: (text: string) => void;
}

/** Returns a process exit code. Writes only to stderr, like the real CLI. */
export async function runRetainCli(options: RetainCliOptions = {}): Promise<number> {
  const write = options.write ?? ((text: string) => process.stderr.write(text));
  try {
    const env = loadEnv(options.env ?? process.env);
    const pool = options.createPool?.(env) ?? new Pool(pgPoolConfig(env));
    try {
      const now = options.now?.() ?? new Date();
      const lagCutoff = retentionCutoff(env.LAG_RETENTION_DAYS, now);
      const incidentCutoff = retentionCutoff(env.INCIDENT_RETENTION_DAYS, now);
      const remove = options.remove ?? deleteOlderThan;
      const lagDeleted = lagCutoff === null ? 0 : await remove(pool, 'lag_observations', lagCutoff);
      const incidentDeleted =
        incidentCutoff === null ? 0 : await remove(pool, 'incidents', incidentCutoff);
      write(
        `kafka-lens retention deleted ${lagDeleted} lag row(s) and ${incidentDeleted} incident row(s)\n`,
      );
    } finally {
      await pool.end();
    }
    return 0;
  } catch (err) {
    const message = err instanceof AppError ? err.message : 'PostgreSQL retention delete failed';
    const context =
      err instanceof AppError && err.context !== undefined ? ` ${JSON.stringify(err.context)}` : '';
    write(`kafka-lens retention failed: ${message}${context}\n`);
    return 1;
  }
}

async function main(): Promise<void> {
  loadDotenv();
  process.exitCode = await runRetainCli();
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
