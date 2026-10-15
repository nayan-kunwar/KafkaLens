import { Pool } from 'pg';

import { loadEnv } from '../config/env.js';
import { loadDotenv } from '../config/load-dotenv.js';
import { AppError } from '../errors/app-error.js';

import { deleteOlderThan, retentionCutoff } from './retention.js';

loadDotenv();

try {
  const env = loadEnv();
  const pool = new Pool({
    connectionString: env.POSTGRES_URL,
    connectionTimeoutMillis: 5_000,
  });
  try {
    const now = new Date();
    const lagCutoff = retentionCutoff(env.LAG_RETENTION_DAYS, now);
    const incidentCutoff = retentionCutoff(env.INCIDENT_RETENTION_DAYS, now);
    const lagDeleted =
      lagCutoff === null ? 0 : await deleteOlderThan(pool, 'lag_observations', lagCutoff);
    const incidentDeleted =
      incidentCutoff === null ? 0 : await deleteOlderThan(pool, 'incidents', incidentCutoff);
    process.stderr.write(
      `kafka-lens retention deleted ${lagDeleted} lag row(s) and ${incidentDeleted} incident row(s)\n`,
    );
  } finally {
    await pool.end();
  }
} catch (err) {
  const message = err instanceof AppError ? err.message : 'PostgreSQL retention delete failed';
  const context =
    err instanceof AppError && err.context !== undefined ? ` ${JSON.stringify(err.context)}` : '';
  process.stderr.write(`kafka-lens retention failed: ${message}${context}\n`);
  process.exitCode = 1;
}
