import { Pool, type PoolConfig } from 'pg';
import type { Logger } from 'pino';

import type { Env } from '../config/env.js';

/**
 * Shared pool configuration. Statement and query timeouts prevent one slow
 * query from pinning a client forever, and the connection timeout keeps a
 * dead database from blocking startup work.
 */
export function pgPoolConfig(env: Env): PoolConfig {
  return {
    connectionString: env.POSTGRES_URL,
    application_name: 'kafka-lens',
    connectionTimeoutMillis: env.POSTGRES_CONNECTION_TIMEOUT_MS,
    statement_timeout: env.POSTGRES_STATEMENT_TIMEOUT_MS,
    query_timeout: env.POSTGRES_QUERY_TIMEOUT_MS,
    idle_in_transaction_session_timeout: env.POSTGRES_STATEMENT_TIMEOUT_MS,
  };
}

/**
 * Creates the process pool and attaches an idle-client error handler. Without
 * this listener, an error on an idle PostgreSQL connection is emitted as an
 * unhandled `error` event and terminates the MCP process.
 */
export function createPgPool(env: Env, logger: Logger): Pool {
  const pool = new Pool(pgPoolConfig(env));
  pool.on('error', (err: Error) => {
    logger.error(
      { err: { name: err.name, message: err.message, stack: err.stack } },
      'idle PostgreSQL client error',
    );
  });
  return pool;
}
