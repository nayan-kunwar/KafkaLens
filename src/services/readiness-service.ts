import type { Pool } from 'pg';

import type { KafkaAdminSession } from './kafka-admin-session.js';

export interface ReadinessCache {
  status(): Promise<'ok' | 'unavailable'>;
}

export interface DependencyCheck {
  name: 'kafka' | 'postgresql' | 'redis';
  status: 'ok' | 'unavailable';
  message: string;
}

export interface ReadinessReport {
  status: 'ready' | 'degraded';
  checkedAt: string;
  dependencies: DependencyCheck[];
}

const CHECK_TIMEOUT_MS = 10_000;

/**
 * Bounds a dependency probe. The underlying work cannot be cancelled, but the
 * report never waits longer than CHECK_TIMEOUT_MS for any single check.
 */
function withCheckTimeout<T>(label: string, work: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} check timed out`)), CHECK_TIMEOUT_MS);
  });
  return Promise.race([work, timeout]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
}

/**
 * Dependency report. Each check fails open: a down dependency is a row in the
 * report, and this method still resolves.
 */
export class ReadinessService {
  constructor(
    private readonly session: KafkaAdminSession,
    private readonly pool: Pool,
    private readonly cache: ReadinessCache,
  ) {}

  async report(): Promise<ReadinessReport> {
    const dependencies = await Promise.all([
      this.checkKafka(),
      this.checkPostgres(),
      this.checkRedis(),
    ]);
    return {
      status: dependencies.every((row) => row.status === 'ok') ? 'ready' : 'degraded',
      checkedAt: new Date().toISOString(),
      dependencies,
    };
  }

  private async checkKafka(): Promise<DependencyCheck> {
    try {
      await withCheckTimeout(
        'kafka',
        this.session.run(async (admin) => {
          await admin.describeCluster();
        }),
      );
      return { name: 'kafka', status: 'ok', message: 'Cluster metadata was read.' };
    } catch {
      return { name: 'kafka', status: 'unavailable', message: 'Kafka could not be reached.' };
    }
  }

  private async checkPostgres(): Promise<DependencyCheck> {
    try {
      await withCheckTimeout('postgresql', this.pool.query('SELECT 1'));
      return { name: 'postgresql', status: 'ok', message: 'PostgreSQL accepted a query.' };
    } catch {
      return {
        name: 'postgresql',
        status: 'unavailable',
        message: 'PostgreSQL could not be reached.',
      };
    }
  }

  private async checkRedis(): Promise<DependencyCheck> {
    try {
      const status = await withCheckTimeout('redis', this.cache.status());
      if (status === 'ok') {
        return { name: 'redis', status: 'ok', message: 'Redis cache answered a ping.' };
      }
    } catch {
      // A throwing cache is a down dependency, not a failed report.
    }
    return {
      name: 'redis',
      status: 'unavailable',
      message: 'Redis cache is not connected.',
    };
  }
}
