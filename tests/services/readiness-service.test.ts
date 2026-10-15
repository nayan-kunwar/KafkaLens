import type { Pool } from 'pg';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { KafkaAdminSession } from '../../src/services/kafka-admin-session.js';
import { ReadinessService } from '../../src/services/readiness-service.js';

function session(run: () => Promise<unknown>): KafkaAdminSession {
  return { run } as unknown as KafkaAdminSession;
}

describe('ReadinessService', () => {
  it('reports each dependency and stays resolved when one is down', async () => {
    const service = new ReadinessService(
      session(() => Promise.reject(new AppError('KAFKA_UNAVAILABLE', 'down'))),
      { query: () => Promise.resolve({ rows: [] }) } as unknown as Pool,
      { status: () => Promise.resolve('unavailable') },
    );

    const report = await service.report();
    expect(report.status).toBe('degraded');
    expect(report.dependencies).toEqual([
      { name: 'kafka', status: 'unavailable', message: 'Kafka could not be reached.' },
      { name: 'postgresql', status: 'ok', message: 'PostgreSQL accepted a query.' },
      { name: 'redis', status: 'unavailable', message: 'Redis cache is not connected.' },
    ]);
    expect(JSON.stringify(report)).not.toMatch(/ECONNREFUSED|postgres:\/\//);
  });

  it('reports ready when every dependency answers', async () => {
    const service = new ReadinessService(
      session(() => Promise.resolve(undefined)),
      { query: () => Promise.resolve({ rows: [] }) } as unknown as Pool,
      { status: () => Promise.resolve('ok') },
    );
    const report = await service.report();
    expect(report.status).toBe('ready');
    expect(report.dependencies.map((row) => row.status)).toEqual(['ok', 'ok', 'ok']);
  });

  it('stays resolved when the cache throws instead of answering', async () => {
    const service = new ReadinessService(
      session(() => Promise.resolve(undefined)),
      { query: () => Promise.resolve({ rows: [] }) } as unknown as Pool,
      {
        status: () => Promise.reject(new Error('boom')),
      },
    );
    const report = await service.report();
    expect(report.status).toBe('degraded');
    expect(report.dependencies.find((row) => row.name === 'redis')).toMatchObject({
      status: 'unavailable',
    });
  });
});
