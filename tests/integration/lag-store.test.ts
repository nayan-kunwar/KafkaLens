import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { LagStore } from '../../src/persistence/lag-store.js';

const POSTGRES_IMAGE = 'postgres:16-alpine';

async function expectStoreUnavailable(promise: Promise<unknown>): Promise<AppError> {
  let thrown: unknown;
  try {
    await promise;
  } catch (err) {
    thrown = err;
  }
  expect(thrown).toBeInstanceOf(AppError);
  const error = thrown as AppError;
  expect(error.code).toBe('STORE_UNAVAILABLE');
  return error;
}

describe('lag store', () => {
  let container: StartedPostgreSqlContainer;
  let store: LagStore;

  beforeAll(async () => {
    container = await new PostgreSqlContainer(POSTGRES_IMAGE).start();
    store = new LagStore(new Pool({ connectionString: container.getConnectionUri() }));
    await store.ensureSchema();
  }, 300_000);

  afterAll(async () => {
    await store?.close();
    await container?.stop();
  });

  it('creates the schema idempotently', async () => {
    await store.ensureSchema();
    await store.ensureSchema();
  });

  it('round-trips observations with unknowns kept null', async () => {
    await store.recordBatch([
      {
        clusterId: 'c1',
        topic: 'orders',
        consumerGroup: 'g1',
        partition: 0,
        highWatermark: '10',
        committedOffset: '4',
        lag: '6',
        observedAt: '2026-01-01T00:00:00.000Z',
      },
      {
        clusterId: 'c1',
        topic: 'orders',
        consumerGroup: 'g1',
        partition: 1,
        highWatermark: '0',
        committedOffset: null,
        lag: null,
        observedAt: '2026-01-02T00:00:00.000Z',
      },
    ]);

    const history = await store.queryHistory({ groupId: 'g1', topic: 'orders', limit: 100 });
    expect(history.truncated).toBe(false);
    expect(history.observations).toEqual([
      {
        partition: 1,
        observedAt: '2026-01-02T00:00:00.000Z',
        highWatermark: '0',
        committedOffset: null,
        lag: null,
      },
      {
        partition: 0,
        observedAt: '2026-01-01T00:00:00.000Z',
        highWatermark: '10',
        committedOffset: '4',
        lag: '6',
      },
    ]);
  });

  it('filters by partition and since, newest first with truncation', async () => {
    const groupId = 'g-filter';
    await store.recordBatch(
      [0, 1, 2].map((n) => ({
        clusterId: 'c1',
        topic: 'orders',
        consumerGroup: groupId,
        partition: n % 2,
        highWatermark: `${10 + n}`,
        committedOffset: `${n}`,
        lag: '10',
        observedAt: `2026-02-0${n + 1}T00:00:00.000Z`,
      })),
    );

    const byPartition = await store.queryHistory({
      groupId,
      topic: 'orders',
      partition: 0,
      limit: 100,
    });
    expect(byPartition.observations).toHaveLength(2);
    expect(byPartition.truncated).toBe(false);

    const since = await store.queryHistory({
      groupId,
      topic: 'orders',
      since: '2026-02-02T00:00:00.000Z',
      limit: 100,
    });
    expect(since.observations.map((row) => row.observedAt)).toEqual([
      '2026-02-03T00:00:00.000Z',
      '2026-02-02T00:00:00.000Z',
    ]);

    const capped = await store.queryHistory({ groupId, topic: 'orders', limit: 2 });
    expect(capped.observations).toHaveLength(2);
    expect(capped.truncated).toBe(true);
  });

  it('reports STORE_UNAVAILABLE when the pool is closed', async () => {
    const broken = new LagStore(new Pool({ connectionString: container.getConnectionUri() }));
    await broken.close();
    await expectStoreUnavailable(broken.queryHistory({ groupId: 'g', topic: 't', limit: 10 }));
    await expectStoreUnavailable(
      broken.recordBatch([
        {
          clusterId: 'c',
          topic: 't',
          consumerGroup: 'g',
          partition: 0,
          highWatermark: '1',
          committedOffset: '0',
          lag: '1',
          observedAt: new Date().toISOString(),
        },
      ]),
    );
    await expectStoreUnavailable(broken.ensureSchema());
  });
});
