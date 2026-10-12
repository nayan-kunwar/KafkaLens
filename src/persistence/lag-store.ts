import { Pool } from 'pg';

import { AppError } from '../errors/app-error.js';

/**
 * Canonical DDL for lag history. Mirrors
 * src/persistence/migrations/001-lag-observations.sql exactly; a unit test
 * fails if the two drift apart. Unknown values are stored as NULL, never zero.
 */
export const LAG_OBSERVATIONS_DDL = `-- M5 lag history.
CREATE TABLE IF NOT EXISTS lag_observations (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  cluster_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  consumer_group TEXT NOT NULL,
  partition INTEGER NOT NULL,
  high_watermark TEXT,
  committed_offset TEXT,
  lag TEXT,
  observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS lag_observations_topic_group_partition_time
  ON lag_observations (topic, consumer_group, partition, observed_at);`;

const RECORD_CHUNK_ROWS = 500;

export interface LagObservationRow {
  clusterId: string;
  topic: string;
  consumerGroup: string;
  partition: number;
  highWatermark: string | null;
  committedOffset: string | null;
  lag: string | null;
  observedAt: string;
}

export interface LagHistoryQuery {
  groupId: string;
  topic: string;
  partition?: number | undefined;
  since?: string | undefined;
  limit: number;
}

export interface LagHistoryPoint {
  observedAt: string;
  highWatermark: string | null;
  committedOffset: string | null;
  lag: string | null;
}

export interface LagHistoryResult {
  observations: LagHistoryPoint[];
  truncated: boolean;
}

export class LagStore {
  private readonly pool: Pool;

  constructor(pool: Pool) {
    this.pool = pool;
  }

  static fromUrl(url: string): LagStore {
    return new LagStore(new Pool({ connectionString: url }));
  }

  async ensureSchema(): Promise<void> {
    try {
      await this.pool.query(LAG_OBSERVATIONS_DDL);
    } catch {
      throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL schema setup failed', {
        store: 'postgres',
      });
    }
  }

  async recordBatch(rows: LagObservationRow[]): Promise<void> {
    for (let start = 0; start < rows.length; start += RECORD_CHUNK_ROWS) {
      await this.insertChunk(rows.slice(start, start + RECORD_CHUNK_ROWS));
    }
  }

  private async insertChunk(rows: LagObservationRow[]): Promise<void> {
    if (rows.length === 0) return;
    const columns =
      '(cluster_id, topic, consumer_group, partition, high_watermark, committed_offset, lag, observed_at)';
    const params: unknown[] = [];
    const placeholders = rows.map((row, index) => {
      const base = index * 8;
      params.push(
        row.clusterId,
        row.topic,
        row.consumerGroup,
        row.partition,
        row.highWatermark,
        row.committedOffset,
        row.lag,
        row.observedAt,
      );
      const refs = Array.from({ length: 8 }, (_, i) => `$${base + i + 1}`);
      return `(${refs.join(', ')})`;
    });
    try {
      await this.pool.query(
        `INSERT INTO lag_observations ${columns} VALUES ${placeholders.join(', ')}`,
        params,
      );
    } catch {
      throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL write failed', { store: 'postgres' });
    }
  }

  async queryHistory(query: LagHistoryQuery): Promise<LagHistoryResult> {
    const conditions = ['consumer_group = $1', 'topic = $2'];
    const params: unknown[] = [query.groupId, query.topic];
    if (query.partition !== undefined) {
      params.push(query.partition);
      conditions.push(`partition = $${params.length}`);
    }
    if (query.since !== undefined) {
      params.push(query.since);
      conditions.push(`observed_at >= $${params.length}::timestamptz`);
    }
    params.push(query.limit + 1);
    const sql =
      'SELECT observed_at, high_watermark, committed_offset, lag FROM lag_observations ' +
      `WHERE ${conditions.join(' AND ')} ` +
      `ORDER BY observed_at DESC, partition ASC LIMIT $${params.length}`;
    let result: { rows: Record<string, unknown>[] };
    try {
      result = await this.pool.query(sql, params);
    } catch {
      throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL read failed', { store: 'postgres' });
    }
    const truncated = result.rows.length > query.limit;
    return {
      observations: result.rows.slice(0, query.limit).map((row) => ({
        observedAt: (row.observed_at as Date).toISOString(),
        highWatermark: row.high_watermark as string | null,
        committedOffset: row.committed_offset as string | null,
        lag: row.lag as string | null,
      })),
      truncated,
    };
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
