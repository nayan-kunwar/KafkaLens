import type { Pool, PoolClient } from 'pg';

import { scoreIncident } from '../domain/similarity.js';
import { AppError } from '../errors/app-error.js';

import { applyMigrations } from './migrate.js';

/**
 * Canonical DDL for incident history. Mirrors
 * src/persistence/migrations/002-incidents.sql exactly; a unit test fails if
 * the two drift apart.
 */
export const INCIDENTS_DDL = `-- M11 similar incidents.
CREATE TABLE IF NOT EXISTS incidents (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  cluster_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  consumer_group TEXT NOT NULL,
  finding_types TEXT[] NOT NULL,
  severity TEXT NOT NULL,
  summary TEXT NOT NULL,
  evidence JSONB NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS incidents_topic_group_time
  ON incidents (topic, consumer_group, observed_at);`;

const CANDIDATE_LIMIT = 1000;

export interface NewIncident {
  clusterId: string;
  topic: string;
  consumerGroup: string;
  findingTypes: string[];
  severity: string;
  summary: string;
  evidence: string[];
  observedAt: string;
}

export interface IncidentRecord {
  id: string;
  clusterId: string;
  topic: string;
  consumerGroup: string;
  findingTypes: string[];
  severity: string;
  summary: string;
  evidence: string[];
  observedAt: string;
}

export interface SimilarIncident extends IncidentRecord {
  score: number;
}

export interface SimilarIncidentQuery {
  clusterId?: string | undefined;
  groupId: string;
  topic: string;
  findingTypes: readonly string[];
  limit: number;
}

export class IncidentStore {
  constructor(private readonly pool: Pool) {}

  async ensureSchema(): Promise<void> {
    await applyMigrations(this.pool);
  }

  async record(
    incident: NewIncident,
    dedupeWithinSeconds: number,
  ): Promise<'stored' | 'duplicate'> {
    const findingTypes = [...incident.findingTypes].sort();
    const since = new Date(
      Date.parse(incident.observedAt) - dedupeWithinSeconds * 1000,
    ).toISOString();
    // Advisory lock keyed by the dedupe identity. Concurrent diagnoses of the
    // same group/topic cannot both pass the SELECT and insert duplicates.
    const lockKey = [
      'incident',
      incident.clusterId,
      incident.topic,
      incident.consumerGroup,
      incident.severity,
      findingTypes.join(','),
    ].join(':');
    let client: PoolClient | undefined;
    try {
      client = await this.pool.connect();
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [lockKey]);
      const existing = await client.query(
        'SELECT id FROM incidents WHERE cluster_id = $1 AND topic = $2 AND consumer_group = $3 ' +
          'AND severity = $4 AND finding_types = $5::text[] AND observed_at >= $6::timestamptz LIMIT 1',
        [
          incident.clusterId,
          incident.topic,
          incident.consumerGroup,
          incident.severity,
          findingTypes,
          since,
        ],
      );
      if (existing.rows.length > 0) {
        await client.query('COMMIT');
        return 'duplicate';
      }
      await client.query(
        'INSERT INTO incidents (cluster_id, topic, consumer_group, finding_types, severity, summary, evidence, observed_at) ' +
          'VALUES ($1, $2, $3, $4::text[], $5, $6, $7::jsonb, $8::timestamptz)',
        [
          incident.clusterId,
          incident.topic,
          incident.consumerGroup,
          findingTypes,
          incident.severity,
          incident.summary,
          JSON.stringify(incident.evidence),
          incident.observedAt,
        ],
      );
      await client.query('COMMIT');
      return 'stored';
    } catch (err) {
      if (err instanceof AppError) throw err;
      await client?.query('ROLLBACK').catch(() => undefined);
      throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL write failed', { store: 'postgres' });
    } finally {
      client?.release();
    }
  }

  async findSimilar(query: SimilarIncidentQuery): Promise<SimilarIncident[]> {
    const params: unknown[] = [];
    const conditions: string[] = [];
    if (query.clusterId !== undefined) {
      params.push(query.clusterId);
      conditions.push(`cluster_id = $${params.length}`);
    }
    params.push(query.topic);
    const topicParam = `$${params.length}`;
    params.push(query.groupId);
    const groupParam = `$${params.length}`;
    let overlap = 'false';
    let overlapScore = '';
    if (query.findingTypes.length > 0) {
      params.push([...query.findingTypes]);
      overlap = `finding_types && $${params.length}::text[]`;
      overlapScore = ` + (finding_types && $${params.length}::text[])::int`;
    }
    conditions.push(`(topic = ${topicParam} OR consumer_group = ${groupParam} OR ${overlap})`);
    // Rank the bounded candidate window by exact group/topic/type overlap
    // before applying the deterministic JS score below.
    const relevance = `((topic = ${topicParam})::int + (consumer_group = ${groupParam})::int${overlapScore})`;
    params.push(CANDIDATE_LIMIT);
    const sql =
      'SELECT id, cluster_id, topic, consumer_group, finding_types, severity, summary, evidence, observed_at ' +
      `FROM incidents WHERE ${conditions.join(' AND ')} ` +
      `ORDER BY ${relevance} DESC, observed_at DESC LIMIT $${params.length}`;
    let result: { rows: Record<string, unknown>[] };
    try {
      result = await this.pool.query(sql, params);
    } catch {
      throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL read failed', { store: 'postgres' });
    }
    return result.rows
      .map((row) => {
        const incident = mapIncident(row);
        return {
          ...incident,
          score: scoreIncident(
            { groupId: query.groupId, topic: query.topic, findingTypes: query.findingTypes },
            incident,
          ),
        };
      })
      .filter((incident) => incident.score > 0)
      .sort((a, b) => b.score - a.score || b.observedAt.localeCompare(a.observedAt))
      .slice(0, query.limit);
  }

  async getById(id: string): Promise<IncidentRecord> {
    if (!/^\d+$/.test(id)) {
      throw new AppError('NOT_FOUND', `Incident "${id}" not found`, { id });
    }
    let result: { rows: Record<string, unknown>[] };
    try {
      result = await this.pool.query(
        'SELECT id, cluster_id, topic, consumer_group, finding_types, severity, summary, evidence, observed_at ' +
          'FROM incidents WHERE id = $1',
        [id],
      );
    } catch {
      throw new AppError('STORE_UNAVAILABLE', 'PostgreSQL read failed', { store: 'postgres' });
    }
    const row = result.rows[0];
    if (row === undefined) {
      throw new AppError('NOT_FOUND', `Incident "${id}" not found`, { id });
    }
    return mapIncident(row);
  }
}

function mapIncident(row: Record<string, unknown>): IncidentRecord {
  const evidence = row.evidence;
  return {
    id: String(row.id),
    clusterId: String(row.cluster_id),
    topic: String(row.topic),
    consumerGroup: String(row.consumer_group),
    findingTypes: Array.isArray(row.finding_types)
      ? row.finding_types.map((entry) => String(entry))
      : [],
    severity: String(row.severity),
    summary: String(row.summary),
    evidence: Array.isArray(evidence) ? evidence.map((entry) => String(entry)) : [],
    observedAt: (row.observed_at as Date).toISOString(),
  };
}
