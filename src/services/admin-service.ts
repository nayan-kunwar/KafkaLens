import type { Admin, Kafka } from 'kafkajs';
import type { Logger } from 'pino';

import type { KeyValueCache } from '../cache/metadata-cache.js';
import { AppError } from '../errors/app-error.js';
import { noopMetrics, type Metrics } from '../observability/metrics.js';
import { logTiming } from '../observability/tool-call.js';

import { KafkaAdminSession } from './kafka-admin-session.js';
import { mapKafkaError } from './kafka-error.js';

const MAX_PARTITION_ROWS = 500;

export interface BrokerInfo {
  nodeId: number;
  host: string;
  port: number;
}

export interface ClusterInfo {
  clusterId: string;
  controllerId: number | null;
  brokerCount: number;
  brokers: BrokerInfo[];
  observedAt: string;
}

export interface TopicSummary {
  name: string;
  internal: boolean;
}

export interface TopicListResult {
  topics: TopicSummary[];
  total: number;
  truncated: boolean;
  observedAt: string;
}

export interface PartitionMeta {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

interface RawPartitionMeta {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

export interface TopicMetadataResult {
  name: string;
  partitionCount: number;
  partitions: PartitionMeta[];
  observedAt: string;
}

export interface PartitionInfoRow extends PartitionMeta {
  highWatermark: string | null;
  lowWatermark: string | null;
}

export interface PartitionInfoResult {
  topic: string;
  partitionCount: number;
  partitions: PartitionInfoRow[];
  truncated: boolean;
  observedAt: string;
}

export interface AdminServiceOptions {
  metrics?: Metrics | undefined;
  cache?: KeyValueCache | undefined;
  cacheTtlSeconds?: number | undefined;
  logger?: Logger | undefined;
  session?: KafkaAdminSession | undefined;
}

export class AdminService {
  private readonly session: KafkaAdminSession;
  private readonly metrics: Metrics;
  private readonly cache: KeyValueCache | undefined;
  private readonly cacheTtlSeconds: number;
  private readonly logger: Logger | undefined;

  constructor(kafka: Kafka, options: AdminServiceOptions = {}) {
    this.session = options.session ?? new KafkaAdminSession(kafka);
    this.metrics = options.metrics ?? noopMetrics;
    this.cache = options.cache;
    this.cacheTtlSeconds = options.cacheTtlSeconds ?? 15;
    this.logger = options.logger;
  }

  async getClusterInfo(): Promise<ClusterInfo> {
    return this.cached('cluster-info', 'describe_cluster', () =>
      this.withAdmin(async (admin) => {
        const cluster = await admin.describeCluster();
        return {
          clusterId: cluster.clusterId,
          controllerId: cluster.controller,
          brokerCount: cluster.brokers.length,
          brokers: cluster.brokers.map(({ nodeId, host, port }) => ({ nodeId, host, port })),
          observedAt: new Date().toISOString(),
        };
      }),
    );
  }

  async listTopics(options: {
    nameContains?: string | undefined;
    limit: number;
  }): Promise<TopicListResult> {
    const { nameContains, limit } = options;
    return this.time('list_topics', () =>
      this.withAdmin(async (admin) => {
        const names = await admin.listTopics();
        const filtered =
          nameContains === undefined ? names : names.filter((name) => name.includes(nameContains));
        filtered.sort((a, b) => a.localeCompare(b));
        const total = filtered.length;
        const page = filtered.slice(0, limit);
        return {
          topics: page.map((name) => ({ name, internal: name.startsWith('__') })),
          total,
          truncated: total > page.length,
          observedAt: new Date().toISOString(),
        };
      }),
    );
  }

  async getTopicMetadata(topic: string): Promise<TopicMetadataResult> {
    return this.cached(`topic-metadata:${topic}`, 'topic_metadata', () =>
      this.withAdmin(async (admin) => {
        const meta = await this.fetchMeta(admin, topic);
        return {
          name: meta.name,
          partitionCount: meta.partitions.length,
          partitions: meta.partitions
            .map(toPartitionMeta)
            .sort((a, b) => a.partitionId - b.partitionId),
          observedAt: new Date().toISOString(),
        };
      }),
    );
  }

  async getPartitionInfo(topic: string, partition?: number): Promise<PartitionInfoResult> {
    return this.time('partition_info', () =>
      this.withAdmin(async (admin) => {
        const meta = await this.fetchMeta(admin, topic);
        const offsets = await admin.fetchTopicOffsets(topic);
        const offsetsByPartition = new Map(offsets.map((entry) => [entry.partition, entry]));

        const allRows: PartitionInfoRow[] = meta.partitions
          .map(toPartitionMeta)
          .map((part) => {
            const offset = offsetsByPartition.get(part.partitionId);
            return {
              ...part,
              highWatermark: offset === undefined ? null : offset.high,
              lowWatermark: offset === undefined ? null : offset.low,
            };
          })
          .sort((a, b) => a.partitionId - b.partitionId);

        if (partition !== undefined) {
          const match = allRows.find((row) => row.partitionId === partition);
          if (match === undefined) {
            throw new AppError(
              'NOT_FOUND',
              `Partition ${partition} does not exist on topic "${topic}"`,
              { topic, partition, partitionCount: allRows.length },
            );
          }
          return {
            topic,
            partitionCount: allRows.length,
            partitions: [match],
            truncated: false,
            observedAt: new Date().toISOString(),
          };
        }

        const page = allRows.slice(0, MAX_PARTITION_ROWS);
        return {
          topic,
          partitionCount: allRows.length,
          partitions: page,
          truncated: allRows.length > page.length,
          observedAt: new Date().toISOString(),
        };
      }),
    );
  }

  private async fetchMeta(
    admin: Admin,
    topic: string,
  ): Promise<{ name: string; partitions: RawPartitionMeta[] }> {
    let result: { topics: { name: string; partitions: RawPartitionMeta[] }[] };
    try {
      result = await admin.fetchTopicMetadata({ topics: [topic] });
    } catch (err) {
      const mapped = mapKafkaError(err);
      if (mapped.code === 'NOT_FOUND') {
        throw new AppError('NOT_FOUND', `Topic "${topic}" not found`, {
          topic,
          ...(mapped.context ?? {}),
        });
      }
      throw mapped;
    }
    const meta = result.topics.find((entry) => entry.name === topic);
    if (meta === undefined || meta.partitions.length === 0) {
      throw new AppError('NOT_FOUND', `Topic "${topic}" not found`, { topic });
    }
    return meta;
  }

  private async cached<T>(key: string, operation: string, load: () => Promise<T>): Promise<T> {
    const hit = await this.readCache<T>(key);
    if (hit !== null) return hit;
    const value = await this.time(operation, load);
    await this.writeCache(key, value);
    return value;
  }

  private async readCache<T>(key: string): Promise<T | null> {
    if (this.cache === undefined) return null;
    try {
      const raw = await this.cache.get(key);
      if (raw === null) return null;
      return JSON.parse(raw) as T;
    } catch {
      return null;
    }
  }

  private async writeCache(key: string, value: unknown): Promise<void> {
    if (this.cache === undefined) return;
    try {
      await this.cache.set(key, JSON.stringify(value), this.cacheTtlSeconds);
    } catch {
      return;
    }
  }

  private async time<T>(operation: string, run: () => Promise<T>): Promise<T> {
    const started = Date.now();
    try {
      return await run();
    } finally {
      const durationMs = Date.now() - started;
      this.metrics.kafkaRequest(operation, durationMs);
      logTiming(this.logger, 'kafka request', { operation, durationMs });
    }
  }

  private withAdmin<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
    return this.session.run(fn);
  }
}

function toPartitionMeta(partition: RawPartitionMeta): PartitionMeta {
  return {
    partitionId: partition.partitionId,
    leader: partition.leader,
    replicas: [...partition.replicas],
    isr: [...partition.isr],
  };
}
