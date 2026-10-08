import type { Admin, Kafka } from 'kafkajs';

import { AppError } from '../errors/app-error.js';

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

export class AdminService {
  private readonly kafka: Kafka;

  constructor(kafka: Kafka) {
    this.kafka = kafka;
  }

  async getClusterInfo(): Promise<ClusterInfo> {
    return this.withAdmin(async (admin) => {
      const cluster = await admin.describeCluster();
      return {
        clusterId: cluster.clusterId,
        controllerId: cluster.controller,
        brokerCount: cluster.brokers.length,
        brokers: cluster.brokers.map(({ nodeId, host, port }) => ({ nodeId, host, port })),
        observedAt: new Date().toISOString(),
      };
    });
  }

  async listTopics(options: {
    nameContains?: string | undefined;
    limit: number;
  }): Promise<TopicListResult> {
    const { nameContains, limit } = options;
    return this.withAdmin(async (admin) => {
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
    });
  }

  async getTopicMetadata(topic: string): Promise<TopicMetadataResult> {
    return this.withAdmin(async (admin) => {
      const meta = await this.fetchMeta(admin, topic);
      return {
        name: meta.name,
        partitionCount: meta.partitions.length,
        partitions: meta.partitions
          .map(toPartitionMeta)
          .sort((a, b) => a.partitionId - b.partitionId),
        observedAt: new Date().toISOString(),
      };
    });
  }

  async getPartitionInfo(topic: string, partition?: number): Promise<PartitionInfoResult> {
    return this.withAdmin(async (admin) => {
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
    });
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

  private async withAdmin<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
    const admin = this.kafka.admin();
    let connected = false;
    try {
      await admin.connect();
      connected = true;
      return await fn(admin);
    } catch (err) {
      throw mapKafkaError(err);
    } finally {
      if (connected) {
        await admin.disconnect().catch(() => undefined);
      }
    }
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
