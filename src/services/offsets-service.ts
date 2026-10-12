import type { Admin, Kafka } from 'kafkajs';
import type { Logger } from 'pino';

import { calculatePartitionLag, sumLags, type LagUnknownReason } from '../domain/lag.js';
import { AppError } from '../errors/app-error.js';
import type { LagObservationRow, LagStore } from '../persistence/lag-store.js';

import type { ConsumerGroupsService } from './consumer-groups-service.js';
import { mapKafkaError } from './kafka-error.js';

const MAX_PARTITION_ROWS = 500;
const MAX_TOPIC_ROWS = 100;

export interface PartitionLagRow {
  partitionId: number;
  highWatermark: string | null;
  committedOffset: string | null;
  lag: string | null;
  unknownReason?: LagUnknownReason;
  adjusted?: true;
}

export interface OrphanedCommit {
  partition: number;
  committedOffset: string;
}

export interface TopicLagResult {
  groupId: string;
  topic: string;
  groupState: string;
  partitionCount: number;
  partitions: PartitionLagRow[];
  totalLag: string | null;
  unknownPartitionCount: number;
  orphanedCommits: OrphanedCommit[];
  truncated: boolean;
  observedAt: string;
}

export interface TopicLagSummary {
  topic: string;
  partitionCount: number;
  totalLag: string | null;
  unknownPartitions: number;
  topicMissing?: boolean;
}

export interface GroupLagResult {
  groupId: string;
  groupState: string;
  topics: TopicLagSummary[];
  topicCount: number;
  truncated: boolean;
  observedAt: string;
}

export class OffsetsService {
  private readonly kafka: Kafka;
  private readonly consumerGroups: ConsumerGroupsService;
  private readonly lagStore: LagStore | undefined;
  private readonly logger: Logger | undefined;
  private clusterId: string | undefined;

  constructor(
    kafka: Kafka,
    consumerGroups: ConsumerGroupsService,
    lagStore?: LagStore,
    logger?: Logger,
  ) {
    this.kafka = kafka;
    this.consumerGroups = consumerGroups;
    this.lagStore = lagStore;
    this.logger = logger;
  }

  async getConsumerLag(groupId: string, topic?: string): Promise<TopicLagResult | GroupLagResult> {
    // Group existence first: unknown (Dead) groups fail with NOT_FOUND here,
    // and the observed state travels with every lag response as context.
    const group = await this.consumerGroups.getConsumerGroup(groupId);
    return this.withAdmin(async (admin) => {
      if (topic !== undefined) {
        const result = await this.topicLag(admin, groupId, group.state, topic);
        await this.bestEffortRecord(admin, groupId, this.toTopicRows(result));
        return result;
      }
      const { result, rows } = await this.groupLag(admin, groupId, group.state);
      await this.bestEffortRecord(admin, groupId, rows);
      return result;
    });
  }

  private toTopicRows(result: TopicLagResult): { topic: string; row: PartitionLagRow }[] {
    return [
      ...result.partitions.map((row) => ({ topic: result.topic, row })),
      ...result.orphanedCommits.map((orphan) => ({
        topic: result.topic,
        row: {
          partitionId: orphan.partition,
          highWatermark: null,
          committedOffset: orphan.committedOffset,
          ...calculatePartitionLag(null, orphan.committedOffset),
        } satisfies PartitionLagRow,
      })),
    ];
  }

  private async bestEffortRecord(
    admin: Admin,
    groupId: string,
    rows: { topic: string; row: PartitionLagRow }[],
  ): Promise<void> {
    if (this.lagStore === undefined || rows.length === 0) return;
    try {
      if (this.clusterId === undefined) {
        const cluster = await admin.describeCluster();
        this.clusterId = cluster.clusterId;
      }
      const observedAt = new Date().toISOString();
      const records: LagObservationRow[] = rows.map(({ topic, row }) => ({
        clusterId: this.clusterId as string,
        topic,
        consumerGroup: groupId,
        partition: row.partitionId,
        highWatermark: row.highWatermark,
        committedOffset: row.committedOffset,
        lag: row.lag,
        observedAt,
      }));
      await this.lagStore.recordBatch(records);
    } catch (err) {
      // History is best-effort: a store failure must never fail the lag call.
      this.logger?.warn(
        { err: err instanceof Error ? err.message : String(err), groupId },
        'lag history record skipped',
      );
    }
  }

  private async topicLag(
    admin: Admin,
    groupId: string,
    groupState: string,
    topic: string,
  ): Promise<TopicLagResult> {
    const partitionIds = await this.fetchPartitionIds(admin, topic);
    const watermarks = await admin.fetchTopicOffsets(topic);
    const highByPartition = new Map(watermarks.map((entry) => [entry.partition, entry.high]));
    const commits = await this.fetchCommits(admin, groupId, [topic]);
    const committedByPartition = new Map(
      (commits.get(topic) ?? []).map((entry) => [entry.partition, entry.offset]),
    );

    const knownIds = new Set(partitionIds);
    const orphanedCommits: OrphanedCommit[] = [...committedByPartition.entries()]
      .filter(([partition]) => !knownIds.has(partition))
      .map(([partition, committedOffset]) => ({ partition, committedOffset }))
      .sort((a, b) => a.partition - b.partition);

    const allRows: PartitionLagRow[] = this.buildRows(
      partitionIds,
      highByPartition,
      committedByPartition,
    );

    const { total, unknownCount } = sumLags(allRows.map((row) => row.lag));
    const page = allRows.slice(0, MAX_PARTITION_ROWS);
    return {
      groupId,
      topic,
      groupState,
      partitionCount: allRows.length,
      partitions: page,
      totalLag: total,
      unknownPartitionCount: unknownCount,
      orphanedCommits,
      truncated: allRows.length > page.length,
      observedAt: new Date().toISOString(),
    };
  }

  private async groupLag(
    admin: Admin,
    groupId: string,
    groupState: string,
  ): Promise<{ result: GroupLagResult; rows: { topic: string; row: PartitionLagRow }[] }> {
    const commits = await this.fetchCommits(admin, groupId);
    const topics = [...commits.keys()].sort((a, b) => a.localeCompare(b));
    const topicCount = topics.length;
    const page = topics.slice(0, MAX_TOPIC_ROWS);
    const observedAt = new Date().toISOString();

    const summaries: TopicLagSummary[] = [];
    const rows: { topic: string; row: PartitionLagRow }[] = [];
    for (const topic of page) {
      const { summary, topicRows } = await this.summarizeTopic(
        admin,
        topic,
        commits.get(topic) ?? [],
      );
      summaries.push(summary);
      for (const row of topicRows) {
        rows.push({ topic, row });
      }
    }
    return {
      result: {
        groupId,
        groupState,
        topics: summaries,
        topicCount,
        truncated: topicCount > page.length,
        observedAt,
      },
      rows,
    };
  }

  private async summarizeTopic(
    admin: Admin,
    topic: string,
    commits: { partition: number; offset: string }[],
  ): Promise<{ summary: TopicLagSummary; topicRows: PartitionLagRow[] }> {
    let watermarks: { partition: number; high: string }[];
    try {
      watermarks = await admin.fetchTopicOffsets(topic);
    } catch (err) {
      // Commits remain after a topic is deleted: keep the topic visible with
      // every partition unknown instead of failing the whole group call.
      const mapped = mapKafkaError(err);
      if (mapped.code === 'NOT_FOUND') {
        return {
          summary: {
            topic,
            partitionCount: commits.length,
            totalLag: null,
            unknownPartitions: commits.length,
            topicMissing: true,
          },
          topicRows: commits.map((entry) => ({
            partitionId: entry.partition,
            highWatermark: null,
            committedOffset: entry.offset,
            ...calculatePartitionLag(null, entry.offset),
          })),
        };
      }
      throw mapped;
    }
    const highByPartition = new Map(watermarks.map((entry) => [entry.partition, entry.high]));
    const committedByPartition = new Map(commits.map((entry) => [entry.partition, entry.offset]));
    const partitionIds = [
      ...new Set<number>([...highByPartition.keys(), ...committedByPartition.keys()]),
    ];
    const topicRows = this.buildRows(partitionIds, highByPartition, committedByPartition);
    const { total, unknownCount } = sumLags(topicRows.map((row) => row.lag));
    return {
      summary: {
        topic,
        partitionCount: topicRows.length,
        totalLag: total,
        unknownPartitions: unknownCount,
      },
      topicRows,
    };
  }

  private buildRows(
    partitionIds: number[],
    highByPartition: Map<number, string>,
    committedByPartition: Map<number, string>,
  ): PartitionLagRow[] {
    return [...partitionIds]
      .sort((a, b) => a - b)
      .map((partitionId) => {
        const highWatermark = highByPartition.get(partitionId) ?? null;
        const committedOffset = committedByPartition.get(partitionId) ?? null;
        return {
          partitionId,
          highWatermark,
          committedOffset,
          ...calculatePartitionLag(highWatermark, committedOffset),
        };
      });
  }

  private async fetchPartitionIds(admin: Admin, topic: string): Promise<number[]> {
    let result: { topics: { name: string; partitions: { partitionId: number }[] }[] };
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
    return meta.partitions.map((partition) => partition.partitionId);
  }

  private async fetchCommits(
    admin: Admin,
    groupId: string,
    topics?: string[],
  ): Promise<Map<string, { partition: number; offset: string }[]>> {
    const fetched = await admin.fetchOffsets(
      topics === undefined ? { groupId } : { groupId, topics },
    );
    return new Map(
      fetched.map((entry) => [
        entry.topic,
        entry.partitions.map((partition) => ({
          partition: partition.partition,
          offset: partition.offset,
        })),
      ]),
    );
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
