import type { Admin, Kafka } from 'kafkajs';
import type { Logger } from 'pino';

import { calculatePartitionLag, sumLags, type LagUnknownReason } from '../domain/lag.js';
import { AppError } from '../errors/app-error.js';
import { noopMetrics, type Metrics } from '../observability/metrics.js';
import { logTiming } from '../observability/tool-call.js';
import type { LagObservationRow, LagStore } from '../persistence/lag-store.js';

import type { ConsumerGroupsService } from './consumer-groups-service.js';
import { KafkaAdminSession } from './kafka-admin-session.js';
import { mapKafkaError } from './kafka-error.js';

const MAX_PARTITION_ROWS = 500;
const MAX_TOPIC_ROWS = 100;
const GROUP_TOPIC_CONCURRENCY = 8;

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

export interface OffsetsServiceOptions {
  lagStore?: LagStore | undefined;
  logger?: Logger | undefined;
  metrics?: Metrics | undefined;
  session?: KafkaAdminSession | undefined;
}

export class OffsetsService {
  private readonly session: KafkaAdminSession;
  private readonly consumerGroups: ConsumerGroupsService;
  private readonly lagStore: LagStore | undefined;
  private readonly logger: Logger | undefined;
  private readonly metrics: Metrics;
  private clusterId: string | undefined;
  private readonly pendingHistory = new Set<Promise<void>>();

  constructor(
    kafka: Kafka,
    consumerGroups: ConsumerGroupsService,
    options: OffsetsServiceOptions = {},
  ) {
    this.session = options.session ?? new KafkaAdminSession(kafka);
    this.consumerGroups = consumerGroups;
    this.lagStore = options.lagStore;
    this.logger = options.logger;
    this.metrics = options.metrics ?? noopMetrics;
  }

  async getConsumerLag(
    groupId: string,
    topic?: string,
    knownGroupState?: string,
  ): Promise<TopicLagResult | GroupLagResult> {
    const started = Date.now();
    try {
      return await this.loadConsumerLag(groupId, topic, knownGroupState);
    } finally {
      const durationMs = Date.now() - started;
      this.metrics.kafkaRequest('consumer_lag', durationMs);
      logTiming(this.logger, 'kafka request', { operation: 'consumer_lag', durationMs });
    }
  }

  private async loadConsumerLag(
    groupId: string,
    topic?: string,
    knownGroupState?: string,
  ): Promise<TopicLagResult | GroupLagResult> {
    // Callers that already described the group (diagnosis) pass its state so
    // this path does not repeat the broker call.
    const groupState =
      knownGroupState ?? (await this.consumerGroups.getConsumerGroup(groupId)).state;
    let result: TopicLagResult | GroupLagResult;
    let rows: { topic: string; row: PartitionLagRow }[];
    if (topic !== undefined) {
      const topicResult = await this.withAdmin((admin) =>
        this.topicLag(admin, groupId, groupState, topic),
      );
      result = topicResult.result;
      rows = topicResult.rows;
    } else {
      const grouped = await this.withAdmin((admin) => this.groupLag(admin, groupId, groupState));
      result = grouped.result;
      rows = grouped.rows;
    }
    // History is best-effort and is not on the response path: recording may
    // take a PostgreSQL round trip and must not delay a live lag read.
    this.trackHistory(groupId, rows);
    return result;
  }

  /**
   * Waits for background history writes. Diagnosis calls this before reading
   * history so its own sample is visible, and shutdown calls it before the
   * PostgreSQL pool closes.
   */
  async flushHistory(): Promise<void> {
    while (this.pendingHistory.size > 0) {
      await Promise.allSettled([...this.pendingHistory]);
    }
  }

  private trackHistory(groupId: string, rows: { topic: string; row: PartitionLagRow }[]): void {
    const write = this.recordHistory(groupId, rows);
    this.pendingHistory.add(write);
    void write.then(
      () => this.pendingHistory.delete(write),
      () => this.pendingHistory.delete(write),
    );
  }

  private async recordHistory(
    groupId: string,
    rows: { topic: string; row: PartitionLagRow }[],
  ): Promise<void> {
    if (this.lagStore === undefined || rows.length === 0) return;
    try {
      if (this.clusterId === undefined) {
        this.clusterId = await this.withAdmin(async (admin) => {
          const cluster = await admin.describeCluster();
          return cluster.clusterId;
        });
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
      this.metrics.lagObservations(records.length);
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
  ): Promise<{
    result: TopicLagResult;
    rows: { topic: string; row: PartitionLagRow }[];
  }> {
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
      result: {
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
      },
      // Persist every partition, not only the response page, so a topic with
      // more than 500 partitions keeps complete history.
      rows: [
        ...allRows.map((row) => ({ topic, row })),
        ...orphanedCommits.map((orphan) => ({
          topic,
          row: {
            partitionId: orphan.partition,
            highWatermark: null,
            committedOffset: orphan.committedOffset,
            ...calculatePartitionLag(null, orphan.committedOffset),
          } satisfies PartitionLagRow,
        })),
      ],
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

    const summarized = await mapWithConcurrency(page, GROUP_TOPIC_CONCURRENCY, (topic) =>
      this.summarizeTopic(admin, topic, commits.get(topic) ?? []),
    );
    const summaries = summarized.map((entry) => entry.summary);
    const rows = summarized.flatMap((entry, index) => {
      const topic = page[index];
      return topic === undefined ? [] : entry.topicRows.map((row) => ({ topic, row }));
    });
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

  private withAdmin<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
    return this.session.run(fn);
  }
}

/**
 * Runs an async mapper with bounded parallelism. A group-wide lag read may
 * cover up to 100 topics; issuing those watermark calls sequentially made the
 * response wait for each topic in turn.
 */
async function mapWithConcurrency<T, R>(
  items: readonly T[],
  limit: number,
  run: (item: T) => Promise<R>,
): Promise<R[]> {
  if (items.length === 0) return [];
  const results = new Array<R>(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const index = next;
      next += 1;
      const item = items[index];
      if (item === undefined) return;
      results[index] = await run(item);
    }
  });
  await Promise.all(workers);
  return results;
}
