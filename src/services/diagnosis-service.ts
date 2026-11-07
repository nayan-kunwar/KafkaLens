import type { Logger } from 'pino';

import { diagnose, type DiagnosisPartition, type DiagnosisResult } from '../domain/diagnosis.js';
import type { Finding } from '../domain/findings.js';
import type { OffsetSample, ProgressClass } from '../domain/stall.js';
import { AppError } from '../errors/app-error.js';
import { noopMetrics, type Metrics } from '../observability/metrics.js';
import { logTiming } from '../observability/tool-call.js';
import type { IncidentStore, SimilarIncident } from '../persistence/incident-store.js';
import type { LagHistoryPoint, LagStore } from '../persistence/lag-store.js';

import type { AdminService } from './admin-service.js';
import type {
  ConsumerAssignmentsResult,
  ConsumerGroupsService,
} from './consumer-groups-service.js';
import type {
  CorrelationEvent,
  CorrelationResult,
  CorrelationService,
} from './correlation-service.js';
import type { OffsetsService, PartitionLagRow, TopicLagResult } from './offsets-service.js';

const HISTORY_LIMIT = 2000;
const DEDUPE_SECONDS = 900;
const MAX_PARTITION_ROWS = 50;
const STORE_SEVERITIES = new Set(['medium', 'high', 'critical']);

export interface PartitionView {
  partitionId: number;
  highWatermark: string | null;
  committedOffset: string | null;
  lag: string | null;
  unknownReason?: PartitionLagRow['unknownReason'];
  assigned: boolean | null;
  progress: ProgressClass;
}

export interface PartialFailure {
  source: string;
  code: string;
  message: string;
}

export interface ReportCorrelation {
  status: CorrelationResult['status'] | 'skipped';
  events: CorrelationEvent[];
  message: string;
}

export interface DiagnosisReport extends DiagnosisResult {
  groupId: string;
  topic: string;
  observedAt: string;
  group: {
    state: string;
    protocolType: string;
    protocol: string;
    memberCount: number;
  };
  topicPartitionCount: number;
  partitions: PartitionView[];
  partitionsTruncated: boolean;
  assignmentsKnown: boolean;
  relatedIncidents: {
    status: 'ok' | 'unavailable' | 'skipped';
    incidents: SimilarIncident[];
    message: string;
  };
  correlation: ReportCorrelation;
  partialFailures: PartialFailure[];
}

export interface DiagnosisServiceOptions {
  imbalanceRatio: number;
  stallWindowSeconds: number;
  metrics?: Metrics | undefined;
  logger?: Logger | undefined;
  /** Test seam. Production uses the clock. */
  now?: () => Date;
}

export class DiagnosisService {
  private readonly metrics: Metrics;
  private readonly now: () => Date;

  constructor(
    private readonly admin: AdminService,
    private readonly groups: ConsumerGroupsService,
    private readonly offsets: OffsetsService,
    private readonly history: LagStore,
    private readonly incidents: IncidentStore,
    private readonly correlation: CorrelationService,
    private readonly options: DiagnosisServiceOptions,
  ) {
    this.metrics = options.metrics ?? noopMetrics;
    this.now = options.now ?? (() => new Date());
  }

  diagnoseConsumerGroup(groupId: string, topic: string): Promise<DiagnosisReport> {
    return this.timed('diagnose_consumer_group', () =>
      this.build(groupId, topic, { recordIncident: true, correlate: true, incidents: true }),
    );
  }

  analyzeConsumerLag(groupId: string, topic: string): Promise<DiagnosisReport> {
    return this.timed('analyze_consumer_lag', () => this.build(groupId, topic, focused()));
  }

  analyzePartitionImbalance(
    groupId: string,
    topic: string,
  ): Promise<{
    groupId: string;
    topic: string;
    observedAt: string;
    imbalance: DiagnosisResult['imbalance'];
    finding: Finding | null;
    partialFailures: PartialFailure[];
  }> {
    return this.timed('analyze_partition_imbalance', async () => {
      const report = await this.build(groupId, topic, focused());
      return {
        groupId,
        topic,
        observedAt: report.observedAt,
        imbalance: report.imbalance,
        finding: report.findings.find((finding) => finding.type === 'PARTITION_IMBALANCE') ?? null,
        partialFailures: report.partialFailures,
      };
    });
  }

  detectStalledConsumers(
    groupId: string,
    topic: string,
  ): Promise<{
    groupId: string;
    topic: string;
    observedAt: string;
    stall: DiagnosisResult['stall'];
    findings: Finding[];
    partialFailures: PartialFailure[];
  }> {
    return this.timed('detect_stalled_consumers', async () => {
      const report = await this.build(groupId, topic, focused());
      return {
        groupId,
        topic,
        observedAt: report.observedAt,
        stall: report.stall,
        findings: report.findings.filter(
          (finding) => finding.type === 'CONSUMER_STALLED' || finding.type === 'CONSUMER_SLOW',
        ),
        partialFailures: report.partialFailures,
      };
    });
  }

  analyzeThroughput(
    groupId: string,
    topic: string,
  ): Promise<{
    groupId: string;
    topic: string;
    observedAt: string;
    throughput: DiagnosisResult['throughput'];
    finding: Finding | null;
    partialFailures: PartialFailure[];
  }> {
    return this.timed('analyze_throughput', async () => {
      const report = await this.build(groupId, topic, focused());
      return {
        groupId,
        topic,
        observedAt: report.observedAt,
        throughput: report.throughput,
        finding:
          report.findings.find((finding) => finding.type === 'PRODUCER_OUTPACING_CONSUMER') ?? null,
        partialFailures: report.partialFailures,
      };
    });
  }

  findSimilarIncidents(input: {
    groupId: string;
    topic: string;
    findingTypes: readonly string[];
    limit: number;
  }): Promise<{
    groupId: string;
    topic: string;
    incidents: SimilarIncident[];
    observedAt: string;
  }> {
    return this.timed('find_similar_incidents', async () => {
      const clusterId = await this.resolveClusterId([]);
      const incidents = await this.incidents.findSimilar({
        ...(clusterId === undefined ? {} : { clusterId }),
        groupId: input.groupId,
        topic: input.topic,
        findingTypes: input.findingTypes,
        limit: input.limit,
      });
      return {
        groupId: input.groupId,
        topic: input.topic,
        incidents,
        observedAt: this.now().toISOString(),
      };
    });
  }

  private async build(
    groupId: string,
    topic: string,
    flags: { recordIncident: boolean; correlate: boolean; incidents: boolean },
  ): Promise<DiagnosisReport> {
    const observedAtDate = this.now();
    const observedAt = observedAtDate.toISOString();
    const partialFailures: PartialFailure[] = [];

    const clusterId = await this.resolveClusterId(partialFailures);
    const membership = await this.groups.getConsumerGroupWithAssignments(groupId);
    const group = membership.group;
    const lag = await this.requireTopicLag(groupId, topic, group.state);
    // The lag read records history in the background; wait for that sample so
    // progress analysis is based on data from this observation, not stale rows.
    await this.offsets.flushHistory();

    const assignments = this.readAssignments(membership.assignments, topic, partialFailures);
    const history = await this.readHistory(
      clusterId,
      groupId,
      topic,
      observedAtDate,
      partialFailures,
    );
    const coverageIncomplete = lag.truncated || history.truncated || assignments.truncated;

    const historyByPartition = groupHistoryByPartition(history.points);
    const partitions = lag.partitions.map((row) =>
      toDiagnosisPartition(
        row,
        assignments,
        historyByPartition.get(row.partitionId) ?? [],
        observedAt,
      ),
    );
    const result = diagnose({
      groupId,
      topic,
      groupState: group.state,
      memberCount: group.memberCount,
      partitions,
      historyAvailable: history.available,
      imbalanceRatio: this.options.imbalanceRatio,
      stallWindowSeconds: this.options.stallWindowSeconds,
      now: observedAtDate,
      coverageIncomplete,
    });

    if (lag.orphanedCommits.length > 0) {
      result.evidence.unshift({
        kind: 'observed_fact',
        text: `${lag.orphanedCommits.length} committed offset(s) refer to partitions that are not in the topic metadata.`,
      });
    }

    const progressById = new Map(
      result.stall.partitions.map((partition) => [partition.partitionId, partition.progress]),
    );
    const views = partitions.map((partition) =>
      toView(partition, progressById.get(partition.partitionId) ?? 'INSUFFICIENT_EVIDENCE'),
    );

    let relatedIncidents: DiagnosisReport['relatedIncidents'] = {
      status: 'skipped',
      incidents: [],
      message: 'Incident lookup runs from diagnose_consumer_group and find_similar_incidents.',
    };
    if (flags.incidents) {
      relatedIncidents = await this.readSimilar(
        clusterId,
        groupId,
        topic,
        result.findings,
        partialFailures,
      );
    }

    let correlation: ReportCorrelation = {
      status: 'skipped',
      events: [],
      message: 'Change correlation runs from diagnose_consumer_group.',
    };
    if (flags.correlate) {
      const since = new Date(
        observedAtDate.getTime() - this.options.stallWindowSeconds * 1000,
      ).toISOString();
      correlation = await this.correlation.correlate({ since, until: observedAt });
    }

    if (flags.recordIncident && shouldStore(result.findings)) {
      await this.storeIncident(clusterId, groupId, topic, observedAt, result, partialFailures);
    }

    return {
      ...result,
      groupId,
      topic,
      observedAt,
      group: {
        state: group.state,
        protocolType: group.protocolType,
        protocol: group.protocol,
        memberCount: group.memberCount,
      },
      topicPartitionCount: lag.partitionCount,
      partitions: views.slice(0, MAX_PARTITION_ROWS),
      partitionsTruncated: views.length > MAX_PARTITION_ROWS || lag.truncated,
      assignmentsKnown: assignments.known,
      relatedIncidents,
      correlation,
      partialFailures,
    };
  }

  private async requireTopicLag(
    groupId: string,
    topic: string,
    groupState: string,
  ): Promise<TopicLagResult> {
    const lag = await this.offsets.getConsumerLag(groupId, topic, groupState);
    if (!('partitions' in lag)) {
      throw new AppError('INTERNAL', 'Expected per-partition lag for a single topic');
    }
    return lag;
  }

  private readAssignments(
    result: ConsumerAssignmentsResult,
    topic: string,
    partialFailures: PartialFailure[],
  ): { known: boolean; truncated: boolean; assigned: Set<number> } {
    if (result.truncated || result.assignments.some((member) => member.assignment === null)) {
      partialFailures.push({
        source: 'assignments',
        code: 'PARTIAL',
        message:
          'At least one member assignment was truncated or undecodable. Unassigned partitions were not inferred.',
      });
      return { known: false, truncated: result.truncated, assigned: new Set() };
    }
    const assigned = new Set<number>();
    for (const member of result.assignments) {
      for (const entry of member.assignment ?? []) {
        if (entry.topic !== topic) continue;
        for (const partition of entry.partitions) assigned.add(partition);
      }
    }
    return { known: true, truncated: false, assigned };
  }

  private async readHistory(
    clusterId: string | undefined,
    groupId: string,
    topic: string,
    now: Date,
    partialFailures: PartialFailure[],
  ): Promise<{ available: boolean; truncated: boolean; points: LagHistoryPoint[] }> {
    try {
      const since = new Date(now.getTime() - this.options.stallWindowSeconds * 1000).toISOString();
      const history = await this.history.queryHistory({
        ...(clusterId === undefined ? {} : { clusterId }),
        groupId,
        topic,
        since,
        limit: HISTORY_LIMIT,
      });
      if (history.truncated) {
        partialFailures.push({
          source: 'lag_history',
          code: 'TRUNCATED',
          message: `Lag history was capped at ${HISTORY_LIMIT} rows.`,
        });
      }
      return { available: true, truncated: history.truncated, points: history.observations };
    } catch (err) {
      partialFailures.push(
        failure(
          'lag_history',
          err,
          'Lag history was unavailable. The live lag sample was still used.',
        ),
      );
      return { available: false, truncated: false, points: [] };
    }
  }

  /**
   * Resolves the cluster once per report so history, incident lookup, and
   * incident storage use the same cluster scope. A failure degrades the report
   * to unscoped history instead of failing the diagnosis.
   */
  private async resolveClusterId(partialFailures: PartialFailure[]): Promise<string | undefined> {
    try {
      return (await this.admin.getClusterInfo()).clusterId;
    } catch (err) {
      partialFailures.push(
        failure(
          'cluster',
          err,
          'Cluster id was unavailable. History and incidents could not be scoped to one cluster.',
        ),
      );
      return undefined;
    }
  }

  private async readSimilar(
    clusterId: string | undefined,
    groupId: string,
    topic: string,
    findings: readonly Finding[],
    partialFailures: PartialFailure[],
  ): Promise<DiagnosisReport['relatedIncidents']> {
    try {
      const incidents = await this.incidents.findSimilar({
        ...(clusterId === undefined ? {} : { clusterId }),
        groupId,
        topic,
        findingTypes: findings.map((finding) => finding.type),
        limit: 5,
      });
      return {
        status: 'ok',
        incidents,
        message:
          incidents.length === 0
            ? 'No stored incident overlapped this group, topic, or finding type.'
            : 'Scores are deterministic overlaps, not a model judgment.',
      };
    } catch (err) {
      partialFailures.push(failure('incidents', err, 'Similar incidents could not be read.'));
      return {
        status: 'unavailable',
        incidents: [],
        message: 'Similar incidents could not be read.',
      };
    }
  }

  private async storeIncident(
    clusterId: string | undefined,
    groupId: string,
    topic: string,
    observedAt: string,
    result: DiagnosisResult,
    partialFailures: PartialFailure[],
  ): Promise<void> {
    const top = result.findings[0];
    if (top === undefined) return;
    try {
      await this.incidents.record(
        {
          clusterId: clusterId ?? 'unknown',
          topic,
          consumerGroup: groupId,
          findingTypes: result.findings.map((finding) => finding.type),
          severity: top.severity,
          summary: `${top.type} on ${groupId}/${topic}. ${result.impact.summary}`.slice(0, 500),
          evidence: top.evidence.slice(0, 8),
          observedAt,
        },
        DEDUPE_SECONDS,
      );
    } catch (err) {
      partialFailures.push(
        failure('incidents', err, 'The diagnosis was not stored as an incident.'),
      );
    }
  }

  private async timed<T>(operation: string, run: () => Promise<T>): Promise<T> {
    const started = Date.now();
    try {
      return await run();
    } finally {
      const durationMs = Date.now() - started;
      this.metrics.diagnosis(operation, durationMs);
      logTiming(this.options.logger, 'diagnosis', { operation, durationMs });
    }
  }
}

function focused(): { recordIncident: false; correlate: false; incidents: false } {
  return { recordIncident: false, correlate: false, incidents: false };
}

function shouldStore(findings: readonly Finding[]): boolean {
  return findings.some(
    (finding) => finding.type !== 'UNKNOWN' && STORE_SEVERITIES.has(finding.severity),
  );
}

function groupHistoryByPartition(
  history: readonly LagHistoryPoint[],
): Map<number, LagHistoryPoint[]> {
  const byPartition = new Map<number, LagHistoryPoint[]>();
  for (const point of history) {
    const existing = byPartition.get(point.partition);
    if (existing === undefined) {
      byPartition.set(point.partition, [point]);
    } else {
      existing.push(point);
    }
  }
  return byPartition;
}

function toDiagnosisPartition(
  row: PartitionLagRow,
  assignments: { known: boolean; assigned: Set<number> },
  history: readonly LagHistoryPoint[],
  observedAt: string,
): DiagnosisPartition {
  const samples: OffsetSample[] = history
    .filter((point) => point.observedAt !== observedAt)
    .map((point) => ({
      observedAt: point.observedAt,
      highWatermark: point.highWatermark,
      committedOffset: point.committedOffset,
      lag: point.lag,
    }));
  samples.push({
    observedAt,
    highWatermark: row.highWatermark,
    committedOffset: row.committedOffset,
    lag: row.lag,
  });
  samples.sort((a, b) => a.observedAt.localeCompare(b.observedAt));
  const partition: DiagnosisPartition = {
    partitionId: row.partitionId,
    highWatermark: row.highWatermark,
    committedOffset: row.committedOffset,
    lag: row.lag,
    assigned: assignments.known ? assignments.assigned.has(row.partitionId) : null,
    samples,
  };
  if (row.unknownReason !== undefined) partition.unknownReason = row.unknownReason;
  return partition;
}

function toView(partition: DiagnosisPartition, progress: ProgressClass): PartitionView {
  const view: PartitionView = {
    partitionId: partition.partitionId,
    highWatermark: partition.highWatermark,
    committedOffset: partition.committedOffset,
    lag: partition.lag,
    assigned: partition.assigned,
    progress,
  };
  if (partition.unknownReason !== undefined) view.unknownReason = partition.unknownReason;
  return view;
}

function failure(source: string, err: unknown, fallback: string): PartialFailure {
  if (err instanceof AppError) {
    return { source, code: err.code, message: fallback };
  }
  return { source, code: 'INTERNAL', message: fallback };
}
